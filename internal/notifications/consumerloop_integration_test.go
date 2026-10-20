//go:build integration

package notifications

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/francisonah/searchsync/internal/orders"
	"github.com/francisonah/searchsync/internal/outbox"
	"github.com/francisonah/searchsync/internal/platform"
)

// TestConsumerLoop_CrashAfterTxCommitBeforeOffsetCommit_SurvivesRestart is
// the notifications-side twin of the indexer's crash/restart test: a real
// order is created (one Postgres transaction -> Debezium outbox router ->
// Kafka), the real ConsumerLoop runs with a FailureInjector that simulates
// a crash right after that specific event's local transaction
// (processed_events + order_notifications) commits but before the Kafka
// offset does, and a restarted consumer in the same group redelivers it.
// processed_events' unique constraint is what makes the redelivery a
// no-op - exactly one order_notifications row must exist, not two, despite
// HandleMessage running twice for the same event.
//
// Run against the Compose stack: go test -tags=integration ./internal/notifications/...
func TestConsumerLoop_CrashAfterTxCommitBeforeOffsetCommit_SurvivesRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, postgresDSNFromEnv())
	if err != nil || pool.Ping(ctx) != nil {
		t.Skipf("postgres not reachable, skipping integration test: %v", err)
	}
	defer pool.Close()

	idempotencyKey := "stage3-crash-test-" + uuid.New().String()
	customerID := uuid.New().String()
	service := orders.NewService(pool, orders.NewRepository(), outbox.NewRepository())
	order, err := service.CreateOrder(ctx, orders.CreateRequest{CustomerID: customerID, TotalCents: 7777}, idempotencyKey)
	if err != nil {
		t.Fatalf("create test order: %v", err)
	}
	defer func() {
		_, _ = pool.Exec(ctx, `DELETE FROM order_notifications WHERE order_id = $1`, order.ID)
		_, _ = pool.Exec(ctx, `DELETE FROM orders WHERE id = $1`, order.ID)
	}()

	group := "stage3-crash-test-" + uuid.New().String()
	brokers := strings.Split(kafkaBrokersFromEnv(), ",")
	topic := orderEventsTopicFromEnv()

	handler := NewHandler(pool)
	injector := &crashOnOrderID{target: order.ID}

	firstClient, err := platform.NewKafkaConsumer(brokers, group, []string{topic})
	if err != nil {
		t.Fatalf("create first consumer: %v", err)
	}
	firstLoop := &platform.ConsumerLoop{
		Client:   firstClient,
		Classify: classify,
		Retry:    platform.RetryPolicy{MaxAttempts: 1, BaseDelay: 0, MaxDelay: 0},
		Logger:   discardLogger(),
		Injector: injector,
		Process: func(ctx context.Context, record *kgo.Record) error {
			injector.observe(record.Value)
			return handler.HandleMessage(ctx, record.Value)
		},
	}

	runErr := runUntilErrorOrTimeout(ctx, firstLoop, 30*time.Second)
	firstClient.Close()
	if !injector.fired() {
		t.Fatal("expected the injected crash to have fired; the test event was never observed in time - check Debezium/Connect lag")
	}
	if runErr == nil {
		t.Fatal("expected the simulated crash to surface as an error from Run")
	}

	// The local transaction must have actually committed despite the "crash".
	assertExactlyOneNotification(t, ctx, pool, order.ID)

	// Restart: a fresh client, same group, no injector. It must resume
	// from the last committed offset (before our event) and redeliver it.
	secondClient, err := platform.NewKafkaConsumer(brokers, group, []string{topic})
	if err != nil {
		t.Fatalf("create second consumer: %v", err)
	}
	defer secondClient.Close()

	reprocessed := &sync.Map{}
	secondLoop := &platform.ConsumerLoop{
		Client:   secondClient,
		Classify: classify,
		Retry:    platform.DefaultRetryPolicy,
		Logger:   discardLogger(),
		Process: func(ctx context.Context, record *kgo.Record) error {
			if id, ok := decodeOrderID(record.Value); ok && id == order.ID {
				reprocessed.Store(order.ID, true)
			}
			return handler.HandleMessage(ctx, record.Value)
		},
	}
	_ = runUntilErrorOrTimeout(ctx, secondLoop, 20*time.Second)
	if _, ok := reprocessed.Load(order.ID); !ok {
		t.Fatal("expected the restarted consumer to redeliver and reprocess the event the crash prevented from committing")
	}

	// Still exactly one row: processed_events' unique constraint absorbed
	// the duplicate delivery, proving "redelivered, not double-applied".
	assertExactlyOneNotification(t, ctx, pool, order.ID)
}

func assertExactlyOneNotification(t *testing.T, ctx context.Context, pool *pgxpool.Pool, orderID string) {
	t.Helper()
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM order_notifications WHERE order_id = $1`, orderID).Scan(&count); err != nil {
		t.Fatalf("count order_notifications: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 order_notifications row for %s, got %d", orderID, count)
	}
}

// crashOnOrderID simulates a process crash (AfterSideEffect returning an
// error) the first time the event for a specific order id is observed,
// once that event's local transaction has already committed.
type crashOnOrderID struct {
	target string

	mu       sync.Mutex
	isTarget bool
	done     bool
}

func (c *crashOnOrderID) observe(value []byte) {
	id, ok := decodeOrderID(value)
	c.mu.Lock()
	c.isTarget = ok && id == c.target
	c.mu.Unlock()
}

func (c *crashOnOrderID) fired() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

func (c *crashOnOrderID) BeforeSideEffect(context.Context, string, int32, int64) error { return nil }

func (c *crashOnOrderID) AfterSideEffect(context.Context, string, int32, int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isTarget && !c.done {
		c.done = true
		return fmt.Errorf("simulated crash after side effect for order %s", c.target)
	}
	return nil
}

func (c *crashOnOrderID) BeforeOffsetCommit(context.Context, string, int32, int64) error {
	return nil
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func runUntilErrorOrTimeout(ctx context.Context, loop *platform.ConsumerLoop, timeout time.Duration) error {
	runCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return loop.Run(runCtx)
}

func decodeOrderID(value []byte) (string, bool) {
	var event orders.OrderCreated
	if err := json.Unmarshal(value, &event); err != nil || event.OrderID == "" {
		return "", false
	}
	return event.OrderID, true
}

func kafkaBrokersFromEnv() string {
	return getenvIntegration("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
}

func orderEventsTopicFromEnv() string {
	return getenvIntegration("ORDER_EVENTS_TOPIC", "searchsync.outbox.event.orders")
}

func postgresDSNFromEnv() string {
	get := getenvIntegration
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		get("POSTGRES_USER", "searchsync_app"),
		get("POSTGRES_PASSWORD", "changeme"),
		get("POSTGRES_HOST", "localhost"),
		get("POSTGRES_PORT", "5433"),
		get("POSTGRES_DB", "searchsync"))
}

func getenvIntegration(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
