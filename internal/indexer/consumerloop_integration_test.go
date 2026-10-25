//go:build integration

package indexer

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

	"github.com/francisonah/searchsync/internal/platform"
	"github.com/francisonah/searchsync/internal/search"
)

// TestConsumerLoop_CrashAfterSideEffectBeforeCommit_SurvivesRestart
// reproduces the headline crash-recovery scenario against the live Compose stack
// (real Postgres -> Debezium -> Kafka -> Elasticsearch), not just the
// in-memory fakes in internal/platform/consumerloop_test.go: a fresh
// product row is written, the indexer's real ConsumerLoop runs with a
// FailureInjector that simulates a crash right after that specific
// record's Elasticsearch write succeeds but before the Kafka offset
// commits, and a second ConsumerLoop (a fresh *kgo.Client standing in for
// a process restart, same consumer group so it resumes from the last
// committed offset) picks the redelivered record back up. The proof is
// that the crash didn't lose the write and the redelivery converged to the
// same correct state without any special-casing.
//
// Run against the Compose stack: go test -tags=integration ./internal/indexer/...
func TestConsumerLoop_CrashAfterSideEffectBeforeCommit_SurvivesRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, postgresDSNFromEnv())
	if err != nil || pool.Ping(ctx) != nil {
		t.Skipf("postgres not reachable, skipping integration test: %v", err)
	}
	defer pool.Close()

	esClient, err := platform.NewElasticsearchClient(elasticsearchURLFromEnv())
	if err != nil {
		t.Fatalf("connect elasticsearch: %v", err)
	}
	searchClient := search.NewClient(esClient, productsIndexFromEnv())

	sku := "CRASH-TEST-" + uuid.New().String()[:8]
	var productID int64
	err = pool.QueryRow(ctx, `
		INSERT INTO products (sku, name, description, category, price_cents)
		VALUES ($1, 'crash recovery test product', 'd', 'test', 12345)
		RETURNING id`, sku).Scan(&productID)
	if err != nil {
		t.Fatalf("insert test product: %v", err)
	}
	documentID := fmt.Sprintf("%d", productID)
	defer func() {
		_, _ = pool.Exec(ctx, `DELETE FROM products WHERE id = $1`, productID)
		_ = searchClient.DeleteDocument(context.Background(), documentID, 1<<62)
	}()

	group := "stage3-crash-test-" + uuid.New().String()
	brokers := strings.Split(kafkaBrokersFromEnv(), ",")
	topic := productsTopicFromEnv()

	handler := NewHandler(searchClient)
	injector := &crashOnProductID{target: productID}

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
		t.Fatal("expected the injected crash to have fired; the test record was never observed in time - check Debezium/Connect lag")
	}
	if runErr == nil {
		t.Fatal("expected the simulated crash to surface as an error from Run")
	}

	// The side effect must have actually landed despite the "crash".
	doc, found, err := searchClient.GetDocument(ctx, documentID)
	if err != nil {
		t.Fatalf("fetch document after simulated crash: %v", err)
	}
	if !found || doc.PriceCents != 12345 {
		t.Fatalf("expected the side effect to be durably applied before the simulated crash, found=%v doc=%+v", found, doc)
	}

	// Restart: a fresh client, same group, no injector. It must resume
	// from the last committed offset (before our record) and reapply it.
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
			if id, ok := decodeProductID(record.Value); ok && id == productID {
				reprocessed.Store(productID, true)
			}
			return handler.HandleMessage(ctx, record.Value)
		},
	}
	_ = runUntilErrorOrTimeout(ctx, secondLoop, 20*time.Second)
	if _, ok := reprocessed.Load(productID); !ok {
		t.Fatal("expected the restarted consumer to redeliver and reprocess the record the crash prevented from committing")
	}

	doc, found, err = searchClient.GetDocument(ctx, documentID)
	if err != nil {
		t.Fatalf("fetch document after restart: %v", err)
	}
	if !found || doc.PriceCents != 12345 {
		t.Fatalf("expected final state correct after duplicate-safe redelivery, found=%v doc=%+v", found, doc)
	}
}

// crashOnProductID simulates a process crash (AfterSideEffect returning an
// error) the first time the record for a specific product id is observed,
// once that record's Elasticsearch write has already happened. observe
// must be called from Process (which does see the record's value, unlike
// the FailureInjector hooks) so AfterSideEffect knows whether *this*
// record is the one to crash on.
type crashOnProductID struct {
	target int64

	mu       sync.Mutex
	isTarget bool
	done     bool
}

func (c *crashOnProductID) observe(value []byte) {
	id, ok := decodeProductID(value)
	c.mu.Lock()
	c.isTarget = ok && id == c.target
	c.mu.Unlock()
}

func (c *crashOnProductID) fired() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

func (c *crashOnProductID) BeforeSideEffect(context.Context, string, int32, int64) error { return nil }

func (c *crashOnProductID) AfterSideEffect(context.Context, string, int32, int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.isTarget && !c.done {
		c.done = true
		return fmt.Errorf("simulated crash after side effect for product %d", c.target)
	}
	return nil
}

func (c *crashOnProductID) BeforeOffsetCommit(context.Context, string, int32, int64) error {
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

func decodeProductID(value []byte) (int64, bool) {
	var env struct {
		After *struct {
			ID int64 `json:"id"`
		} `json:"after"`
	}
	if err := json.Unmarshal(value, &env); err != nil || env.After == nil {
		return 0, false
	}
	return env.After.ID, true
}

func kafkaBrokersFromEnv() string {
	return getenvIntegration("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
}

func productsTopicFromEnv() string {
	return getenvIntegration("PRODUCTS_TOPIC", "searchsync.public.products")
}

func elasticsearchURLFromEnv() string {
	return getenvIntegration("ELASTICSEARCH_URL", "http://localhost:9200")
}

func productsIndexFromEnv() string {
	return getenvIntegration("PRODUCTS_INDEX", "products")
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
