package saga

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/francisonah/searchsync/internal/platform"
)

var permanentPostgresCodes = map[string]bool{
	"23502": true, // not_null_violation
	"23514": true, // check_violation
	"22P02": true, // invalid_text_representation
	"23503": true, // foreign_key_violation
}

func classify(err error) platform.FailureKind {
	if errors.Is(err, ErrPermanent) {
		return platform.FailurePermanent
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && permanentPostgresCodes[pgErr.Code] {
		return platform.FailurePermanent
	}
	return platform.FailureTransient
}

// Topics bundles every topic the coordinator subscribes to, so
// cmd/saga-coordinator only has to build one of these from config instead
// of passing seven separate strings around.
type Topics struct {
	OrderCreated           string
	InventoryReserved      string
	InventoryRejected      string
	InventoryReleased      string
	InventoryReleaseFailed string
	PaymentAuthorized      string
	PaymentDeclined        string
}

// Worker subscribes to all seven of the coordinator's input topics on one
// client/consumer group and dispatches on record.Topic - each topic
// carries exactly one message type (see outbox_types.go in this package,
// internal/inventory, and internal/payments), so no header inspection is
// needed.
type Worker struct {
	*platform.ConsumerLoop
}

func NewWorker(client *kgo.Client, service *Service, topics Topics, logger *slog.Logger, dlq *platform.DLQProducer, consumerGroup string, metrics platform.MetricsRecorder) *Worker {
	return &Worker{
		ConsumerLoop: &platform.ConsumerLoop{
			Client:        client,
			ConsumerGroup: consumerGroup,
			Process: func(ctx context.Context, record *kgo.Record) error {
				switch record.Topic {
				case topics.OrderCreated:
					return service.HandleOrderCreated(ctx, record.Value)
				case topics.InventoryReserved:
					return service.HandleInventoryReserved(ctx, record.Value)
				case topics.InventoryRejected:
					return service.HandleInventoryRejected(ctx, record.Value)
				case topics.InventoryReleased:
					return service.HandleInventoryReleased(ctx, record.Value)
				case topics.InventoryReleaseFailed:
					return service.HandleInventoryReleaseFailed(ctx, record.Value)
				case topics.PaymentAuthorized:
					return service.HandlePaymentAuthorized(ctx, record.Value)
				case topics.PaymentDeclined:
					return service.HandlePaymentDeclined(ctx, record.Value)
				default:
					return fmt.Errorf("%w: unexpected topic %s", ErrPermanent, record.Topic)
				}
			},
			Classify: classify,
			Retry:    platform.DefaultRetryPolicy,
			DLQ:      dlq,
			Logger:   logger,
			Metrics:  metrics,
		},
	}
}

// Topics as a slice, for subscribing the Kafka client to all of them.
func (t Topics) Slice() []string {
	return []string{
		t.OrderCreated, t.InventoryReserved, t.InventoryRejected,
		t.InventoryReleased, t.InventoryReleaseFailed,
		t.PaymentAuthorized, t.PaymentDeclined,
	}
}
