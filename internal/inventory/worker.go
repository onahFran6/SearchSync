package inventory

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/onahFran6/SearchSync/internal/platform"
)

// permanentPostgresCodes mirrors internal/notifications' classification:
// these mean the input itself is invalid, not that the database is
// briefly unavailable.
var permanentPostgresCodes = map[string]bool{
	"23502": true, // not_null_violation
	"23514": true, // check_violation
	"22P02": true, // invalid_text_representation
	"23503": true, // foreign_key_violation
}

// classify treats the dev-injected failure as transient by design (see
// service.go), checks ErrPermanent next, then falls back to Postgres
// error codes exactly like internal/notifications.classify.
func classify(err error) platform.FailureKind {
	if errors.Is(err, errDevInjectedFailure) {
		return platform.FailureTransient
	}
	if errors.Is(err, ErrPermanent) {
		return platform.FailurePermanent
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && permanentPostgresCodes[pgErr.Code] {
		return platform.FailurePermanent
	}
	return platform.FailureTransient
}

// Worker is a thin platform.ConsumerLoop configuration, same shape as
// internal/notifications.Worker. It subscribes to both of Inventory's
// command topics on one client/consumer group and dispatches on
// record.Topic - each topic carries exactly one message type (see
// outbox_types.go), so no header inspection is needed to know which.
type Worker struct {
	*platform.ConsumerLoop
}

func NewWorker(client *kgo.Client, service *Service, reserveTopic, releaseTopic string, logger *slog.Logger, dlq *platform.DLQProducer, consumerGroup string, metrics platform.MetricsRecorder) *Worker {
	return &Worker{
		ConsumerLoop: &platform.ConsumerLoop{
			Client:        client,
			ConsumerGroup: consumerGroup,
			Process: func(ctx context.Context, record *kgo.Record) error {
				switch record.Topic {
				case reserveTopic:
					return service.HandleReserveInventory(ctx, record.Value)
				case releaseTopic:
					return service.HandleReleaseInventory(ctx, record.Value)
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
