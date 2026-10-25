package payments

import (
	"context"
	"errors"
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

// Worker subscribes to the single AuthorizePayment topic - one message
// type, no header inspection needed.
type Worker struct {
	*platform.ConsumerLoop
}

func NewWorker(client *kgo.Client, service *Service, logger *slog.Logger, dlq *platform.DLQProducer, consumerGroup string, metrics platform.MetricsRecorder) *Worker {
	return &Worker{
		ConsumerLoop: &platform.ConsumerLoop{
			Client:        client,
			ConsumerGroup: consumerGroup,
			Process: func(ctx context.Context, record *kgo.Record) error {
				return service.HandleAuthorizePayment(ctx, record.Value)
			},
			Classify: classify,
			Retry:    platform.DefaultRetryPolicy,
			DLQ:      dlq,
			Logger:   logger,
			Metrics:  metrics,
		},
	}
}
