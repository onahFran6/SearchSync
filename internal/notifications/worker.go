package notifications

import (
	"context"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/francisonah/searchsync/internal/platform"
)

// Worker mirrors internal/indexer's Worker: a thin, package-specific
// configuration of the shared platform.ConsumerLoop engine. See that
// package's worker.go for why this is embedding rather than a second copy
// of the retry/DLQ/offset-commit logic.
type Worker struct {
	*platform.ConsumerLoop
}

// NewWorker wires handler.HandleMessage into the shared consumer loop. dlq
// may be nil only in tests that don't exercise the quarantine path;
// production wiring (cmd/notifications) always supplies one.
func NewWorker(client *kgo.Client, handler *Handler, logger *slog.Logger, dlq *platform.DLQProducer, consumerGroup string, metrics platform.MetricsRecorder) *Worker {
	return &Worker{
		ConsumerLoop: &platform.ConsumerLoop{
			Client:        client,
			ConsumerGroup: consumerGroup,
			Process: func(ctx context.Context, record *kgo.Record) error {
				return handler.HandleMessage(ctx, record.Value)
			},
			Classify: classify,
			Retry:    platform.DefaultRetryPolicy,
			DLQ:      dlq,
			Logger:   logger,
			Metrics:  metrics,
		},
	}
}
