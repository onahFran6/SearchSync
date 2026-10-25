package indexer

import (
	"context"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/onahFran6/SearchSync/internal/platform"
)

// Worker is a thin, package-specific configuration of platform.ConsumerLoop:
// the retry/DLQ/offset-commit engine and FailureInjector hooks live there
// exactly once, shared with internal/notifications. Embedding means tests
// can set Worker.Injector directly to reproduce a failure-matrix row
// deterministically (see worker_test.go).
type Worker struct {
	*platform.ConsumerLoop
}

// NewWorker wires handler.HandleMessage into the shared consumer loop. dlq
// may be nil only in tests that don't exercise the quarantine path;
// production wiring (cmd/indexer) always supplies one, since the
// correctness guarantee is that a permanently unprocessable record is
// quarantined, never silently dropped.
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
