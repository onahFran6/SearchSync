package orders

import (
	"context"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/francisonah/searchsync/internal/platform"
)

// SagaWorker is a thin platform.ConsumerLoop configuration for
// SagaTransitioned notifications, same shape as internal/notifications'
// Worker. It runs inside cmd/orders alongside the HTTP server, not as a
// separate binary - see cmd/orders/main.go's comment on why.
type SagaWorker struct {
	*platform.ConsumerLoop
}

func NewSagaWorker(client *kgo.Client, handler *SagaHandler, logger *slog.Logger, dlq *platform.DLQProducer, consumerGroup string) *SagaWorker {
	return &SagaWorker{
		ConsumerLoop: &platform.ConsumerLoop{
			Client:        client,
			ConsumerGroup: consumerGroup,
			Process: func(ctx context.Context, record *kgo.Record) error {
				return handler.HandleMessage(ctx, record.Value)
			},
			Classify: ClassifySagaNotification,
			Retry:    platform.DefaultRetryPolicy,
			DLQ:      dlq,
			Logger:   logger,
		},
	}
}
