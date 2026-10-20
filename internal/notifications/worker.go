package notifications

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Worker struct {
	client  *kgo.Client
	handler *Handler
	logger  *slog.Logger
}

func NewWorker(client *kgo.Client, handler *Handler, logger *slog.Logger) *Worker {
	return &Worker{client: client, handler: handler, logger: logger}
}

// Run mirrors internal/indexer's Worker: process strictly in delivery order
// and commit each record's offset only after its local transaction has
// committed, so a restart resumes from the last durably-applied event and
// retries (rather than skips) whatever failed.
func (w *Worker) Run(ctx context.Context) error {
	for {
		fetches := w.client.PollFetches(ctx)
		if err := ctx.Err(); err != nil {
			return err
		}

		var fetchErr error
		fetches.EachError(func(topic string, partition int32, err error) {
			fetchErr = fmt.Errorf("fetch error on %s[%d]: %w", topic, partition, err)
		})
		if fetchErr != nil {
			return fetchErr
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()

			if err := w.handler.HandleMessage(ctx, record.Value); err != nil {
				return fmt.Errorf("process record %s[%d]@%d: %w", record.Topic, record.Partition, record.Offset, err)
			}

			if err := w.client.CommitRecords(ctx, record); err != nil {
				return fmt.Errorf("commit record %s[%d]@%d: %w", record.Topic, record.Partition, record.Offset, err)
			}

			w.logger.Info("processed order event",
				"topic", record.Topic, "partition", record.Partition, "offset", record.Offset,
				"key", string(record.Key))
		}
	}
}
