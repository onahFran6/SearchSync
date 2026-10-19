package indexer

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

// Run polls for records and processes them strictly in delivery order,
// committing each record's offset only after it has been successfully
// applied to Elasticsearch. On the first processing or commit failure, Run
// returns without committing that (or any later) offset, so a restart
// resumes from the last successfully committed position and retries the
// failed record rather than silently skipping it. Bounded retries, DLQ
// routing, and continuing past a single bad record are Stage 3 concerns;
// Stage 1 trades availability for the simplest correct at-least-once
// guarantee.
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
				return fmt.Errorf("index record %s[%d]@%d: %w", record.Topic, record.Partition, record.Offset, err)
			}

			if err := w.client.CommitRecords(ctx, record); err != nil {
				return fmt.Errorf("commit record %s[%d]@%d: %w", record.Topic, record.Partition, record.Offset, err)
			}

			w.logger.Info("indexed record",
				"topic", record.Topic, "partition", record.Partition, "offset", record.Offset,
				"key", string(record.Key))
		}
	}
}
