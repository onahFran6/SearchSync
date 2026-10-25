package observability

import (
	"context"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// RunLagPoller periodically reads one consumer group's lag (messages
// behind, summed per topic) and, if dlqTopics is non-empty, each DLQ
// topic's total backlog (last-produced offset - no consumer group ever
// reads a DLQ topic, so there is no "committed offset" to subtract) and
// feeds both into metrics. It reuses the consumer's own already-open
// *kgo.Client for these metadata-only admin calls rather than standing up
// a separate kafka_exporter container, and blocks until ctx is done.
func RunLagPoller(ctx context.Context, client *kgo.Client, group string, dlqTopics []string, metrics *Metrics, interval time.Duration, logger *slog.Logger) {
	admin := kadm.NewClient(client)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pollLagOnce(ctx, admin, group, dlqTopics, metrics, logger)
		}
	}
}

func pollLagOnce(ctx context.Context, admin *kadm.Client, group string, dlqTopics []string, metrics *Metrics, logger *slog.Logger) {
	lags, err := admin.Lag(ctx, group)
	if err != nil {
		logger.Warn("consumer lag poll failed", "group", group, "error", err)
	} else if l, ok := lags[group]; ok && l.Error() == nil {
		for _, tl := range l.Lag.TotalByTopic() {
			metrics.SetConsumerLag(group, tl.Topic, float64(tl.Lag))
		}
	}

	if len(dlqTopics) == 0 {
		return
	}
	offsets, err := admin.ListEndOffsets(ctx, dlqTopics...)
	if err != nil {
		logger.Warn("dlq depth poll failed", "group", group, "error", err)
		return
	}
	var total int64
	offsets.Each(func(o kadm.ListedOffset) {
		if o.Offset > 0 {
			total += o.Offset
		}
	})
	metrics.SetDLQDepth(group, float64(total))
}
