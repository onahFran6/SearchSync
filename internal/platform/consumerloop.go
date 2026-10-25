package platform

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// tracerName identifies spans ConsumerLoop starts, in the OpenTelemetry
// convention of using the instrumented package's import path.
const tracerName = "github.com/francisonah/searchsync/internal/platform"

// ProcessFunc applies one Kafka record's side effect. It must be
// idempotent under at-least-once redelivery: ConsumerLoop guarantees a
// crash can only produce a duplicate call to ProcessFunc for the same
// record, never a silent skip.
type ProcessFunc func(ctx context.Context, record *kgo.Record) error

// KafkaClient is the subset of *kgo.Client ConsumerLoop needs. It exists so
// the retry/DLQ/offset-commit decision logic in handleRecord can be
// unit-tested with a fake broker-free implementation; *kgo.Client satisfies
// this interface unmodified, so production wiring is unaffected. Building
// a fake kgo.Fetches for Run's poll loop itself is impractical (its fields
// are not constructible outside the kgo package), so Run is covered by the
// //go:build integration tests against a live broker instead.
type KafkaClient interface {
	PollFetches(ctx context.Context) kgo.Fetches
	CommitRecords(ctx context.Context, rs ...*kgo.Record) error
}

// DLQPublisher is the subset of *DLQProducer ConsumerLoop needs, broken out
// so tests can assert quarantine behavior (and simulate a DLQ outage)
// without a live Kafka broker. *DLQProducer satisfies this unmodified.
type DLQPublisher interface {
	Publish(ctx context.Context, rec DLQRecord) error
}

// MetricsRecorder is the subset of internal/observability.Metrics
// ConsumerLoop needs. It is defined here, not imported from observability,
// for the same reason KafkaClient/DLQPublisher are defined here: tests
// construct a fake without any Prometheus dependency, and
// *observability.Metrics satisfies it unmodified. This is the single
// instrumentation point for events_processed_total, events_failed_total,
// event_processing_duration_seconds, and event_retry_attempts - every
// consumer gets them from one place rather than six copies.
type MetricsRecorder interface {
	ObserveProcessed(group, topic, result string)
	ObserveFailed(group, topic, kind string)
	ObserveProcessingDuration(group, topic string, d time.Duration)
	ObserveRetryAttempts(group, topic string, attempts int)
	ObserveCommitDuration(group string, d time.Duration)
}

type noopMetricsRecorder struct{}

func (noopMetricsRecorder) ObserveProcessed(string, string, string)                 {}
func (noopMetricsRecorder) ObserveFailed(string, string, string)                    {}
func (noopMetricsRecorder) ObserveProcessingDuration(string, string, time.Duration) {}
func (noopMetricsRecorder) ObserveRetryAttempts(string, string, int)                {}
func (noopMetricsRecorder) ObserveCommitDuration(string, time.Duration)             {}

// kafkaHeaderCarrier adapts a Kafka record's headers to
// propagation.TextMapCarrier so ConsumerLoop can extract a W3C traceparent
// that Debezium's outbox EventRouter placed there as a literal header,
// without pulling in a third-party Kafka instrumentation library for
// franz-go. It is
// extraction-only: ConsumerLoop never produces a record, so Set is a no-op.
type kafkaHeaderCarrier []kgo.RecordHeader

func (c kafkaHeaderCarrier) Get(key string) string {
	for _, h := range c {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c kafkaHeaderCarrier) Set(string, string) {}

func (c kafkaHeaderCarrier) Keys() []string {
	keys := make([]string, len(c))
	for i, h := range c {
		keys[i] = h.Key
	}
	return keys
}

// dlqRetryPolicy and commitRetryPolicy are deliberately smaller than a
// consumer's own processing retry budget: both the DLQ topic and the
// consumer's own group coordinator are Kafka itself, so a prolonged outage
// here means the whole broker is unavailable, not a slow downstream
// dependency, and the loop should fail fast and let the operator notice.
var (
	dlqRetryPolicy    = RetryPolicy{MaxAttempts: 3, BaseDelay: 250 * time.Millisecond, MaxDelay: 5 * time.Second}
	commitRetryPolicy = RetryPolicy{MaxAttempts: 3, BaseDelay: 250 * time.Millisecond, MaxDelay: 5 * time.Second}
)

// alwaysTransient classifies every error as transient except context
// cancellation/deadline, which retryLoop's Sleep call would immediately
// turn into a stop anyway. It is used for the DLQ-publish and
// offset-commit retry loops, where "permanent" has no meaning (a produce
// or commit call either works or the broker/network is down).
func alwaysTransient(err error) FailureKind {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return FailurePermanent
	}
	return FailureTransient
}

// retryLoop calls fn (1-indexed attempt number) until it returns nil, a
// permanently classified error, or the policy's attempt budget is
// exhausted, sleeping with backoff+jitter between attempts.
func retryLoop(ctx context.Context, policy RetryPolicy, classify ClassifyFunc, onAttemptFailed func(attempt int, kind FailureKind, err error), fn func(attempt int) error) error {
	var err error
	for attempt := 1; attempt <= policy.MaxAttempts; attempt++ {
		err = fn(attempt)
		if err == nil {
			return nil
		}
		kind := classify(err)
		if onAttemptFailed != nil {
			onAttemptFailed(attempt, kind, err)
		}
		if kind == FailurePermanent || attempt == policy.MaxAttempts {
			return err
		}
		if sleepErr := Sleep(ctx, policy.Backoff(attempt)); sleepErr != nil {
			return sleepErr
		}
	}
	return err
}

// ConsumerLoop is the shared poll -> process -> (retry | DLQ) -> commit
// engine used by both internal/indexer and internal/notifications. Each
// package supplies its own Process (the actual side effect) and Classify
// (what counts as permanent for that side effect); everything about retry
// bounds, DLQ durability, failure injection, and offset-commit discipline
// lives here exactly once.
type ConsumerLoop struct {
	Client        KafkaClient
	ConsumerGroup string
	Process       ProcessFunc
	Classify      ClassifyFunc
	Retry         RetryPolicy
	DLQ           DLQPublisher
	Injector      FailureInjector
	Logger        *slog.Logger
	Metrics       MetricsRecorder
}

func (l *ConsumerLoop) injector() FailureInjector {
	if l.Injector == nil {
		return NoopFailureInjector{}
	}
	return l.Injector
}

func (l *ConsumerLoop) logger() *slog.Logger {
	if l.Logger == nil {
		return slog.Default()
	}
	return l.Logger
}

func (l *ConsumerLoop) metrics() MetricsRecorder {
	if l.Metrics == nil {
		return noopMetricsRecorder{}
	}
	return l.Metrics
}

func failureKindLabel(k FailureKind) string {
	if k == FailurePermanent {
		return "permanent"
	}
	return "transient"
}

// Run processes assigned partitions strictly in the order PollFetches
// returns them, one record at a time, in a single goroutine. This is a
// deliberate trade-off: a record stuck retrying or being DLQ'd
// head-of-line-blocks every other record in the same fetch batch,
// including ones from other partitions, in exchange for never having to
// reason about concurrent reordering.
func (l *ConsumerLoop) Run(ctx context.Context) error {
	for {
		fetches := l.Client.PollFetches(ctx)
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
			if err := l.handleRecord(ctx, record); err != nil {
				return err
			}
		}
	}
}

func (l *ConsumerLoop) handleRecord(ctx context.Context, record *kgo.Record) error {
	injector := l.injector()
	logger := l.logger()
	metrics := l.metrics()

	ctx = otel.GetTextMapPropagator().Extract(ctx, kafkaHeaderCarrier(record.Headers))
	ctx, span := otel.Tracer(tracerName).Start(ctx, l.ConsumerGroup+" process", trace.WithAttributes(
		attribute.String("messaging.system", "kafka"),
		attribute.String("messaging.destination.name", record.Topic),
		attribute.Int64("messaging.kafka.destination.partition", int64(record.Partition)),
		attribute.Int64("messaging.kafka.message.offset", record.Offset),
	))
	defer span.End()

	start := time.Now()
	attempts := 0
	processErr := retryLoop(ctx, l.Retry, l.Classify, func(attempt int, kind FailureKind, err error) {
		attempts = attempt
		metrics.ObserveFailed(l.ConsumerGroup, record.Topic, failureKindLabel(kind))
		logger.Warn("process attempt failed",
			"topic", record.Topic, "partition", record.Partition, "offset", record.Offset,
			"attempt", attempt, "transient", kind == FailureTransient, "error", err)
	}, func(attempt int) error {
		if err := injector.BeforeSideEffect(ctx, record.Topic, record.Partition, record.Offset); err != nil {
			return err
		}
		return l.Process(ctx, record)
	})
	if attempts == 0 {
		attempts = 1
	}
	metrics.ObserveProcessingDuration(l.ConsumerGroup, record.Topic, time.Since(start))
	metrics.ObserveRetryAttempts(l.ConsumerGroup, record.Topic, attempts)

	if processErr != nil {
		span.RecordError(processErr)
		span.SetStatus(codes.Error, processErr.Error())
		if err := l.quarantine(ctx, record, processErr, attempts); err != nil {
			return err
		}
	}

	if err := injector.AfterSideEffect(ctx, record.Topic, record.Partition, record.Offset); err != nil {
		return fmt.Errorf("simulated crash after side effect on %s[%d]@%d: %w",
			record.Topic, record.Partition, record.Offset, err)
	}

	if err := injector.BeforeOffsetCommit(ctx, record.Topic, record.Partition, record.Offset); err != nil {
		return fmt.Errorf("simulated failure before offset commit on %s[%d]@%d: %w",
			record.Topic, record.Partition, record.Offset, err)
	}

	commitStart := time.Now()
	if err := retryLoop(ctx, commitRetryPolicy, alwaysTransient, func(attempt int, _ FailureKind, err error) {
		logger.Warn("commit attempt failed",
			"topic", record.Topic, "partition", record.Partition, "offset", record.Offset,
			"attempt", attempt, "error", err)
	}, func(int) error {
		return l.Client.CommitRecords(ctx, record)
	}); err != nil {
		return fmt.Errorf("commit record %s[%d]@%d: %w", record.Topic, record.Partition, record.Offset, err)
	}
	metrics.ObserveCommitDuration(l.ConsumerGroup, time.Since(commitStart))

	result := "ok"
	if processErr != nil {
		result = "quarantined"
	}
	metrics.ObserveProcessed(l.ConsumerGroup, record.Topic, result)

	logger.Info("processed record",
		"topic", record.Topic, "partition", record.Partition, "offset", record.Offset,
		"key", string(record.Key), "quarantined", processErr != nil)
	return nil
}

// quarantine durably places a record that is either permanently
// unprocessable or has exhausted its retry budget onto this consumer's
// DLQ topic. It only returns nil once the DLQ broker has acknowledged the
// write — the caller must not commit the original offset otherwise.
func (l *ConsumerLoop) quarantine(ctx context.Context, record *kgo.Record, cause error, attempts int) error {
	if l.DLQ == nil {
		return fmt.Errorf("unrecoverable error on %s[%d]@%d, no DLQ configured, refusing to advance offset: %w",
			record.Topic, record.Partition, record.Offset, cause)
	}

	logger := l.logger()
	dlqErr := retryLoop(ctx, dlqRetryPolicy, alwaysTransient, func(attempt int, _ FailureKind, err error) {
		logger.Warn("dlq publish attempt failed",
			"topic", record.Topic, "partition", record.Partition, "offset", record.Offset,
			"attempt", attempt, "error", err)
	}, func(int) error {
		return l.DLQ.Publish(ctx, DLQRecord{
			OriginalTopic:     record.Topic,
			OriginalPartition: record.Partition,
			OriginalOffset:    record.Offset,
			Key:               record.Key,
			Value:             record.Value,
			Reason:            cause.Error(),
			Attempts:          attempts,
			FailedAt:          time.Now(),
			ConsumerGroup:     l.ConsumerGroup,
		})
	})
	if dlqErr != nil {
		return fmt.Errorf("could not durably quarantine %s[%d]@%d, refusing to commit offset: %w",
			record.Topic, record.Partition, record.Offset, dlqErr)
	}

	logger.Error("quarantined record to dlq",
		"topic", record.Topic, "partition", record.Partition, "offset", record.Offset,
		"attempts", attempts, "reason", cause.Error())
	return nil
}
