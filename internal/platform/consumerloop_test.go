package platform

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// fakeKafkaClient implements KafkaClient without a broker, so handleRecord's
// retry/DLQ/offset-commit decisions can be tested deterministically.
type fakeKafkaClient struct {
	commitErrs []error // consumed in order, one per CommitRecords call; last value sticks
	commits    int
}

func (f *fakeKafkaClient) PollFetches(ctx context.Context) kgo.Fetches { return kgo.Fetches{} }

func (f *fakeKafkaClient) CommitRecords(ctx context.Context, rs ...*kgo.Record) error {
	var err error
	if len(f.commitErrs) > 0 {
		idx := f.commits
		if idx >= len(f.commitErrs) {
			idx = len(f.commitErrs) - 1
		}
		err = f.commitErrs[idx]
	}
	f.commits++
	return err
}

// fakeDLQ implements DLQPublisher without a broker.
type fakeDLQ struct {
	publishErrs []error
	published   []DLQRecord
	calls       int
}

func (f *fakeDLQ) Publish(ctx context.Context, rec DLQRecord) error {
	var err error
	if f.calls < len(f.publishErrs) {
		err = f.publishErrs[f.calls]
	}
	f.calls++
	if err == nil {
		f.published = append(f.published, rec)
	}
	return err
}

// fakeInjector lets each test configure exactly the hook behavior it needs;
// unset funcs behave like NoopFailureInjector.
type fakeInjector struct {
	beforeSideEffect   func(attempt int) error
	afterSideEffect    error
	beforeOffsetCommit error
	beforeAttempt      int
}

func (f *fakeInjector) BeforeSideEffect(ctx context.Context, topic string, partition int32, offset int64) error {
	f.beforeAttempt++
	if f.beforeSideEffect == nil {
		return nil
	}
	return f.beforeSideEffect(f.beforeAttempt)
}

func (f *fakeInjector) AfterSideEffect(ctx context.Context, topic string, partition int32, offset int64) error {
	return f.afterSideEffect
}

func (f *fakeInjector) BeforeOffsetCommit(ctx context.Context, topic string, partition int32, offset int64) error {
	return f.beforeOffsetCommit
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func fastRetryPolicy() RetryPolicy {
	return RetryPolicy{MaxAttempts: 3, BaseDelay: 0, MaxDelay: 0}
}

var errTransientProbe = errors.New("probe: transient")
var errPermanentProbe = errors.New("probe: permanent")

func classifyProbe(err error) FailureKind {
	if errors.Is(err, errPermanentProbe) {
		return FailurePermanent
	}
	return FailureTransient
}

func newTestRecord() *kgo.Record {
	return &kgo.Record{Topic: "t", Partition: 0, Offset: 42, Key: []byte("k"), Value: []byte("v")}
}

func TestHandleRecord_SuccessCommitsOffsetNoDLQ(t *testing.T) {
	calls := 0
	kafka := &fakeKafkaClient{}
	dlq := &fakeDLQ{}
	loop := &ConsumerLoop{
		Client:   kafka,
		Classify: classifyProbe,
		Retry:    fastRetryPolicy(),
		DLQ:      dlq,
		Logger:   testLogger(),
		Process: func(ctx context.Context, record *kgo.Record) error {
			calls++
			return nil
		},
	}

	if err := loop.handleRecord(context.Background(), newTestRecord()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected 1 process call, got %d", calls)
	}
	if kafka.commits != 1 {
		t.Fatalf("expected 1 commit, got %d", kafka.commits)
	}
	if dlq.calls != 0 {
		t.Fatalf("expected no dlq calls, got %d", dlq.calls)
	}
}

// TestHandleRecord_TransientThenSuccess proves a transiently classified
// failure is retried within the attempt budget and the offset still
// commits once it eventually succeeds — "Elasticsearch returns 503" row.
func TestHandleRecord_TransientThenSuccess(t *testing.T) {
	calls := 0
	kafka := &fakeKafkaClient{}
	loop := &ConsumerLoop{
		Client:   kafka,
		Classify: classifyProbe,
		Retry:    fastRetryPolicy(),
		DLQ:      &fakeDLQ{},
		Logger:   testLogger(),
		Process: func(ctx context.Context, record *kgo.Record) error {
			calls++
			if calls < 3 {
				return errTransientProbe
			}
			return nil
		},
	}

	if err := loop.handleRecord(context.Background(), newTestRecord()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 process calls (2 failures + success), got %d", calls)
	}
	if kafka.commits != 1 {
		t.Fatalf("expected 1 commit, got %d", kafka.commits)
	}
}

// TestHandleRecord_PermanentFailureSkipsRetryBudget proves a permanently
// classified failure goes straight to the DLQ instead of spending the
// retry budget — "malformed event" row.
func TestHandleRecord_PermanentFailureSkipsRetryBudget(t *testing.T) {
	calls := 0
	kafka := &fakeKafkaClient{}
	dlq := &fakeDLQ{}
	loop := &ConsumerLoop{
		Client:        kafka,
		ConsumerGroup: "test-group",
		Classify:      classifyProbe,
		Retry:         fastRetryPolicy(),
		DLQ:           dlq,
		Logger:        testLogger(),
		Process: func(ctx context.Context, record *kgo.Record) error {
			calls++
			return errPermanentProbe
		},
	}

	if err := loop.handleRecord(context.Background(), newTestRecord()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected exactly 1 process call for a permanent failure, got %d", calls)
	}
	if dlq.calls != 1 {
		t.Fatalf("expected 1 dlq publish, got %d", dlq.calls)
	}
	if kafka.commits != 1 {
		t.Fatalf("expected offset to still commit after durable dlq placement, got %d", kafka.commits)
	}
	if len(dlq.published) != 1 || dlq.published[0].OriginalOffset != 42 {
		t.Fatalf("dlq record missing original offset: %+v", dlq.published)
	}
}

// TestHandleRecord_TransientExhaustsRetriesGoesToDLQ proves a transient
// failure that never recovers still ends up quarantined rather than
// retried forever.
func TestHandleRecord_TransientExhaustsRetriesGoesToDLQ(t *testing.T) {
	calls := 0
	kafka := &fakeKafkaClient{}
	dlq := &fakeDLQ{}
	loop := &ConsumerLoop{
		Client:   kafka,
		Classify: classifyProbe,
		Retry:    fastRetryPolicy(),
		DLQ:      dlq,
		Logger:   testLogger(),
		Process: func(ctx context.Context, record *kgo.Record) error {
			calls++
			return errTransientProbe
		},
	}

	if err := loop.handleRecord(context.Background(), newTestRecord()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected process to be attempted MaxAttempts=3 times, got %d", calls)
	}
	if dlq.calls != 1 {
		t.Fatalf("expected 1 dlq publish after exhausting retries, got %d", dlq.calls)
	}
	if kafka.commits != 1 {
		t.Fatalf("expected offset to commit after durable dlq placement, got %d", kafka.commits)
	}
}

// TestHandleRecord_DLQPublishFailureBlocksOffsetCommit proves the
// "never commit a poison record's offset without a recorded DLQ placement
// first" constraint: if the DLQ itself cannot be written to, the consumer
// must stop rather than silently advance.
func TestHandleRecord_DLQPublishFailureBlocksOffsetCommit(t *testing.T) {
	kafka := &fakeKafkaClient{}
	dlq := &fakeDLQ{publishErrs: []error{errors.New("dlq broker down"), errors.New("dlq broker down"), errors.New("dlq broker down")}}
	loop := &ConsumerLoop{
		Client:   kafka,
		Classify: classifyProbe,
		Retry:    fastRetryPolicy(),
		DLQ:      dlq,
		Logger:   testLogger(),
		Process: func(ctx context.Context, record *kgo.Record) error {
			return errPermanentProbe
		},
	}

	err := loop.handleRecord(context.Background(), newTestRecord())
	if err == nil {
		t.Fatal("expected error when dlq publish cannot be durably acknowledged")
	}
	if kafka.commits != 0 {
		t.Fatalf("offset must not commit when dlq placement failed, got %d commits", kafka.commits)
	}
}

// TestHandleRecord_NoDLQConfiguredFailsClosed proves there is no silent
// drop path: without a DLQ, an unrecoverable record is a hard stop, not a
// skip.
func TestHandleRecord_NoDLQConfiguredFailsClosed(t *testing.T) {
	kafka := &fakeKafkaClient{}
	loop := &ConsumerLoop{
		Client:   kafka,
		Classify: classifyProbe,
		Retry:    fastRetryPolicy(),
		DLQ:      nil,
		Logger:   testLogger(),
		Process: func(ctx context.Context, record *kgo.Record) error {
			return errPermanentProbe
		},
	}

	if err := loop.handleRecord(context.Background(), newTestRecord()); err == nil {
		t.Fatal("expected error with no dlq configured")
	}
	if kafka.commits != 0 {
		t.Fatalf("offset must not commit without a dlq, got %d commits", kafka.commits)
	}
}

// TestHandleRecord_CrashAfterSideEffectBeforeCommit is the headline
// scenario: the side effect succeeded, but the process "crashes" (the
// injector simulates it) before the offset commits. ConsumerLoop must
// return without committing, so a restart redelivers this exact record —
// idempotency in Process is what then makes that redelivery a no-op, not
// ConsumerLoop itself.
func TestHandleRecord_CrashAfterSideEffectBeforeCommit(t *testing.T) {
	processCalls := 0
	kafka := &fakeKafkaClient{}
	loop := &ConsumerLoop{
		Client:   kafka,
		Classify: classifyProbe,
		Retry:    fastRetryPolicy(),
		DLQ:      &fakeDLQ{},
		Logger:   testLogger(),
		Injector: &fakeInjector{afterSideEffect: errors.New("simulated crash")},
		Process: func(ctx context.Context, record *kgo.Record) error {
			processCalls++
			return nil
		},
	}

	if err := loop.handleRecord(context.Background(), newTestRecord()); err == nil {
		t.Fatal("expected simulated-crash error to propagate")
	}
	if processCalls != 1 {
		t.Fatalf("expected the side effect to have actually run once, got %d", processCalls)
	}
	if kafka.commits != 0 {
		t.Fatalf("offset must not commit when the crash is simulated between side effect and commit, got %d", kafka.commits)
	}

	// Simulate the restart: a fresh loop (no injector) redelivers the same
	// record. Process must be safe to call again — ConsumerLoop does not
	// itself dedupe, that's the handler's job, but the commit must now
	// succeed since nothing is simulating a crash this time.
	kafka2 := &fakeKafkaClient{}
	restarted := &ConsumerLoop{
		Client:   kafka2,
		Classify: classifyProbe,
		Retry:    fastRetryPolicy(),
		DLQ:      &fakeDLQ{},
		Logger:   testLogger(),
		Process: func(ctx context.Context, record *kgo.Record) error {
			processCalls++
			return nil
		},
	}
	if err := restarted.handleRecord(context.Background(), newTestRecord()); err != nil {
		t.Fatalf("unexpected error on redelivery: %v", err)
	}
	if processCalls != 2 {
		t.Fatalf("expected redelivery to reprocess (duplicate call), got %d total calls", processCalls)
	}
	if kafka2.commits != 1 {
		t.Fatalf("expected redelivery to commit once recovery succeeds, got %d", kafka2.commits)
	}
}

// TestHandleRecord_CrashBeforeOffsetCommit proves the third hook's
// distinct timing: the commit RPC itself is where the injected failure
// happens, and ConsumerLoop still must not commit.
func TestHandleRecord_CrashBeforeOffsetCommit(t *testing.T) {
	kafka := &fakeKafkaClient{}
	loop := &ConsumerLoop{
		Client:   kafka,
		Classify: classifyProbe,
		Retry:    fastRetryPolicy(),
		DLQ:      &fakeDLQ{},
		Logger:   testLogger(),
		Injector: &fakeInjector{beforeOffsetCommit: errors.New("simulated commit RPC failure")},
		Process: func(ctx context.Context, record *kgo.Record) error {
			return nil
		},
	}

	if err := loop.handleRecord(context.Background(), newTestRecord()); err == nil {
		t.Fatal("expected error")
	}
	if kafka.commits != 0 {
		t.Fatalf("offset must not commit, got %d", kafka.commits)
	}
}

// TestHandleRecord_BeforeSideEffectInjectionRoutesThroughRetry proves the
// "before processing" instrumentation point shares the same
// classify/retry machinery as a normal processing failure, rather than
// being a special case.
func TestHandleRecord_BeforeSideEffectInjectionRoutesThroughRetry(t *testing.T) {
	kafka := &fakeKafkaClient{}
	processCalls := 0
	loop := &ConsumerLoop{
		Client:   kafka,
		Classify: classifyProbe,
		Retry:    fastRetryPolicy(),
		DLQ:      &fakeDLQ{},
		Logger:   testLogger(),
		Injector: &fakeInjector{beforeSideEffect: func(attempt int) error {
			if attempt == 1 {
				return errTransientProbe
			}
			return nil
		}},
		Process: func(ctx context.Context, record *kgo.Record) error {
			processCalls++
			return nil
		},
	}

	if err := loop.handleRecord(context.Background(), newTestRecord()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if processCalls != 1 {
		t.Fatalf("process should only run once the injected pre-side-effect failure clears, got %d calls", processCalls)
	}
	if kafka.commits != 1 {
		t.Fatalf("expected 1 commit, got %d", kafka.commits)
	}
}

func TestRetryPolicy_BackoffNeverExceedsMaxDelay(t *testing.T) {
	policy := RetryPolicy{MaxAttempts: 10, BaseDelay: 100 * time.Millisecond, MaxDelay: 1000 * time.Millisecond}
	for attempt := 1; attempt <= 10; attempt++ {
		for i := 0; i < 20; i++ {
			d := policy.Backoff(attempt)
			if d < 0 || d > policy.MaxDelay {
				t.Fatalf("attempt %d: backoff %v out of [0, %v]", attempt, d, policy.MaxDelay)
			}
		}
	}
}
