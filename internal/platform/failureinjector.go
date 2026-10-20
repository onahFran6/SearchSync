package platform

import "context"

// FailureInjector lets a test deterministically reproduce a crash at one of
// the three meaningful points in ConsumerLoop's per-record lifecycle,
// instead of relying on manually timed `docker stop` races. Production
// wiring always uses NoopFailureInjector; every non-trivial implementation
// lives in a _test.go file, so these hooks never do anything outside `go
// test`.
type FailureInjector interface {
	// BeforeSideEffect runs immediately before the record is handed to
	// the consumer's handler. Returning an error stands in for a crash
	// (or any failure) before the side effect is attempted at all, and
	// is treated as an ordinary processing failure: it goes through the
	// normal classify/retry/DLQ path.
	BeforeSideEffect(ctx context.Context, topic string, partition int32, offset int64) error

	// AfterSideEffect runs after the handler has returned success (the
	// side effect is durably applied) but before the Kafka offset is
	// committed. Returning an error simulates a process crash in that
	// exact window: ConsumerLoop.Run returns immediately without
	// committing, so a restart redelivers this record and the handler's
	// idempotency is what has to make the redelivery a no-op.
	AfterSideEffect(ctx context.Context, topic string, partition int32, offset int64) error

	// BeforeOffsetCommit runs immediately before the commit call itself.
	// Returning an error simulates the commit RPC failing (as opposed to
	// AfterSideEffect's full-process-crash semantics): ConsumerLoop
	// surfaces it as a normal error and stops without committing.
	BeforeOffsetCommit(ctx context.Context, topic string, partition int32, offset int64) error
}

// NoopFailureInjector is the production FailureInjector: every hook is a
// no-op, so it adds no behavior beyond the hook calls themselves.
type NoopFailureInjector struct{}

func (NoopFailureInjector) BeforeSideEffect(context.Context, string, int32, int64) error {
	return nil
}

func (NoopFailureInjector) AfterSideEffect(context.Context, string, int32, int64) error {
	return nil
}

func (NoopFailureInjector) BeforeOffsetCommit(context.Context, string, int32, int64) error {
	return nil
}
