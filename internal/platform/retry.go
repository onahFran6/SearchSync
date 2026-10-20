package platform

import (
	"context"
	"math/rand"
	"time"
)

// FailureKind classifies an error returned from a consumer side effect so
// retry behavior never depends on matching an error message string.
type FailureKind int

const (
	// FailureTransient means the same operation is expected to succeed on
	// a later attempt (network blip, dependency outage, lock contention).
	FailureTransient FailureKind = iota
	// FailurePermanent means retrying will never succeed (malformed
	// input, schema violation, unknown contract version) and the record
	// should go straight to the DLQ instead of consuming retry attempts.
	FailurePermanent
)

// ClassifyFunc decides whether an error from a side effect is worth
// retrying. Implementations live next to each consumer's handler, since
// what counts as permanent is domain-specific (an Elasticsearch 4xx means
// something different than a Postgres check-constraint violation).
type ClassifyFunc func(error) FailureKind

// RetryPolicy bounds retry attempts and controls the exponential backoff
// with full jitter applied between them.
type RetryPolicy struct {
	MaxAttempts int
	BaseDelay   time.Duration
	MaxDelay    time.Duration
}

// DefaultRetryPolicy is tuned for a local dependency outage lasting a few
// seconds, not a prolonged one; a prolonged outage should exhaust the
// budget and let the DLQ (or an operator) take over rather than retry
// forever inside the consumer.
var DefaultRetryPolicy = RetryPolicy{
	MaxAttempts: 5,
	BaseDelay:   200 * time.Millisecond,
	MaxDelay:    10 * time.Second,
}

// Backoff returns the delay before attempt number `attempt` (1-indexed),
// using full jitter: a uniform random duration between 0 and the
// exponential cap. Full jitter avoids every failed consumer retrying in
// lockstep after a shared outage clears.
func (p RetryPolicy) Backoff(attempt int) time.Duration {
	ceiling := p.BaseDelay << (attempt - 1) // #nosec G115 -- attempt is always small and positive
	if ceiling <= 0 || ceiling > p.MaxDelay {
		ceiling = p.MaxDelay
	}
	return time.Duration(rand.Int63n(int64(ceiling) + 1))
}

// Sleep waits for the given duration or until ctx is done, returning
// ctx.Err() in the latter case so callers can stop promptly on shutdown
// instead of riding out a backoff delay.
func Sleep(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
