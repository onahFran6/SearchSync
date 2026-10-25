package main

import (
	"fmt"
	"io"
	"time"
)

// Report is cmd/loadgen's output: achieved rate and errors, not just
// configured concurrency, plus both request latency and (benchmark mode
// only) search-visibility latency - the two numbers this stage exists to
// keep distinct from Kafka consumer lag. Field names are exported so -json
// output is self-describing without a separate schema doc.
type Report struct {
	Experiment  string        `json:"experiment"`
	RunDuration time.Duration `json:"run_duration_ns"`

	Requested int   `json:"requested"`
	Successes int64 `json:"successes"`
	Failures  int64 `json:"failures"`

	Creates            int64 `json:"creates,omitempty"`
	Updates            int64 `json:"updates,omitempty"`
	VisibilityTimeouts int64 `json:"visibility_timeouts,omitempty"`

	ThroughputOps float64 `json:"throughput_ops_per_sec"`

	RequestP50 time.Duration `json:"request_p50_ns"`
	RequestP95 time.Duration `json:"request_p95_ns"`
	RequestP99 time.Duration `json:"request_p99_ns"`

	VisibilityP50         time.Duration `json:"visibility_p50_ns,omitempty"`
	VisibilityP95         time.Duration `json:"visibility_p95_ns,omitempty"`
	VisibilityP99         time.Duration `json:"visibility_p99_ns,omitempty"`
	VisibilitySampleCount int           `json:"visibility_sample_count,omitempty"`
}

func (r Report) Print(w io.Writer) {
	fmt.Fprintf(w, "Experiment: %s\n", r.Experiment)
	fmt.Fprintf(w, "Run duration: %s\n", r.RunDuration.Round(time.Millisecond))
	fmt.Fprintf(w, "Requested ops: %d\n", r.Requested)
	fmt.Fprintf(w, "Successes: %d\n", r.Successes)
	fmt.Fprintf(w, "Failures: %d\n", r.Failures)
	if r.Creates > 0 || r.Updates > 0 {
		fmt.Fprintf(w, "Creates: %d, Updates: %d\n", r.Creates, r.Updates)
	}
	fmt.Fprintf(w, "Achieved throughput: %.2f ops/sec\n", r.ThroughputOps)
	fmt.Fprintf(w, "Request latency: p50=%s p95=%s p99=%s\n",
		formatDuration(r.RequestP50), formatDuration(r.RequestP95), formatDuration(r.RequestP99))
	if r.VisibilitySampleCount > 0 {
		fmt.Fprintf(w, "Search visibility latency (n=%d): p50=%s p95=%s p99=%s\n",
			r.VisibilitySampleCount, formatDuration(r.VisibilityP50), formatDuration(r.VisibilityP95), formatDuration(r.VisibilityP99))
	}
	if r.VisibilityTimeouts > 0 {
		fmt.Fprintf(w, "Visibility timeouts: %d\n", r.VisibilityTimeouts)
	}
}
