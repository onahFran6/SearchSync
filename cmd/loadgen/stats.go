package main

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

// sampleSet collects latency observations from many goroutines behind one
// mutex. Percentiles need every sample sorted at the end, not a streaming
// approximation - acceptable here because a benchmark run produces at most
// tens of thousands of samples, never an unbounded stream.
type sampleSet struct {
	mu      sync.Mutex
	samples []time.Duration
}

func (s *sampleSet) add(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.samples = append(s.samples, d)
}

func (s *sampleSet) percentiles() (p50, p95, p99 time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.samples) == 0 {
		return 0, 0, 0
	}
	sorted := make([]time.Duration, len(s.samples))
	copy(sorted, s.samples)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })

	return percentileOf(sorted, 0.50), percentileOf(sorted, 0.95), percentileOf(sorted, 0.99)
}

func (s *sampleSet) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.samples)
}

// percentileOf assumes sorted is already ascending. Nearest-rank method:
// simple, standard for benchmark reporting, no interpolation subtlety to
// get wrong under time pressure.
func percentileOf(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p*float64(len(sorted))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// counter is a trivial mutex-guarded counter - fine at this scale, no need
// for sync/atomic subtleties the rest of this binary doesn't otherwise need.
type counter struct {
	mu sync.Mutex
	n  int64
}

func (c *counter) inc() {
	c.mu.Lock()
	c.n++
	c.mu.Unlock()
}

func (c *counter) value() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

func formatDuration(d time.Duration) string {
	return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000)
}
