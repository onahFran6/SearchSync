// cmd/loadgen is the load generator and seed script in one binary:
// -mode=seed bulk-creates N products with no
// visibility tracking (fast, for populating a baseline corpus before a
// benchmark run); -mode=benchmark runs controlled create/update traffic at
// a target rate and measures both request latency and end-to-end search
// visibility latency (PostgreSQL commit, approximated by the POST/PATCH
// response, to the product appearing in /search) - a number this tool
// exists to keep distinct from Kafka consumer lag.
//
// It reports achieved rate and errors, never just configured concurrency -
// a load generator that only logs "sent N requests" is not acceptable
// evidence.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/google/uuid"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stderr, nil))

	mode := flag.String("mode", "benchmark", "seed | benchmark")
	apiAddr := flag.String("api", getenv("API_ADDR_URL", "http://localhost:8081"), "base URL of cmd/api")
	count := flag.Int("count", 1000, "seed mode: number of products to create")
	rate := flag.Float64("rate", 20, "benchmark mode: target requests/sec")
	duration := flag.Duration("duration", 30*time.Second, "benchmark mode: run duration")
	concurrency := flag.Int("concurrency", 10, "max in-flight requests")
	updateFraction := flag.Float64("update-fraction", 0.3, "benchmark mode: fraction of ops that are PATCH updates to an already-created product rather than POST creates")
	visibilityTimeout := flag.Duration("visibility-timeout", 10*time.Second, "benchmark mode: how long to poll /search before counting a create as a visibility timeout")
	visibilityPoll := flag.Duration("visibility-poll", 100*time.Millisecond, "benchmark mode: interval between /search polls")
	jsonOut := flag.Bool("json", false, "emit the report as JSON instead of text")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client := &apiClient{httpClient: &http.Client{Timeout: 10 * time.Second}, baseURL: *apiAddr}

	var report Report
	switch *mode {
	case "seed":
		report = runSeed(ctx, client, *count, *concurrency, logger)
	case "benchmark":
		report = runBenchmark(ctx, client, benchmarkConfig{
			rate:              *rate,
			duration:          *duration,
			concurrency:       *concurrency,
			updateFraction:    *updateFraction,
			visibilityTimeout: *visibilityTimeout,
			visibilityPoll:    *visibilityPoll,
		}, logger)
	default:
		fmt.Fprintf(os.Stderr, "unknown -mode %q (want seed or benchmark)\n", *mode)
		os.Exit(2)
	}

	if *jsonOut {
		_ = json.NewEncoder(os.Stdout).Encode(report)
		return
	}
	report.Print(os.Stdout)
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// runSeed bulk-creates count products with concurrency-bounded goroutines
// and no visibility tracking - it exists to populate a baseline corpus
// (1k/10k/50k) before a benchmark run, not to measure one.
func runSeed(ctx context.Context, client *apiClient, count, concurrency int, logger *slog.Logger) Report {
	runID := uuid.New().String()[:8]
	start := time.Now()

	latencies := &sampleSet{}
	successes := &counter{}
	failures := &counter{}

	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup

	for i := 0; i < count; i++ {
		if ctx.Err() != nil {
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(seq int) {
			defer wg.Done()
			defer func() { <-sem }()

			reqStart := time.Now()
			_, err := client.createProduct(ctx, fmt.Sprintf("LOADGEN-SEED-%s-%06d", runID, seq), fmt.Sprintf("seed product %s-%06d", runID, seq))
			latencies.add(time.Since(reqStart))
			if err != nil {
				failures.inc()
				logger.Warn("seed create failed", "seq", seq, "error", err)
				return
			}
			successes.inc()
		}(i)
	}
	wg.Wait()

	elapsed := time.Since(start)
	p50, p95, p99 := latencies.percentiles()
	return Report{
		Experiment:    fmt.Sprintf("seed / %d products / concurrency %d", count, concurrency),
		RunDuration:   elapsed,
		Requested:     count,
		Successes:     successes.value(),
		Failures:      failures.value(),
		ThroughputOps: float64(successes.value()) / elapsed.Seconds(),
		RequestP50:    p50,
		RequestP95:    p95,
		RequestP99:    p99,
	}
}

type benchmarkConfig struct {
	rate              float64
	duration          time.Duration
	concurrency       int
	updateFraction    float64
	visibilityTimeout time.Duration
	visibilityPoll    time.Duration
}

// runBenchmark issues create/update traffic at cfg.rate for cfg.duration,
// bounded to cfg.concurrency in-flight requests at once, and measures both
// request latency and (for creates) search-visibility latency: wall-clock
// time from the create response (the write's commit, per this repo's rule
// that indexing never happens in the write transaction) to the product
// first appearing in /search.
func runBenchmark(ctx context.Context, client *apiClient, cfg benchmarkConfig, logger *slog.Logger) Report {
	runID := uuid.New().String()[:8]
	runCtx, cancel := context.WithTimeout(ctx, cfg.duration)
	defer cancel()

	requestLatencies := &sampleSet{}
	visibilityLatencies := &sampleSet{}
	requestErrors := &counter{}
	visibilityTimeouts := &counter{}
	creates := &counter{}
	updates := &counter{}

	var createdMu sync.Mutex
	var createdIDs []int64

	sem := make(chan struct{}, cfg.concurrency)
	var wg sync.WaitGroup

	interval := time.Duration(float64(time.Second) / cfg.rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	seq := 0
	start := time.Now()

loop:
	for {
		select {
		case <-runCtx.Done():
			break loop
		case <-ticker.C:
			seq++
			sem <- struct{}{}
			wg.Add(1)
			go func(seq int) {
				defer wg.Done()
				defer func() { <-sem }()

				createdMu.Lock()
				havePool := len(createdIDs) > 0
				var updateID int64
				if havePool {
					updateID = createdIDs[rand.Intn(len(createdIDs))] //nolint:gosec // load generation, not security-sensitive
				}
				createdMu.Unlock()

				if havePool && rand.Float64() < cfg.updateFraction { //nolint:gosec // load generation, not security-sensitive
					updates.inc()
					reqStart := time.Now()
					err := client.updateProduct(runCtx, updateID, rand.Intn(100_000)) //nolint:gosec // load generation, not security-sensitive
					requestLatencies.add(time.Since(reqStart))
					if err != nil {
						requestErrors.inc()
						logger.Warn("update failed", "id", updateID, "error", err)
					}
					return
				}

				creates.inc()
				marker := fmt.Sprintf("loadgen-%s-%06d", runID, seq)
				reqStart := time.Now()
				id, err := client.createProduct(runCtx, fmt.Sprintf("LOADGEN-BENCH-%s-%06d", runID, seq), marker)
				commitTime := time.Now()
				requestLatencies.add(commitTime.Sub(reqStart))
				if err != nil {
					requestErrors.inc()
					logger.Warn("create failed", "seq", seq, "error", err)
					return
				}

				createdMu.Lock()
				createdIDs = append(createdIDs, id)
				createdMu.Unlock()

				visible, err := client.pollUntilSearchable(runCtx, marker, cfg.visibilityTimeout, cfg.visibilityPoll)
				if err != nil {
					visibilityTimeouts.inc()
					logger.Warn("visibility poll failed", "id", id, "marker", marker, "error", err)
					return
				}
				visibilityLatencies.add(visible.Sub(commitTime))
			}(seq)
		}
	}
	wg.Wait()
	elapsed := time.Since(start)

	reqP50, reqP95, reqP99 := requestLatencies.percentiles()
	visP50, visP95, visP99 := visibilityLatencies.percentiles()

	return Report{
		Experiment:            fmt.Sprintf("benchmark / rate %.1f/s / concurrency %d / duration %s / update-fraction %.2f", cfg.rate, cfg.concurrency, cfg.duration, cfg.updateFraction),
		RunDuration:           elapsed,
		Requested:             seq,
		Successes:             creates.value() + updates.value() - requestErrors.value(),
		Failures:              requestErrors.value(),
		Creates:               creates.value(),
		Updates:               updates.value(),
		VisibilityTimeouts:    visibilityTimeouts.value(),
		ThroughputOps:         float64(creates.value()+updates.value()-requestErrors.value()) / elapsed.Seconds(),
		RequestP50:            reqP50,
		RequestP95:            reqP95,
		RequestP99:            reqP99,
		VisibilityP50:         visP50,
		VisibilityP95:         visP95,
		VisibilityP99:         visP99,
		VisibilitySampleCount: visibilityLatencies.count(),
	}
}

// apiClient is a thin HTTP wrapper around cmd/api's product and search
// endpoints - no retries, no backoff: a load generator must report what
// actually happened, including raw failures, not mask them with its own
// resilience logic.
type apiClient struct {
	httpClient *http.Client
	baseURL    string
}

type createProductRequest struct {
	SKU         string `json:"sku"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Category    string `json:"category"`
	PriceCents  int    `json:"price_cents"`
}

type productResponse struct {
	ID int64 `json:"id"`
}

func (c *apiClient) createProduct(ctx context.Context, sku, marker string) (int64, error) {
	body, err := json.Marshal(createProductRequest{
		SKU: sku, Name: marker, Description: "generated by cmd/loadgen",
		Category: "loadgen", PriceCents: 1000 + rand.Intn(9000), //nolint:gosec // load generation, not security-sensitive
	})
	if err != nil {
		return 0, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/products", bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated {
		b, _ := io.ReadAll(resp.Body)
		return 0, fmt.Errorf("create product: status %d: %s", resp.StatusCode, b)
	}

	var parsed productResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return 0, fmt.Errorf("decode create response: %w", err)
	}
	return parsed.ID, nil
}

func (c *apiClient) updateProduct(ctx context.Context, id int64, priceCents int) error {
	body, err := json.Marshal(map[string]any{"price_cents": priceCents})
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPatch, fmt.Sprintf("%s/products/%d", c.baseURL, id), bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("update product %d: status %d: %s", id, resp.StatusCode, b)
	}
	return nil
}

var errNotYetVisible = errors.New("loadgen: not yet visible")

// pollUntilSearchable polls GET /search?q=marker until it returns at least
// one hit or timeout elapses. marker is unique per create (embedded in the
// product's Name), so a hit unambiguously means this specific write is
// visible, not some other loadgen-generated row.
func (c *apiClient) pollUntilSearchable(ctx context.Context, marker string, timeout, pollInterval time.Duration) (time.Time, error) {
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		found, err := c.searchHasMarker(ctx, marker)
		if err == nil && found {
			return time.Now(), nil
		}
		if time.Now().After(deadline) {
			return time.Time{}, fmt.Errorf("%w: %q after %s", errNotYetVisible, marker, timeout)
		}
		select {
		case <-ctx.Done():
			return time.Time{}, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (c *apiClient) searchHasMarker(ctx context.Context, marker string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/search?q="+marker, nil)
	if err != nil {
		return false, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return false, fmt.Errorf("search: status %d: %s", resp.StatusCode, b)
	}

	var parsed struct {
		Results []json.RawMessage `json:"results"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return false, fmt.Errorf("decode search response: %w", err)
	}
	return len(parsed.Results) > 0, nil
}
