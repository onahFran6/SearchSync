// Package observability is the single home for cross-cutting
// instrumentation: Prometheus metrics, OpenTelemetry tracing, and an
// opt-in pprof listener.
package observability

import (
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics owns one process's private Prometheus registry (never the
// global default registry, so two binaries in the same test process never
// collide) and every metric this stage's implementation doc specifies.
// Every label set here is a small, fixed vocabulary - consumer group
// names, topic names, outcome/kind enums - never an order/saga/customer
// ID, per the hard constraint on label cardinality.
type Metrics struct {
	registry *prometheus.Registry

	eventsProcessed      *prometheus.CounterVec
	eventsFailed         *prometheus.CounterVec
	processingSeconds    *prometheus.HistogramVec
	retryAttempts        *prometheus.HistogramVec
	commitSeconds        *prometheus.HistogramVec
	dlqDepth             *prometheus.GaugeVec
	consumerLag          *prometheus.GaugeVec
	replicationSlotBytes *prometheus.GaugeVec
	connectConnectorUp   *prometheus.GaugeVec
	connectTaskUp        *prometheus.GaugeVec

	httpRequests     *prometheus.CounterVec
	httpDurationSecs *prometheus.HistogramVec

	sagaCompletionSecs *prometheus.HistogramVec
	sagaRejectedTotal  prometheus.Counter
}

// NewMetrics builds and registers every metric this binary might emit.
// Registering metrics a given binary never actually updates (e.g.
// saga_completion_seconds on cmd/indexer) is deliberate: one shared type
// keeps the metric definitions in one place, and an unused metric simply
// never appears with a sample in that binary's /metrics output.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	m := &Metrics{
		registry: reg,
		eventsProcessed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "events_processed_total",
			Help: "Records a ConsumerLoop finished handling (committed its offset), labeled by whether it succeeded or was quarantined to a DLQ.",
		}, []string{"consumer_group", "topic", "result"}),
		eventsFailed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "events_failed_total",
			Help: "Individual Process attempts that returned an error, by failure classification.",
		}, []string{"consumer_group", "topic", "kind"}),
		processingSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "event_processing_duration_seconds",
			Help:    "Wall-clock time spent in Process for one record, including retries, excluding DLQ publish and offset commit.",
			Buckets: prometheus.DefBuckets,
		}, []string{"consumer_group", "topic"}),
		retryAttempts: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "event_retry_attempts",
			Help:    "Number of Process attempts one record took before it succeeded or was quarantined.",
			Buckets: []float64{1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
		}, []string{"consumer_group", "topic"}),
		commitSeconds: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "consumer_commit_duration_seconds",
			Help:    "Time spent committing an offset after a record was processed or quarantined.",
			Buckets: prometheus.DefBuckets,
		}, []string{"consumer_group"}),
		dlqDepth: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "dlq_depth",
			Help: "Last-produced offset summed across a DLQ topic's partitions, polled on a timer. No consumer group reads DLQ topics, so this is total backlog, not lag against an active reader.",
		}, []string{"consumer_group"}),
		consumerLag: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "consumer_lag_records",
			Help: "Messages behind: latest available offset minus this group's committed offset, summed across partitions. Lag is messages, not seconds.",
		}, []string{"consumer_group", "topic"}),
		replicationSlotBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "postgres_replication_slot_retained_bytes",
			Help: "WAL bytes retained by a logical replication slot (current WAL position minus the slot's restart_lsn) - a Debezium connector outage grows this; it is disk pressure PostgreSQL carries until that connector catches up.",
		}, []string{"slot_name"}),
		connectConnectorUp: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "kafka_connect_connector_up",
			Help: "1 if Kafka Connect reports this connector's state as RUNNING, 0 otherwise (including an unreachable Connect REST API).",
		}, []string{"connector"}),
		connectTaskUp: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "kafka_connect_task_up",
			Help: "1 if Kafka Connect reports this connector task's state as RUNNING, 0 otherwise. task_id is bounded (every connector in this system runs exactly one task).",
		}, []string{"connector", "task_id"}),
		httpRequests: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "HTTP requests served, labeled by the registered route pattern (never the raw path, so an ID never becomes a label value) and status code.",
		}, []string{"method", "path", "status"}),
		httpDurationSecs: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP handler latency by registered route pattern.",
			Buckets: prometheus.DefBuckets,
		}, []string{"method", "path"}),
		sagaCompletionSecs: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "saga_completion_seconds",
			Help:    "Order creation to terminal outcome, excluding REJECTED (see saga_rejected_total - a different, much faster code path with no payment/inventory round trip).",
			Buckets: []float64{.1, .25, .5, 1, 2, 5, 10, 20, 30, 60, 120},
		}, []string{"outcome"}),
		sagaRejectedTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "saga_rejected_total",
			Help: "Sagas that ended REJECTED: stock unavailable before any payment/inventory reservation work started.",
		}),
	}

	reg.MustRegister(
		m.eventsProcessed, m.eventsFailed, m.processingSeconds, m.retryAttempts,
		m.commitSeconds, m.dlqDepth, m.consumerLag, m.replicationSlotBytes,
		m.connectConnectorUp, m.connectTaskUp,
		m.httpRequests, m.httpDurationSecs,
		m.sagaCompletionSecs, m.sagaRejectedTotal,
	)
	return m
}

// Handler serves this binary's metrics in Prometheus exposition format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// The methods below satisfy internal/platform.ConsumerLoop's MetricsRecorder
// interface unmodified - see consumerloop.go for why that interface is
// defined there rather than platform importing this package.

func (m *Metrics) ObserveProcessed(group, topic, result string) {
	m.eventsProcessed.WithLabelValues(group, topic, result).Inc()
}

func (m *Metrics) ObserveFailed(group, topic, kind string) {
	m.eventsFailed.WithLabelValues(group, topic, kind).Inc()
}

func (m *Metrics) ObserveProcessingDuration(group, topic string, d time.Duration) {
	m.processingSeconds.WithLabelValues(group, topic).Observe(d.Seconds())
}

func (m *Metrics) ObserveRetryAttempts(group, topic string, attempts int) {
	m.retryAttempts.WithLabelValues(group, topic).Observe(float64(attempts))
}

func (m *Metrics) ObserveCommitDuration(group string, d time.Duration) {
	m.commitSeconds.WithLabelValues(group).Observe(d.Seconds())
}

// SetDLQDepth and SetConsumerLag are driven by a poller in each consumer
// binary (see lag.go) rather than a separate kafka_exporter container -
// every consumer already holds a live *kgo.Client.
func (m *Metrics) SetDLQDepth(group string, depth float64) {
	m.dlqDepth.WithLabelValues(group).Set(depth)
}

func (m *Metrics) SetConsumerLag(group, topic string, lag float64) {
	m.consumerLag.WithLabelValues(group, topic).Set(lag)
}

func (m *Metrics) SetReplicationSlotBytes(slotName string, bytes float64) {
	m.replicationSlotBytes.WithLabelValues(slotName).Set(bytes)
}

func (m *Metrics) SetConnectConnectorUp(connector string, up float64) {
	m.connectConnectorUp.WithLabelValues(connector).Set(up)
}

func (m *Metrics) SetConnectTaskUp(connector string, taskID int, up float64) {
	m.connectTaskUp.WithLabelValues(connector, strconv.Itoa(taskID)).Set(up)
}

func (m *Metrics) ObserveSagaCompletion(outcome string, d time.Duration) {
	m.sagaCompletionSecs.WithLabelValues(outcome).Observe(d.Seconds())
}

func (m *Metrics) IncSagaRejected() {
	m.sagaRejectedTotal.Inc()
}

// InstrumentMux wraps an *http.ServeMux so every request is counted and
// timed by its registered route pattern (resolved via the mux's own
// Handler method, which has returned the matched pattern string, e.g.
// "GET /health", since Go's method-pattern routing landed) rather than the
// raw request path - the only way to guarantee an ID embedded in a path
// never becomes a label value without editing every handler's Register
// method individually.
func (m *Metrics) InstrumentMux(mux *http.ServeMux) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, pattern := mux.Handler(r)
		if pattern == "" {
			pattern = "unmatched"
		}
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		mux.ServeHTTP(rec, r)
		m.ObserveHTTPRequest(r.Method, pattern, rec.status, time.Since(start))
	})
}

func (m *Metrics) ObserveHTTPRequest(method, pattern string, status int, d time.Duration) {
	m.httpRequests.WithLabelValues(method, pattern, strconv.Itoa(status)).Inc()
	m.httpDurationSecs.WithLabelValues(method, pattern).Observe(d.Seconds())
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}
