package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/onahFran6/SearchSync/internal/observability"
	"github.com/onahFran6/SearchSync/internal/platform"
	"github.com/onahFran6/SearchSync/internal/saga"
)

// cmd/saga-coordinator runs the event-driven ConsumerLoop and the
// persisted-deadline poller concurrently in one process - same dual-loop
// shape as any of this stage's other services that pair a consumer with
// a second responsibility, except here the second responsibility is the
// poller (poller.go) rather than an HTTP server. Neither loop depends on
// the other to make progress: a poller tick firing while the consumer is
// mid-retry, or vice versa, is exactly the kind of concurrent access
// GuardedTransition's WHERE clause is designed to make safe.
func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := loadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := observability.NewTracerProvider(ctx, "searchsync-saga-coordinator", cfg.otlpEndpoint)
	if err != nil {
		logger.Error("start tracing", "error", err)
		os.Exit(1)
	}
	defer func() { _ = shutdownTracing(context.Background()) }()

	if cfg.pprofAddr != "" {
		stopPprof, err := observability.StartPprof(cfg.pprofAddr)
		if err != nil {
			logger.Error("start pprof", "error", err)
			os.Exit(1)
		}
		defer func() { _ = stopPprof(context.Background()) }()
	}

	pool, err := platform.NewPostgresPool(ctx, cfg.postgresDSN())
	if err != nil {
		logger.Error("connect postgres", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	kafkaClient, err := platform.NewKafkaConsumer(cfg.kafkaBrokers, cfg.consumerGroup, cfg.topics.Slice())
	if err != nil {
		logger.Error("connect kafka", "error", err)
		os.Exit(1)
	}
	defer kafkaClient.Close()

	dlq, err := platform.NewDLQProducer(cfg.kafkaBrokers, cfg.dlqTopic)
	if err != nil {
		logger.Error("connect dlq producer", "error", err)
		os.Exit(1)
	}
	defer dlq.Close()

	// cmd/saga-coordinator has no HTTP server otherwise (pure
	// consumer + poller). Rather than leaving it unscrapable, it gets a
	// metrics-only listener like cmd/indexer/cmd/notifications - a
	// deliberate choice, not an oversight, since saga_completion_seconds
	// and saga_rejected_total only ever originate here.
	metrics := observability.NewMetrics()
	metricsMux := http.NewServeMux()
	metricsMux.Handle("GET /metrics", metrics.Handler())
	metricsServer := &http.Server{Addr: cfg.metricsAddr, Handler: metricsMux, ReadHeaderTimeout: 5 * time.Second}

	service := saga.NewService(pool, saga.NewRepository(), cfg.sagaConfig, metrics)
	worker := saga.NewWorker(kafkaClient, service, cfg.topics, logger, dlq, cfg.consumerGroup, metrics)

	errCh := make(chan error, 3)
	go func() {
		logger.Info("starting saga coordinator metrics server", "addr", cfg.metricsAddr)
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("metrics server: %w", err)
		}
	}()
	go func() {
		logger.Info("starting saga coordinator consumer", "group", cfg.consumerGroup, "topics", cfg.topics.Slice())
		if err := worker.Run(ctx); err != nil && ctx.Err() == nil {
			errCh <- fmt.Errorf("consumer: %w", err)
		}
	}()
	go func() {
		logger.Info("starting saga deadline poller", "interval", cfg.sagaConfig.PollInterval)
		if err := service.RunPoller(ctx, logger); err != nil && ctx.Err() == nil {
			errCh <- fmt.Errorf("poller: %w", err)
		}
	}()
	go observability.RunLagPoller(ctx, kafkaClient, cfg.consumerGroup, []string{cfg.dlqTopic}, metrics, 10*time.Second, logger)

	select {
	case <-ctx.Done():
		logger.Info("shutting down")
	case err := <-errCh:
		logger.Error("saga coordinator stopped", "error", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("metrics server shutdown error", "error", err)
	}
}

type config struct {
	postgresHost     string
	postgresPort     string
	postgresDB       string
	postgresUser     string
	postgresPassword string
	kafkaBrokers     []string
	topics           saga.Topics
	consumerGroup    string
	dlqTopic         string
	sagaConfig       saga.Config
	metricsAddr      string
	otlpEndpoint     string
	pprofAddr        string
}

func loadConfig() config {
	return config{
		postgresHost:     getenv("POSTGRES_HOST", "localhost"),
		postgresPort:     getenv("POSTGRES_PORT", "5432"),
		postgresDB:       getenv("POSTGRES_DB", "searchsync"),
		postgresUser:     getenv("SAGA_DB_USER", "searchsync_saga_app"),
		postgresPassword: os.Getenv("SAGA_DB_PASSWORD"),
		kafkaBrokers:     strings.Split(getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), ","),
		topics: saga.Topics{
			OrderCreated:           getenv("ORDER_EVENTS_TOPIC", "searchsync.outbox.event.orders"),
			InventoryReserved:      getenv("INVENTORY_RESERVED_TOPIC", "searchsync.outbox.event.inventory_reserved"),
			InventoryRejected:      getenv("INVENTORY_REJECTED_TOPIC", "searchsync.outbox.event.inventory_rejected"),
			InventoryReleased:      getenv("INVENTORY_RELEASED_TOPIC", "searchsync.outbox.event.inventory_released"),
			InventoryReleaseFailed: getenv("INVENTORY_RELEASE_FAILED_TOPIC", "searchsync.outbox.event.inventory_release_failed"),
			PaymentAuthorized:      getenv("PAYMENT_AUTHORIZED_TOPIC", "searchsync.outbox.event.payment_authorized"),
			PaymentDeclined:        getenv("PAYMENT_DECLINED_TOPIC", "searchsync.outbox.event.payment_declined"),
		},
		consumerGroup: getenv("SAGA_CONSUMER_GROUP", "searchsync-saga-coordinator"),
		dlqTopic:      getenv("SAGA_DLQ_TOPIC", "searchsync.dlq.saga"),
		sagaConfig:    sagaConfigFromEnv(),
		metricsAddr:   getenv("SAGA_METRICS_ADDR", ":9103"),
		otlpEndpoint:  getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317"),
		pprofAddr:     os.Getenv("SAGA_PPROF_ADDR"),
	}
}

func sagaConfigFromEnv() saga.Config {
	cfg := saga.DefaultConfig
	if v := durationEnv("SAGA_PAYMENT_TIMEOUT"); v > 0 {
		cfg.PaymentTimeout = v
	}
	if v := durationEnv("SAGA_COMPENSATION_RETRY_INTERVAL"); v > 0 {
		cfg.CompensationRetryInterval = v
	}
	if v := durationEnv("SAGA_POLL_INTERVAL"); v > 0 {
		cfg.PollInterval = v
	}
	return cfg
}

func durationEnv(key string) time.Duration {
	v := os.Getenv(key)
	if v == "" {
		return 0
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0
	}
	return d
}

func (c config) postgresDSN() string {
	return fmt.Sprintf("postgres://%s:%s@%s:%s/%s?sslmode=disable",
		c.postgresUser, c.postgresPassword, c.postgresHost, c.postgresPort, c.postgresDB)
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
