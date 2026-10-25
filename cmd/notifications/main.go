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

	"github.com/francisonah/searchsync/internal/notifications"
	"github.com/francisonah/searchsync/internal/observability"
	"github.com/francisonah/searchsync/internal/platform"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := loadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := observability.NewTracerProvider(ctx, "searchsync-notifications", cfg.otlpEndpoint)
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

	kafkaClient, err := platform.NewKafkaConsumer(cfg.kafkaBrokers, cfg.consumerGroup, []string{cfg.topic})
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

	metrics := observability.NewMetrics()
	metricsMux := http.NewServeMux()
	metricsMux.Handle("GET /metrics", metrics.Handler())
	metricsServer := &http.Server{Addr: cfg.metricsAddr, Handler: metricsMux, ReadHeaderTimeout: 5 * time.Second}

	worker := notifications.NewWorker(kafkaClient, notifications.NewHandler(pool), logger, dlq, cfg.consumerGroup, metrics)

	errCh := make(chan error, 2)
	go func() {
		logger.Info("starting notifications metrics server", "addr", cfg.metricsAddr)
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("metrics server: %w", err)
		}
	}()
	go func() {
		logger.Info("starting notifications consumer", "topic", cfg.topic, "group", cfg.consumerGroup, "dlq_topic", cfg.dlqTopic)
		if err := worker.Run(ctx); err != nil && ctx.Err() == nil {
			errCh <- fmt.Errorf("notifications consumer: %w", err)
		}
	}()
	go observability.RunLagPoller(ctx, kafkaClient, cfg.consumerGroup, []string{cfg.dlqTopic}, metrics, 10*time.Second, logger)

	select {
	case <-ctx.Done():
		logger.Info("shutting down")
	case err := <-errCh:
		logger.Error("notifications service stopped", "error", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("metrics server shutdown error", "error", err)
	}
	logger.Info("notifications consumer shut down")
}

type config struct {
	postgresHost     string
	postgresPort     string
	postgresDB       string
	postgresUser     string
	postgresPassword string
	kafkaBrokers     []string
	topic            string
	consumerGroup    string
	dlqTopic         string
	metricsAddr      string
	otlpEndpoint     string
	pprofAddr        string
}

func loadConfig() config {
	return config{
		postgresHost:     getenv("POSTGRES_HOST", "localhost"),
		postgresPort:     getenv("POSTGRES_PORT", "5432"),
		postgresDB:       getenv("POSTGRES_DB", "searchsync"),
		postgresUser:     getenv("POSTGRES_USER", "searchsync_app"),
		postgresPassword: os.Getenv("POSTGRES_PASSWORD"),
		kafkaBrokers:     strings.Split(getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), ","),
		topic:            getenv("ORDER_EVENTS_TOPIC", "searchsync.outbox.event.orders"),
		consumerGroup:    getenv("ORDER_EVENTS_CONSUMER_GROUP", "searchsync-notifications"),
		dlqTopic:         getenv("ORDER_EVENTS_DLQ_TOPIC", "searchsync.dlq.orders"),
		metricsAddr:      getenv("NOTIFICATIONS_METRICS_ADDR", ":9102"),
		otlpEndpoint:     getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317"),
		pprofAddr:        os.Getenv("NOTIFICATIONS_PPROF_ADDR"),
	}
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
