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

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"

	"github.com/onahFran6/SearchSync/internal/inventory"
	"github.com/onahFran6/SearchSync/internal/observability"
	"github.com/onahFran6/SearchSync/internal/platform"
)

// cmd/inventory runs both the dev-only HTTP endpoint and the
// ReserveInventory/ReleaseInventory consumer in one process, the same
// dual-role shape cmd/orders uses for its HTTP server plus (after Stage
// 4) its saga-notification consumer: one deployable unit per service,
// not one binary per responsibility.
func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := loadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := observability.NewTracerProvider(ctx, "searchsync-inventory", cfg.otlpEndpoint)
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

	kafkaClient, err := platform.NewKafkaConsumer(cfg.kafkaBrokers, cfg.consumerGroup, []string{cfg.reserveTopic, cfg.releaseTopic})
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

	repo := inventory.NewRepository()
	service := inventory.NewService(pool, repo)
	worker := inventory.NewWorker(kafkaClient, service, cfg.reserveTopic, cfg.releaseTopic, logger, dlq, cfg.consumerGroup, metrics)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler(pool))
	mux.Handle("GET /metrics", metrics.Handler())
	inventory.NewDevHandler(pool, repo, logger).Register(mux)
	server := &http.Server{Addr: cfg.apiAddr, Handler: otelhttp.NewHandler(metrics.InstrumentMux(mux), "searchsync-inventory"), ReadHeaderTimeout: 5 * time.Second}

	errCh := make(chan error, 2)
	go func() {
		logger.Info("starting inventory dev api", "addr", cfg.apiAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("http server: %w", err)
		}
	}()
	go func() {
		logger.Info("starting inventory consumer", "reserve_topic", cfg.reserveTopic, "release_topic", cfg.releaseTopic, "group", cfg.consumerGroup)
		if err := worker.Run(ctx); err != nil && ctx.Err() == nil {
			errCh <- fmt.Errorf("consumer: %w", err)
		}
	}()
	go observability.RunLagPoller(ctx, kafkaClient, cfg.consumerGroup, []string{cfg.dlqTopic}, metrics, 10*time.Second, logger)

	select {
	case <-ctx.Done():
		logger.Info("shutting down")
	case err := <-errCh:
		logger.Error("inventory service stopped", "error", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("http shutdown error", "error", err)
	}
}

func healthHandler(pool interface {
	Ping(ctx context.Context) error
}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := pool.Ping(ctx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte("db unavailable"))
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}
}

type config struct {
	postgresHost     string
	postgresPort     string
	postgresDB       string
	postgresUser     string
	postgresPassword string
	kafkaBrokers     []string
	reserveTopic     string
	releaseTopic     string
	consumerGroup    string
	dlqTopic         string
	apiAddr          string
	otlpEndpoint     string
	pprofAddr        string
}

func loadConfig() config {
	return config{
		postgresHost:     getenv("POSTGRES_HOST", "localhost"),
		postgresPort:     getenv("POSTGRES_PORT", "5432"),
		postgresDB:       getenv("POSTGRES_DB", "searchsync"),
		postgresUser:     getenv("INVENTORY_DB_USER", "searchsync_inventory_app"),
		postgresPassword: os.Getenv("INVENTORY_DB_PASSWORD"),
		kafkaBrokers:     strings.Split(getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), ","),
		reserveTopic:     getenv("RESERVE_INVENTORY_TOPIC", "searchsync.outbox.event.reserve_inventory"),
		releaseTopic:     getenv("RELEASE_INVENTORY_TOPIC", "searchsync.outbox.event.release_inventory"),
		consumerGroup:    getenv("INVENTORY_CONSUMER_GROUP", "searchsync-inventory"),
		dlqTopic:         getenv("INVENTORY_DLQ_TOPIC", "searchsync.dlq.inventory"),
		apiAddr:          getenv("INVENTORY_API_ADDR", ":8084"),
		otlpEndpoint:     getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317"),
		pprofAddr:        os.Getenv("INVENTORY_PPROF_ADDR"),
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
