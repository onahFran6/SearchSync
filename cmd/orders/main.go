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

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/francisonah/searchsync/internal/orders"
	"github.com/francisonah/searchsync/internal/outbox"
	"github.com/francisonah/searchsync/internal/platform"
)

// cmd/orders runs the HTTP server and the Stage 4 saga-notification
// consumer (SagaWorker) in one process: both write exclusively to
// orders-owned data (the orders table, the saga_transitions projection),
// so there is no ownership reason to split them into separate binaries -
// same reasoning as cmd/inventory and cmd/payments-sim pairing their own
// consumer with their own dev-only HTTP endpoint.
func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := loadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := platform.NewPostgresPool(ctx, cfg.postgresDSN())
	if err != nil {
		logger.Error("connect postgres", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	kafkaClient, err := platform.NewKafkaConsumer(cfg.kafkaBrokers, cfg.sagaConsumerGroup, []string{cfg.sagaTransitionedTopic})
	if err != nil {
		logger.Error("connect kafka", "error", err)
		os.Exit(1)
	}
	defer kafkaClient.Close()

	dlq, err := platform.NewDLQProducer(cfg.kafkaBrokers, cfg.sagaDLQTopic)
	if err != nil {
		logger.Error("connect dlq producer", "error", err)
		os.Exit(1)
	}
	defer dlq.Close()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler(pool))

	service := orders.NewService(pool, orders.NewRepository(), outbox.NewRepository())
	orders.NewHandler(service, logger).Register(mux)

	sagaWorker := orders.NewSagaWorker(kafkaClient, orders.NewSagaHandler(pool), logger, dlq, cfg.sagaConsumerGroup)

	server := &http.Server{
		Addr:              cfg.ordersAPIAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	errCh := make(chan error, 2)
	go func() {
		logger.Info("starting orders api server", "addr", cfg.ordersAPIAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("http server: %w", err)
		}
	}()
	go func() {
		logger.Info("starting orders saga-notification consumer", "topic", cfg.sagaTransitionedTopic, "group", cfg.sagaConsumerGroup)
		if err := sagaWorker.Run(ctx); err != nil && ctx.Err() == nil {
			errCh <- fmt.Errorf("saga consumer: %w", err)
		}
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutting down")
	case err := <-errCh:
		logger.Error("orders service stopped", "error", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown error", "error", err)
	}
}

func healthHandler(pool *pgxpool.Pool) http.HandlerFunc {
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
	postgresHost          string
	postgresPort          string
	postgresDB            string
	postgresUser          string
	postgresPassword      string
	ordersAPIAddr         string
	kafkaBrokers          []string
	sagaTransitionedTopic string
	sagaConsumerGroup     string
	sagaDLQTopic          string
}

func loadConfig() config {
	return config{
		postgresHost:          getenv("POSTGRES_HOST", "localhost"),
		postgresPort:          getenv("POSTGRES_PORT", "5432"),
		postgresDB:            getenv("POSTGRES_DB", "searchsync"),
		postgresUser:          getenv("POSTGRES_USER", "searchsync_app"),
		postgresPassword:      os.Getenv("POSTGRES_PASSWORD"),
		ordersAPIAddr:         getenv("ORDERS_API_ADDR", ":8082"),
		kafkaBrokers:          strings.Split(getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), ","),
		sagaTransitionedTopic: getenv("SAGA_TRANSITIONED_TOPIC", "searchsync.outbox.event.saga_transitioned"),
		sagaConsumerGroup:     getenv("ORDERS_SAGA_CONSUMER_GROUP", "searchsync-orders-saga"),
		sagaDLQTopic:          getenv("ORDERS_SAGA_DLQ_TOPIC", "searchsync.dlq.orders_saga"),
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
