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

	"github.com/francisonah/searchsync/internal/payments"
	"github.com/francisonah/searchsync/internal/platform"
)

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

	kafkaClient, err := platform.NewKafkaConsumer(cfg.kafkaBrokers, cfg.consumerGroup, []string{cfg.authorizeTopic})
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

	repo := payments.NewRepository()
	service := payments.NewService(pool, repo)
	worker := payments.NewWorker(kafkaClient, service, logger, dlq, cfg.consumerGroup)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler(pool))
	payments.NewDevHandler(pool, repo, logger).Register(mux)
	server := &http.Server{Addr: cfg.apiAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}

	errCh := make(chan error, 2)
	go func() {
		logger.Info("starting payments dev api", "addr", cfg.apiAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("http server: %w", err)
		}
	}()
	go func() {
		logger.Info("starting payments consumer", "topic", cfg.authorizeTopic, "group", cfg.consumerGroup)
		if err := worker.Run(ctx); err != nil && ctx.Err() == nil {
			errCh <- fmt.Errorf("consumer: %w", err)
		}
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutting down")
	case err := <-errCh:
		logger.Error("payments service stopped", "error", err)
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
	authorizeTopic   string
	consumerGroup    string
	dlqTopic         string
	apiAddr          string
}

func loadConfig() config {
	return config{
		postgresHost:     getenv("POSTGRES_HOST", "localhost"),
		postgresPort:     getenv("POSTGRES_PORT", "5432"),
		postgresDB:       getenv("POSTGRES_DB", "searchsync"),
		postgresUser:     getenv("PAYMENTS_DB_USER", "searchsync_payments_app"),
		postgresPassword: os.Getenv("PAYMENTS_DB_PASSWORD"),
		kafkaBrokers:     strings.Split(getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), ","),
		authorizeTopic:   getenv("AUTHORIZE_PAYMENT_TOPIC", "searchsync.outbox.event.authorize_payment"),
		consumerGroup:    getenv("PAYMENTS_CONSUMER_GROUP", "searchsync-payments"),
		dlqTopic:         getenv("PAYMENTS_DLQ_TOPIC", "searchsync.dlq.payments"),
		apiAddr:          getenv("PAYMENTS_API_ADDR", ":8085"),
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
