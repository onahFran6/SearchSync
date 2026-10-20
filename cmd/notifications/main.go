package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/francisonah/searchsync/internal/notifications"
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

	worker := notifications.NewWorker(kafkaClient, notifications.NewHandler(pool), logger, dlq, cfg.consumerGroup)

	logger.Info("starting notifications consumer", "topic", cfg.topic, "group", cfg.consumerGroup, "dlq_topic", cfg.dlqTopic)
	if err := worker.Run(ctx); err != nil && ctx.Err() == nil {
		logger.Error("notifications consumer stopped", "error", err)
		os.Exit(1)
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
