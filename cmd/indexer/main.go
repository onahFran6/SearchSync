package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/francisonah/searchsync/internal/indexer"
	"github.com/francisonah/searchsync/internal/platform"
	"github.com/francisonah/searchsync/internal/search"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := loadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	esClient, err := platform.NewElasticsearchClient(cfg.elasticsearchURL)
	if err != nil {
		logger.Error("connect elasticsearch", "error", err)
		os.Exit(1)
	}
	searchClient := search.NewClient(esClient, cfg.productsIndex)

	kafkaClient, err := platform.NewKafkaConsumer(cfg.kafkaBrokers, cfg.consumerGroup, []string{cfg.topic})
	if err != nil {
		logger.Error("connect kafka", "error", err)
		os.Exit(1)
	}
	defer kafkaClient.Close()

	worker := indexer.NewWorker(kafkaClient, indexer.NewHandler(searchClient), logger)

	logger.Info("starting indexer", "topic", cfg.topic, "group", cfg.consumerGroup, "index", cfg.productsIndex)
	if err := worker.Run(ctx); err != nil && ctx.Err() == nil {
		logger.Error("indexer stopped", "error", err)
		os.Exit(1)
	}
	logger.Info("indexer shut down")
}

type config struct {
	kafkaBrokers     []string
	topic            string
	consumerGroup    string
	elasticsearchURL string
	productsIndex    string
}

func loadConfig() config {
	return config{
		kafkaBrokers:     strings.Split(getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), ","),
		topic:            getenv("PRODUCTS_TOPIC", "searchsync.public.products"),
		consumerGroup:    getenv("PRODUCTS_CONSUMER_GROUP", "searchsync-indexer"),
		elasticsearchURL: getenv("ELASTICSEARCH_URL", "http://localhost:9200"),
		productsIndex:    getenv("PRODUCTS_INDEX", "products"),
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
