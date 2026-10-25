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

	"github.com/francisonah/searchsync/internal/indexer"
	"github.com/francisonah/searchsync/internal/observability"
	"github.com/francisonah/searchsync/internal/platform"
	"github.com/francisonah/searchsync/internal/search"
)

// cmd/indexer has no public HTTP server, so it gets a metrics-only listener
// of its own (INDEXER_METRICS_ADDR) rather than retrofitting a public mux
// onto a binary that never needed one.
func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cfg := loadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	shutdownTracing, err := observability.NewTracerProvider(ctx, "searchsync-indexer", cfg.otlpEndpoint)
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

	worker := indexer.NewWorker(kafkaClient, indexer.NewHandler(searchClient), logger, dlq, cfg.consumerGroup, metrics)

	errCh := make(chan error, 2)
	go func() {
		logger.Info("starting indexer metrics server", "addr", cfg.metricsAddr)
		if err := metricsServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- fmt.Errorf("metrics server: %w", err)
		}
	}()
	go func() {
		logger.Info("starting indexer", "topic", cfg.topic, "group", cfg.consumerGroup, "index", cfg.productsIndex, "dlq_topic", cfg.dlqTopic)
		if err := worker.Run(ctx); err != nil && ctx.Err() == nil {
			errCh <- fmt.Errorf("indexer: %w", err)
		}
	}()
	go observability.RunLagPoller(ctx, kafkaClient, cfg.consumerGroup, []string{cfg.dlqTopic}, metrics, 10*time.Second, logger)

	select {
	case <-ctx.Done():
		logger.Info("shutting down")
	case err := <-errCh:
		logger.Error("indexer service stopped", "error", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := metricsServer.Shutdown(shutdownCtx); err != nil {
		logger.Error("metrics server shutdown error", "error", err)
	}
	logger.Info("indexer shut down")
}

type config struct {
	kafkaBrokers     []string
	topic            string
	consumerGroup    string
	dlqTopic         string
	elasticsearchURL string
	productsIndex    string
	metricsAddr      string
	otlpEndpoint     string
	pprofAddr        string
}

func loadConfig() config {
	return config{
		kafkaBrokers:     strings.Split(getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"), ","),
		topic:            getenv("PRODUCTS_TOPIC", "searchsync.public.products"),
		consumerGroup:    getenv("PRODUCTS_CONSUMER_GROUP", "searchsync-indexer"),
		dlqTopic:         getenv("PRODUCTS_DLQ_TOPIC", "searchsync.dlq.products"),
		elasticsearchURL: getenv("ELASTICSEARCH_URL", "http://localhost:9200"),
		productsIndex:    getenv("PRODUCTS_INDEX", "products"),
		metricsAddr:      getenv("INDEXER_METRICS_ADDR", ":9101"),
		otlpEndpoint:     getenv("OTEL_EXPORTER_OTLP_ENDPOINT", "localhost:4317"),
		pprofAddr:        os.Getenv("INDEXER_PPROF_ADDR"),
	}
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
