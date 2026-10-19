package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/francisonah/searchsync/internal/platform"
	"github.com/francisonah/searchsync/internal/products"
	"github.com/francisonah/searchsync/internal/search"
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

	esClient, err := platform.NewElasticsearchClient(cfg.elasticsearchURL)
	if err != nil {
		logger.Error("connect elasticsearch", "error", err)
		os.Exit(1)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", healthHandler(pool))

	productsHandler := products.NewHandler(products.NewService(products.NewRepository(pool)), logger)
	productsHandler.Register(mux)

	searchHandler := search.NewHandler(search.NewClient(esClient, cfg.productsIndex), logger)
	searchHandler.Register(mux)

	server := &http.Server{
		Addr:              cfg.apiAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		logger.Info("starting api server", "addr", cfg.apiAddr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down")

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
	postgresHost     string
	postgresPort     string
	postgresDB       string
	postgresUser     string
	postgresPassword string
	elasticsearchURL string
	productsIndex    string
	apiAddr          string
}

func loadConfig() config {
	return config{
		postgresHost:     getenv("POSTGRES_HOST", "localhost"),
		postgresPort:     getenv("POSTGRES_PORT", "5432"),
		postgresDB:       getenv("POSTGRES_DB", "searchsync"),
		postgresUser:     getenv("POSTGRES_USER", "searchsync_app"),
		postgresPassword: os.Getenv("POSTGRES_PASSWORD"),
		elasticsearchURL: getenv("ELASTICSEARCH_URL", "http://localhost:9200"),
		productsIndex:    getenv("PRODUCTS_INDEX", "products"),
		apiAddr:          getenv("API_ADDR", ":8080"),
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
