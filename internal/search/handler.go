package search

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"strconv"
)

type Handler struct {
	client *Client
	logger *slog.Logger
}

func NewHandler(client *Client, logger *slog.Logger) *Handler {
	return &Handler{client: client, logger: logger}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("GET /search", h.search)
}

func (h *Handler) search(w http.ResponseWriter, r *http.Request) {
	values := r.URL.Query()
	query := Query{
		Text:     values.Get("q"),
		Category: values.Get("category"),
	}
	if v := values.Get("min_price_cents"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			query.MinPriceCents = &n
		}
	}
	if v := values.Get("max_price_cents"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			query.MaxPriceCents = &n
		}
	}

	docs, err := h.client.Search(r.Context(), query)
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		h.logger.Error("search failed", "error", err)
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{"error": "search failed"})
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{"results": docs})
}
