package inventory

import (
	"encoding/json"
	"log/slog"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

const maxBodyBytes = 1 << 20 // 1 MiB

// DevHandler exposes the local-only failure-injection endpoint Experiment
// #6 needs. Never register this outside local development; there is no
// equivalent production concern - it exists purely to make a transient
// Inventory failure reproducible on demand instead of waiting for a real
// one.
type DevHandler struct {
	pool   *pgxpool.Pool
	repo   *Repository
	logger *slog.Logger
}

func NewDevHandler(pool *pgxpool.Pool, repo *Repository, logger *slog.Logger) *DevHandler {
	return &DevHandler{pool: pool, repo: repo, logger: logger}
}

func (h *DevHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /dev/inventory/release-failures", h.setReleaseFailures)
}

type setReleaseFailuresRequest struct {
	Count int `json:"count"`
}

func (h *DevHandler) setReleaseFailures(w http.ResponseWriter, r *http.Request) {
	var req setReleaseFailuresRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}
	if req.Count < 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "count must be >= 0"})
		return
	}

	if err := h.repo.SetDevReleaseFailures(r.Context(), h.pool, req.Count); err != nil {
		h.logger.Error("set dev release failures", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"remaining": req.Count})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
