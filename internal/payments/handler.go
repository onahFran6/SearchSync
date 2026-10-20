package payments

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

const maxBodyBytes = 1 << 20 // 1 MiB

// DevHandler exposes POST /dev/payments/mode, the implementation doc's
// required local-only endpoint for configuring deterministic outcomes.
// Never expose this beyond localhost - there is no production equivalent;
// a real payment processor's outcome is not something the caller gets to
// dictate.
type DevHandler struct {
	pool   *pgxpool.Pool
	repo   *Repository
	logger *slog.Logger
}

func NewDevHandler(pool *pgxpool.Pool, repo *Repository, logger *slog.Logger) *DevHandler {
	return &DevHandler{pool: pool, repo: repo, logger: logger}
}

func (h *DevHandler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /dev/payments/mode", h.setMode)
	mux.HandleFunc("GET /dev/payments/mode", h.getMode)
}

type setModeRequest struct {
	Mode string `json:"mode"`
}

func (h *DevHandler) setMode(w http.ResponseWriter, r *http.Request) {
	var req setModeRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}

	mode := Mode(req.Mode)
	switch mode {
	case ModeSuccess, ModeDecline, ModeTimeout:
	default:
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": fmt.Sprintf("mode must be one of success, decline, timeout; got %q", req.Mode)})
		return
	}

	if err := h.repo.SetMode(r.Context(), h.pool, mode); err != nil {
		h.logger.Error("set payments mode", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"mode": string(mode)})
}

func (h *DevHandler) getMode(w http.ResponseWriter, r *http.Request) {
	mode, err := h.repo.GetMode(r.Context(), h.pool)
	if err != nil {
		h.logger.Error("get payments mode", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"mode": string(mode)})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
