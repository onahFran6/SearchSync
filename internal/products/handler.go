package products

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
)

const maxBodyBytes = 1 << 20 // 1 MiB

type Handler struct {
	service *Service
	logger  *slog.Logger
}

func NewHandler(service *Service, logger *slog.Logger) *Handler {
	return &Handler{service: service, logger: logger}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /products", h.create)
	mux.HandleFunc("GET /products/{id}", h.get)
	mux.HandleFunc("PATCH /products/{id}", h.update)
	mux.HandleFunc("DELETE /products/{id}", h.delete)
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	var req CreateRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	product, err := h.service.Create(r.Context(), req)
	if err != nil {
		h.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, product)
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	id, ok := idFromPath(w, r)
	if !ok {
		return
	}
	product, err := h.service.Get(r.Context(), id)
	if err != nil {
		h.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, product)
}

func (h *Handler) update(w http.ResponseWriter, r *http.Request) {
	id, ok := idFromPath(w, r)
	if !ok {
		return
	}
	var req UpdateRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	product, err := h.service.Update(r.Context(), id, req)
	if err != nil {
		h.writeError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, product)
}

func (h *Handler) delete(w http.ResponseWriter, r *http.Request) {
	id, ok := idFromPath(w, r)
	if !ok {
		return
	}
	if err := h.service.Delete(r.Context(), id); err != nil {
		h.writeError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (h *Handler) writeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrNotFound):
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
	case errors.Is(err, ErrValidation):
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
	default:
		h.logger.Error("product handler error", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
	}
}

func idFromPath(w http.ResponseWriter, r *http.Request) (int64, bool) {
	id, err := strconv.ParseInt(r.PathValue("id"), 10, 64)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid id"})
		return 0, false
	}
	return id, true
}

func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return false
	}
	return true
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
