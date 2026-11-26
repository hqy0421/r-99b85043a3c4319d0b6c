// Package api serves the HTTP API for shoebox's standalone-server mode
// (shoeboxd). The handlers are a thin adapter over the storage and dlq
// packages — they contain no queue logic of their own.
//
// Endpoints:
//
//	POST   /queues/{name}/messages         enqueue a message
//	GET    /queues/{name}/messages/next    consume one message (pull-based)
//	GET    /queues/{name}/stats            queue depth + counters
//	GET    /queues/{name}/dlq              list dead-letter messages
//	POST   /queues/{name}/dlq/{id}/replay  replay a dead message
//	DELETE /queues/{name}/messages/{id}    acknowledge/delete a message
package api

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/adexaja/shoebox/internal/dlq"
	"github.com/adexaja/shoebox/internal/naming"
	"github.com/adexaja/shoebox/internal/storage"
)

// defaultAPIVisibility is the lease duration stamped on pull-based claims
// when no override is configured. It mirrors the broker's default so a
// pulled message and a dispatched message have the same hand-off boundary.
const defaultAPIVisibility = 30 * time.Second

// Handler holds the dependencies shared by all HTTP endpoints: the storage
// backend (for enqueue/dequeue/ack/stats) and the DLQ manager (for list/replay).
type Handler struct {
	store      storage.Storage
	visibility time.Duration
	dlq        *dlq.Manager
	logger     *slog.Logger
}

// NewHandler creates an API Handler.
func NewHandler(store storage.Storage, dlqMgr *dlq.Manager, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{
		store:      store,
		visibility: defaultAPIVisibility,
		dlq:        dlqMgr,
		logger:     logger,
	}
}

// WithVisibilityTimeout overrides the lease duration granted to pull-based
// claims. Non-positive values leave the default in place. It returns the
// handler for chaining at construction time.
func (h *Handler) WithVisibilityTimeout(d time.Duration) *Handler {
	if d > 0 {
		h.visibility = d
	}
	return h
}

// Register mounts all API routes on the given mux. Uses Go 1.22+ routing
// patterns so path parameters ({name}, {id}) are extracted by the mux.
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /queues/{name}/messages", h.enqueue)
	mux.HandleFunc("GET /queues/{name}/messages/next", h.consume)
	mux.HandleFunc("GET /queues/{name}/stats", h.stats)
	mux.HandleFunc("GET /queues/{name}/dlq", h.listDLQ)
	mux.HandleFunc("POST /queues/{name}/dlq/{id}/replay", h.replayDLQ)
	mux.HandleFunc("DELETE /queues/{name}/messages/{id}", h.ack)
}

// enqueueRequest is the JSON body for POST /queues/{name}/messages.
// Payload is a string to keep the API user-friendly (JSON []byte would
// require base64 encoding); it is converted to []byte internally.
type enqueueRequest struct {
	Payload  string            `json:"payload"`
	Delay    string            `json:"delay,omitempty"` // e.g. "5s", "1m"
	Metadata map[string]string `json:"metadata,omitempty"`
}

// enqueueResponse is the JSON body returned on successful enqueue.
type enqueueResponse struct {
	ID string `json:"id"`
}

// maxPayloadSize caps the request body on enqueue to prevent memory
// exhaustion from oversized payloads. 1 MB is generous for a single message.
const maxPayloadSize = 1 << 20 // 1 MB

const maxDLQLimit = 1000

// enqueue handles POST /queues/{name}/messages.
func (h *Handler) enqueue(w http.ResponseWriter, r *http.Request) {
	queue := r.PathValue("name")
	if !naming.ValidQueueName(queue) {
		writeError(w, http.StatusBadRequest, "invalid queue name")
		return
	}

	// Limit body size to prevent memory exhaustion attacks.
	r.Body = http.MaxBytesReader(w, r.Body, maxPayloadSize)

	var req enqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if req.Payload == "" {
		writeError(w, http.StatusBadRequest, "payload is required")
		return
	}

	msg := storage.Message{
		ID:       newRequestID(),
		Queue:    queue,
		Payload:  []byte(req.Payload),
		Metadata: req.Metadata,
	}

	// Parse optional delay.
	if req.Delay != "" {
		d, err := time.ParseDuration(req.Delay)
		if err != nil {
			writeError(w, http.StatusBadRequest, "invalid delay: "+err.Error())
			return
		}
		if d > 0 {
			msg.ScheduledAt = time.Now().Add(d)
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	if err := h.store.Enqueue(ctx, queue, msg); err != nil {
		h.logger.ErrorContext(ctx, "api: enqueue failed",
			slog.String("queue", queue), slog.Any("err", err))
		writeError(w, http.StatusInternalServerError, "enqueue failed")
		return
	}

	writeJSON(w, http.StatusCreated, enqueueResponse{ID: msg.ID})
}

// dequeue pulls messages through the lease-aware path when the backend
// supports it, so pulled messages carry a LeaseID and become re-claimable
// after the visibility timeout instead of only on process restart.
func (h *Handler) dequeue(ctx context.Context, queue string, limit int) ([]storage.Message, error) {
	if leased, ok := h.store.(storage.VisibilityStorage); ok {
		return leased.DequeueClaimed(ctx, queue, limit, h.visibility)
	}
	return h.store.Dequeue(ctx, queue, limit)
}

// consume handles GET /queues/{name}/messages/next.
// It pulls one message from the queue under a lease. The caller is
// responsible for DELETE-ing the message (echoing its lease_id) to ack it;
// if they don't, the message becomes re-claimable once its visibility
// timeout expires — no restart required.
func (h *Handler) consume(w http.ResponseWriter, r *http.Request) {
	queue := r.PathValue("name")

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	msgs, err := h.dequeue(ctx, queue, 1)
	if err != nil {
		if errors.Is(err, storage.ErrEmpty) {
			writeError(w, http.StatusNotFound, "no messages available")
			return
		}
		h.logger.ErrorContext(ctx, "api: dequeue failed",
			slog.String("queue", queue), slog.Any("err", err))
		writeError(w, http.StatusInternalServerError, "dequeue failed")
		return
	}

	writeJSON(w, http.StatusOK, msgs[0])
}

// stats handles GET /queues/{name}/stats.
func (h *Handler) stats(w http.ResponseWriter, r *http.Request) {
	queue := r.PathValue("name")

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	stats, err := h.store.Stats(ctx, queue)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "stats failed")
		return
	}

	writeJSON(w, http.StatusOK, stats)
}

// listDLQ handles GET /queues/{name}/dlq.
func (h *Handler) listDLQ(w http.ResponseWriter, r *http.Request) {
	queue := r.PathValue("name")
	limit := 50
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 {
			if parsed > maxDLQLimit {
				parsed = maxDLQLimit
			}
			limit = parsed
		}
	}

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	records, err := h.dlq.List(ctx, queue, limit)
	if err != nil {
		if errors.Is(err, storage.ErrEmpty) {
			writeJSON(w, http.StatusOK, []any{})
			return
		}
		writeError(w, http.StatusInternalServerError, "dlq list failed")
		return
	}

	writeJSON(w, http.StatusOK, records)
}

// replayDLQ handles POST /queues/{name}/dlq/{id}/replay.
func (h *Handler) replayDLQ(w http.ResponseWriter, r *http.Request) {
	queue := r.PathValue("name")
	id := r.PathValue("id")

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	if err := h.dlq.Replay(ctx, queue, id); err != nil {
		if errors.Is(err, storage.ErrEmpty) {
			writeError(w, http.StatusNotFound, "dlq message not found")
			return
		}
		h.logger.ErrorContext(ctx, "api: replay failed",
			slog.String("queue", queue), slog.String("id", id), slog.Any("err", err))
		writeError(w, http.StatusInternalServerError, "replay failed")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "replayed"})
}

// ack handles DELETE /queues/{name}/messages/{id}.
//
// When the storage supports leases, the request must present the lease_id
// returned with the message, either as the ?lease_id= query parameter or the
// X-Shoebox-Lease-Id header. An ack carrying a stale or missing lease is
// rejected with 409 Conflict: the claim has either expired and been
// re-claimed by another worker, or the message is already completed.
// Backends without leases accept the plain DELETE as before.
func (h *Handler) ack(w http.ResponseWriter, r *http.Request) {
	queue := r.PathValue("name")
	id := r.PathValue("id")

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	if leased, ok := h.store.(storage.VisibilityStorage); ok {
		leaseID := r.URL.Query().Get("lease_id")
		if leaseID == "" {
			leaseID = r.Header.Get("X-Shoebox-Lease-Id")
		}
		if leaseID == "" {
			writeError(w, http.StatusConflict, "lease_id is required to acknowledge a leased message")
			return
		}
		if err := leased.AckClaimed(ctx, queue, id, leaseID); err != nil {
			if errors.Is(err, storage.ErrLeaseLost) {
				writeError(w, http.StatusConflict, "lease no longer held; message was re-claimed or completed")
				return
			}
			h.logger.ErrorContext(ctx, "api: ack failed",
				slog.String("queue", queue), slog.String("id", id), slog.Any("err", err))
			writeError(w, http.StatusInternalServerError, "ack failed")
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "acked"})
		return
	}

	if err := h.store.Ack(ctx, queue, id); err != nil {
		writeError(w, http.StatusInternalServerError, "ack failed")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "acked"})
}

// --- helpers ---

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// newRequestID generates a unique ID for an enqueued message. Uses the same
// crypto/rand approach as the broker.
func newRequestID() string {
	return storage.NewMessageID()
}
