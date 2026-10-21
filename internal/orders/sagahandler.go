package orders

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/francisonah/searchsync/internal/platform"
	"github.com/francisonah/searchsync/internal/saga"
)

// ErrSagaPermanent marks a failure that will never succeed on retry (a
// malformed SagaTransitioned payload). Named distinctly from the
// package's existing ErrValidation (an HTTP-layer concept) since this is
// the consumer-side permanent/transient distinction Stage 3 established.
var ErrSagaPermanent = errors.New("orders: permanent saga notification failure")

var permanentPostgresCodes = map[string]bool{
	"23502": true, // not_null_violation
	"23514": true, // check_violation
	"22P02": true, // invalid_text_representation
	"23503": true, // foreign_key_violation
}

// ClassifySagaNotification distinguishes retryable Postgres/network
// failures from permanent ones, same convention as
// internal/notifications.classify.
func ClassifySagaNotification(err error) platform.FailureKind {
	if errors.Is(err, ErrSagaPermanent) {
		return platform.FailurePermanent
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && permanentPostgresCodes[pgErr.Code] {
		return platform.FailurePermanent
	}
	return platform.FailureTransient
}

// SagaHandler applies one SagaTransitioned notification: it is the only
// code that ever writes orders.status after creation, and the only
// writer of the saga_transitions projection - Orders mirrors the saga's
// state machine onto its own data by consuming events, never by another
// service reaching into this schema directly (see
// docs/clarification/stage-04-ownership-decision.md).
type SagaHandler struct {
	pool *pgxpool.Pool
}

func NewSagaHandler(pool *pgxpool.Pool) *SagaHandler {
	return &SagaHandler{pool: pool}
}

// HandleMessage decodes one SagaTransitioned event and, in one local
// transaction: dedups by MessageID against the same processed_events
// table Stage 2's notifications consumer uses (message/event ids are
// globally unique UUIDs regardless of which topic they came from, so
// sharing the dedup table avoids a redundant one), appends a
// saga_transitions row, and updates orders.status to ToState.
func (h *SagaHandler) HandleMessage(ctx context.Context, value []byte) error {
	var evt saga.SagaTransitioned
	if err := json.Unmarshal(value, &evt); err != nil {
		return fmt.Errorf("%w: decode SagaTransitioned: %v", ErrSagaPermanent, err)
	}
	if evt.MessageID == "" || evt.OrderID == "" || evt.ToState == "" {
		return fmt.Errorf("%w: SagaTransitioned missing required fields", ErrSagaPermanent)
	}

	tx, err := h.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	_, err = tx.Exec(ctx, `INSERT INTO processed_events (event_id) VALUES ($1)`, evt.MessageID)
	if err != nil {
		if isUniqueViolation(err) {
			return nil
		}
		return fmt.Errorf("insert processed event: %w", err)
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO saga_transitions (event_id, order_id, from_state, to_state, reason, event_version)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		evt.MessageID, evt.OrderID, evt.FromState, evt.ToState, evt.Reason, evt.EventVersion)
	if err != nil {
		return fmt.Errorf("insert saga transition: %w", err)
	}

	tag, err := tx.Exec(ctx, `UPDATE orders SET status = $2 WHERE id = $1`, evt.OrderID, evt.ToState)
	if err != nil {
		return fmt.Errorf("update order status: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("%w: SagaTransitioned for unknown order %s", ErrSagaPermanent, evt.OrderID)
	}

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}
