package payments

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/francisonah/searchsync/internal/outbox"
)

// AuthorizationStatus mirrors payments.authorizations.status's CHECK
// constraint (migrations/0007_create_payments_schema.sql).
type AuthorizationStatus string

const (
	StatusAuthorized AuthorizationStatus = "AUTHORIZED"
	StatusDeclined   AuthorizationStatus = "DECLINED"
)

// Mode is the simulator's configured deterministic outcome, same three
// values as payments.mode_config.mode's CHECK constraint.
type Mode string

const (
	ModeSuccess Mode = "success"
	ModeDecline Mode = "decline"
	ModeTimeout Mode = "timeout"
)

// Querier is satisfied by both pgx.Tx and *pgxpool.Pool.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

const pgUniqueViolationCode = "23505"

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolationCode
}

type Repository struct{}

func NewRepository() *Repository {
	return &Repository{}
}

// InsertProcessedCommand is the same structural dedup every Stage 4
// service uses: true means proceed, false means this exact message was
// already applied.
func (r *Repository) InsertProcessedCommand(ctx context.Context, q Querier, messageID string) (bool, error) {
	_, err := q.Exec(ctx, `INSERT INTO payments.processed_commands (message_id) VALUES ($1)`, messageID)
	if err != nil {
		if isUniqueViolation(err) {
			return false, nil
		}
		return false, fmt.Errorf("insert processed command: %w", err)
	}
	return true, nil
}

// GetMode reads the current configured outcome. There is always exactly
// one row (seeded by migrations/0007); callers never need a not-found
// case.
func (r *Repository) GetMode(ctx context.Context, q Querier) (Mode, error) {
	var mode Mode
	err := q.QueryRow(ctx, `SELECT mode FROM payments.mode_config WHERE id = 1`).Scan(&mode)
	if err != nil {
		return "", fmt.Errorf("get mode: %w", err)
	}
	return mode, nil
}

// SetMode is the write side of POST /dev/payments/mode.
func (r *Repository) SetMode(ctx context.Context, q Querier, mode Mode) error {
	_, err := q.Exec(ctx, `UPDATE payments.mode_config SET mode = $1 WHERE id = 1`, mode)
	if err != nil {
		return fmt.Errorf("set mode: %w", err)
	}
	return nil
}

// GetAuthorization looks up a prior authorization decision for an order,
// if one was already made.
func (r *Repository) GetAuthorization(ctx context.Context, q Querier, orderID string) (AuthorizationStatus, bool, error) {
	var status AuthorizationStatus
	err := q.QueryRow(ctx, `SELECT status FROM payments.authorizations WHERE order_id = $1`, orderID).Scan(&status)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("get authorization: %w", err)
	}
	return status, true, nil
}

// InsertAuthorization records a fresh decision. The unique PRIMARY KEY on
// order_id is what makes a duplicate AuthorizePayment command idempotent
// - callers must check GetAuthorization first and skip this entirely if a
// row already exists, rather than relying on this insert's conflict (the
// decision, success/decline, must not be re-rolled on a retry, since the
// configured mode may have changed since the first attempt).
func (r *Repository) InsertAuthorization(ctx context.Context, q Querier, orderID string, status AuthorizationStatus, modeUsed Mode) error {
	_, err := q.Exec(ctx, `
		INSERT INTO payments.authorizations (order_id, status, mode_used)
		VALUES ($1, $2, $3)`, orderID, status, modeUsed)
	if err != nil {
		return fmt.Errorf("insert authorization: %w", err)
	}
	return nil
}

// InsertOutboxEvent writes one row to payments.outbox_events. Does not
// reuse internal/outbox.Repository.Insert for the same reason
// internal/inventory's equivalent method doesn't - see that method's
// comment.
func (r *Repository) InsertOutboxEvent(ctx context.Context, q Querier, event outbox.Event) error {
	const query = `
		INSERT INTO payments.outbox_events (id, aggregatetype, aggregateid, type, payload)
		VALUES ($1, $2, $3, $4, $5)`

	_, err := q.Exec(ctx, query, event.ID, event.AggregateType, event.AggregateID, event.Type, event.Payload)
	if err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}
	return nil
}
