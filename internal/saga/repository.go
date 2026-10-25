package saga

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/onahFran6/SearchSync/internal/outbox"
)

// Querier is satisfied by both pgx.Tx and *pgxpool.Pool.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

const pgUniqueViolationCode = "23505"

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolationCode
}

// Row is one saga.sagas row.
type Row struct {
	OrderID              string
	State                State
	Version              int
	SKU                  string
	Quantity             int
	AmountCents          int64
	PaymentDeadline      *time.Time
	CompensationDeadline *time.Time
	CompensationAttempts int
	// CreatedAt is order-creation time, the start of the
	// saga_completion_seconds measurement (see service.go's terminal
	// transition handlers).
	CreatedAt time.Time
}

type Repository struct{}

func NewRepository() *Repository {
	return &Repository{}
}

// InsertProcessedMessage is the same structural dedup every new service
// uses: true means this message hasn't been applied yet and the
// caller should proceed; false means it's a duplicate and the caller
// should commit the no-op and stop.
func (r *Repository) InsertProcessedMessage(ctx context.Context, q Querier, messageID string) (bool, error) {
	_, err := q.Exec(ctx, `INSERT INTO saga.processed_messages (message_id) VALUES ($1)`, messageID)
	if err != nil {
		if isUniqueViolation(err) {
			return false, nil
		}
		return false, fmt.Errorf("insert processed message: %w", err)
	}
	return true, nil
}

// InsertSaga creates a new saga row in PENDING at version 0. Deliberately
// has no ON CONFLICT clause: InsertProcessedMessage already absorbs the
// normal "OrderCreated redelivered" case before this is ever called, so a
// unique-violation here would mean two different OrderCreated events
// claimed the same order_id - a real anomaly worth surfacing as an error,
// not silently swallowing.
func (r *Repository) InsertSaga(ctx context.Context, q Querier, row Row) error {
	const query = `
		INSERT INTO saga.sagas (order_id, state, version, sku, quantity, amount_cents)
		VALUES ($1, $2, 0, $3, $4, $5)`

	_, err := q.Exec(ctx, query, row.OrderID, row.State, row.SKU, row.Quantity, row.AmountCents)
	if err != nil {
		return fmt.Errorf("insert saga: %w", err)
	}
	return nil
}

func (r *Repository) Get(ctx context.Context, q Querier, orderID string) (Row, bool, error) {
	const query = `
		SELECT order_id, state, version, sku, quantity, amount_cents,
		       payment_deadline, compensation_deadline, compensation_attempts, created_at
		FROM saga.sagas WHERE order_id = $1`

	var row Row
	err := q.QueryRow(ctx, query, orderID).Scan(
		&row.OrderID, &row.State, &row.Version, &row.SKU, &row.Quantity, &row.AmountCents,
		&row.PaymentDeadline, &row.CompensationDeadline, &row.CompensationAttempts, &row.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Row{}, false, nil
	}
	if err != nil {
		return Row{}, false, fmt.Errorf("get saga: %w", err)
	}
	return row, true, nil
}

// GuardedTransition is the optimistic-concurrency guard every event-driven
// transition goes through: it only applies if the row is currently in
// exactly `expected` state. ok=false means it wasn't (already transitioned
// by an earlier delivery of this same logical event, or genuinely stale) -
// the caller must treat that as a safe no-op, never as an error. createdAt
// is returned alongside version so a caller landing on a terminal state can
// observe saga_completion_seconds (order creation to this moment) without
// a second round trip.
func (r *Repository) GuardedTransition(ctx context.Context, q Querier, orderID string, expected, next State) (version int, createdAt time.Time, ok bool, err error) {
	const query = `
		UPDATE saga.sagas SET state = $3, version = version + 1, updated_at = now()
		WHERE order_id = $1 AND state = $2
		RETURNING version, created_at`

	row := q.QueryRow(ctx, query, orderID, expected, next)
	if err := row.Scan(&version, &createdAt); err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, time.Time{}, false, nil
		}
		return 0, time.Time{}, false, fmt.Errorf("guarded transition: %w", err)
	}
	return version, createdAt, true, nil
}

func (r *Repository) SetPaymentDeadline(ctx context.Context, q Querier, orderID string, deadline time.Time) error {
	_, err := q.Exec(ctx, `UPDATE saga.sagas SET payment_deadline = $2 WHERE order_id = $1`, orderID, deadline)
	if err != nil {
		return fmt.Errorf("set payment deadline: %w", err)
	}
	return nil
}

func (r *Repository) ClearPaymentDeadline(ctx context.Context, q Querier, orderID string) error {
	_, err := q.Exec(ctx, `UPDATE saga.sagas SET payment_deadline = NULL WHERE order_id = $1`, orderID)
	if err != nil {
		return fmt.Errorf("clear payment deadline: %w", err)
	}
	return nil
}

// SetCompensation sets the compensation deadline and attempt count
// together - used both when first entering COMPENSATING (attempts=1) and
// is superseded by AttemptCompensationRetry for subsequent retries.
func (r *Repository) SetCompensation(ctx context.Context, q Querier, orderID string, deadline time.Time, attempts int) error {
	_, err := q.Exec(ctx, `UPDATE saga.sagas SET compensation_deadline = $2, compensation_attempts = $3 WHERE order_id = $1`,
		orderID, deadline, attempts)
	if err != nil {
		return fmt.Errorf("set compensation: %w", err)
	}
	return nil
}

func (r *Repository) ClearCompensation(ctx context.Context, q Querier, orderID string) error {
	_, err := q.Exec(ctx, `UPDATE saga.sagas SET compensation_deadline = NULL WHERE order_id = $1`, orderID)
	if err != nil {
		return fmt.Errorf("clear compensation: %w", err)
	}
	return nil
}

// ListDuePaymentTimeouts returns sagas whose payment_deadline has passed
// while still STOCK_RESERVED - candidates for the poller to treat as a
// payment timeout. This is a plain SELECT (no FOR UPDATE): a single
// saga-coordinator process is the only writer this stage runs, and the
// actual safety comes from GuardedTransition's WHERE clause when each
// candidate is acted on, not from a lock taken here. A row that raced
// past STOCK_RESERVED between this SELECT and that UPDATE simply fails
// the guard and is skipped - never double-processed.
func (r *Repository) ListDuePaymentTimeouts(ctx context.Context, q Querier, now time.Time) ([]Row, error) {
	const query = `
		SELECT order_id, state, version, sku, quantity, amount_cents,
		       payment_deadline, compensation_deadline, compensation_attempts, created_at
		FROM saga.sagas WHERE state = 'STOCK_RESERVED' AND payment_deadline < $1`
	return r.queryRows(ctx, q, query, now)
}

// ListDueCompensationRetries returns sagas whose compensation_deadline has
// passed while still COMPENSATING - candidates for the poller to either
// retry (dispatch another ReleaseInventory) or escalate to
// NEEDS_ATTENTION, decided atomically by AttemptCompensationRetry.
func (r *Repository) ListDueCompensationRetries(ctx context.Context, q Querier, now time.Time) ([]Row, error) {
	const query = `
		SELECT order_id, state, version, sku, quantity, amount_cents,
		       payment_deadline, compensation_deadline, compensation_attempts, created_at
		FROM saga.sagas WHERE state = 'COMPENSATING' AND compensation_deadline < $1`
	return r.queryRows(ctx, q, query, now)
}

func (r *Repository) queryRows(ctx context.Context, q Querier, query string, args ...any) ([]Row, error) {
	rows, err := q.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query sagas: %w", err)
	}
	defer rows.Close()

	var out []Row
	for rows.Next() {
		var row Row
		if err := rows.Scan(&row.OrderID, &row.State, &row.Version, &row.SKU, &row.Quantity, &row.AmountCents,
			&row.PaymentDeadline, &row.CompensationDeadline, &row.CompensationAttempts, &row.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan saga row: %w", err)
		}
		out = append(out, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate saga rows: %w", err)
	}
	return out, nil
}

// AttemptCompensationRetry is the one atomic statement behind both halves
// of Experiment #6: it bumps compensation_attempts and, in the same
// UPDATE, either resets the deadline for another retry or escalates to
// NEEDS_ATTENTION if maxAttempts has been reached - whichever it is,
// decided by Postgres in one round trip, not by a read-then-write the
// poller could race against itself across ticks. The WHERE clause
// (state = 'COMPENSATING' AND compensation_deadline < now) means a saga
// that already left COMPENSATING via a concurrent InventoryReleased
// simply matches zero rows here - found=false, nothing to escalate or
// retry, no race to reason about.
func (r *Repository) AttemptCompensationRetry(ctx context.Context, q Querier, orderID string, now, nextDeadline time.Time, maxAttempts int) (row Row, escalated bool, found bool, err error) {
	const query = `
		UPDATE saga.sagas SET
			compensation_attempts = compensation_attempts + 1,
			compensation_deadline = CASE WHEN compensation_attempts + 1 >= $4 THEN NULL::timestamptz ELSE $3 END,
			state = CASE WHEN compensation_attempts + 1 >= $4 THEN 'NEEDS_ATTENTION' ELSE state END,
			version = CASE WHEN compensation_attempts + 1 >= $4 THEN version + 1 ELSE version END,
			updated_at = now()
		WHERE order_id = $1 AND state = 'COMPENSATING' AND compensation_deadline < $2
		RETURNING order_id, state, version, sku, quantity, amount_cents,
		          payment_deadline, compensation_deadline, compensation_attempts, created_at`

	r2 := q.QueryRow(ctx, query, orderID, now, nextDeadline, maxAttempts)
	if scanErr := r2.Scan(&row.OrderID, &row.State, &row.Version, &row.SKU, &row.Quantity, &row.AmountCents,
		&row.PaymentDeadline, &row.CompensationDeadline, &row.CompensationAttempts, &row.CreatedAt); scanErr != nil {
		if errors.Is(scanErr, pgx.ErrNoRows) {
			return Row{}, false, false, nil
		}
		return Row{}, false, false, fmt.Errorf("attempt compensation retry: %w", scanErr)
	}
	return row, row.State == StateNeedsAttention, true, nil
}

// InsertOutboxEvent writes one row to saga.outbox_events. Does not reuse
// internal/outbox.Repository.Insert - see
// internal/inventory/repository.go's InsertOutboxEvent for why.
func (r *Repository) InsertOutboxEvent(ctx context.Context, q Querier, event outbox.Event) error {
	const query = `
		INSERT INTO saga.outbox_events (id, aggregatetype, aggregateid, type, payload, trace_parent)
		VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''))`

	_, err := q.Exec(ctx, query, event.ID, event.AggregateType, event.AggregateID, event.Type, event.Payload, event.TraceParent)
	if err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}
	return nil
}
