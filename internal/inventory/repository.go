package inventory

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/onahFran6/SearchSync/internal/outbox"
)

// Querier is satisfied by both pgx.Tx and *pgxpool.Pool, same role as every
// other package's Querier in this codebase: Service opens one transaction
// per command and every repository call shares it.
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

// InsertProcessedCommand attempts the command-dedup insert. It returns
// true if this is the first time messageID has been seen (the caller
// should proceed) and false if it is a duplicate (the caller should
// commit the no-op and stop) - the same structural dedup pattern as
// processed_events, applied to incoming commands instead of
// incoming domain events.
func (r *Repository) InsertProcessedCommand(ctx context.Context, q Querier, messageID string) (bool, error) {
	_, err := q.Exec(ctx, `INSERT INTO inventory.processed_commands (message_id) VALUES ($1)`, messageID)
	if err != nil {
		if isUniqueViolation(err) {
			return false, nil
		}
		return false, fmt.Errorf("insert processed command: %w", err)
	}
	return true, nil
}

// GetReservation looks up the (at most one) reservation for an order. The
// zero Reservation and found=false mean no reservation exists yet - the
// caller is free to create one.
func (r *Repository) GetReservation(ctx context.Context, q Querier, orderID string) (Reservation, bool, error) {
	const query = `SELECT order_id, sku, quantity, status FROM inventory.reservations WHERE order_id = $1`

	var res Reservation
	err := q.QueryRow(ctx, query, orderID).Scan(&res.OrderID, &res.SKU, &res.Quantity, &res.Status)
	if errors.Is(err, pgx.ErrNoRows) {
		return Reservation{}, false, nil
	}
	if err != nil {
		return Reservation{}, false, fmt.Errorf("get reservation: %w", err)
	}
	return res, true, nil
}

// TryReserveStock is the conditional update that makes concurrent
// reservations for the same SKU safe (see migrations/0005's comment on
// inventory.stock): it only decrements available if there is enough of
// it, atomically, in the same statement. ok=false means insufficient
// stock - the caller must not create a reservation row or otherwise act
// as if the decrement happened.
func (r *Repository) TryReserveStock(ctx context.Context, q Querier, sku string, quantity int) (ok bool, err error) {
	const query = `UPDATE inventory.stock SET available = available - $2 WHERE sku = $1 AND available >= $2`

	tag, err := q.Exec(ctx, query, sku, quantity)
	if err != nil {
		return false, fmt.Errorf("reserve stock: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// CreditStock reverses a reservation's effect on available stock during
// compensation. Unlike TryReserveStock there is no WHERE-clause condition
// to fail: crediting stock back can never be "insufficient."
func (r *Repository) CreditStock(ctx context.Context, q Querier, sku string, quantity int) error {
	const query = `UPDATE inventory.stock SET available = available + $2 WHERE sku = $1`

	tag, err := q.Exec(ctx, query, sku, quantity)
	if err != nil {
		return fmt.Errorf("credit stock: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("credit stock: no stock row for sku %s", sku)
	}
	return nil
}

// InsertReservation records a successful reservation. Called only after
// TryReserveStock succeeded, in the same transaction - the unique
// PRIMARY KEY on order_id is what stops any other code path from ever
// creating two reservations for the same order (Experiment #4).
func (r *Repository) InsertReservation(ctx context.Context, q Querier, res Reservation) error {
	const query = `
		INSERT INTO inventory.reservations (order_id, sku, quantity, status)
		VALUES ($1, $2, $3, $4)`

	_, err := q.Exec(ctx, query, res.OrderID, res.SKU, res.Quantity, ReservationReserved)
	if err != nil {
		return fmt.Errorf("insert reservation: %w", err)
	}
	return nil
}

// MarkReleased flips a RESERVED row to RELEASED. Scoped to
// status = 'RESERVED' in the WHERE clause so calling it twice (a
// redelivered ReleaseInventory after the first one already committed) is
// a safe zero-row no-op rather than a second CreditStock-worthy event -
// service.go checks the reservation's status before ever calling this,
// so RowsAffected() == 0 here would indicate a logic error upstream, not
// a normal race.
func (r *Repository) MarkReleased(ctx context.Context, q Querier, orderID string) error {
	const query = `UPDATE inventory.reservations SET status = 'RELEASED', released_at = now() WHERE order_id = $1 AND status = 'RESERVED'`

	_, err := q.Exec(ctx, query, orderID)
	if err != nil {
		return fmt.Errorf("mark reservation released: %w", err)
	}
	return nil
}

// ConsumeDevReleaseFailure is Experiment #6's local-only failure
// injection: if the single dev_release_failures row has remaining > 0, it
// decrements it and returns true (the caller must simulate a failure and
// must not touch any other table in the same transaction - rolling back
// leaves remaining decremented because this UPDATE commits as part of the
// caller's retry attempt, so each retry genuinely consumes one simulated
// failure). remaining == 0 returns false: proceed normally.
func (r *Repository) ConsumeDevReleaseFailure(ctx context.Context, q Querier) (bool, error) {
	const query = `UPDATE inventory.dev_release_failures SET remaining = remaining - 1 WHERE id = 1 AND remaining > 0`

	tag, err := q.Exec(ctx, query)
	if err != nil {
		return false, fmt.Errorf("consume dev release failure: %w", err)
	}
	return tag.RowsAffected() == 1, nil
}

// InsertOutboxEvent writes one row to inventory.outbox_events.
//
// This does NOT reuse internal/outbox.Repository.Insert: that package's
// SQL names the table as the bare `outbox_events`, which only resolves
// correctly for roles whose search_path falls through to `public` (true
// for the original searchsync_app). The dedicated, schema-scoped roles
// introduced for these services deliberately do not rely on search_path for anything -
// every new repository qualifies its
// table names explicitly, including this one, so a misconfigured
// search_path can fail loudly (permission denied) instead of silently
// writing to - or reading from - the wrong schema.
func (r *Repository) InsertOutboxEvent(ctx context.Context, q Querier, event outbox.Event) error {
	const query = `
		INSERT INTO inventory.outbox_events (id, aggregatetype, aggregateid, type, payload, trace_parent)
		VALUES ($1, $2, $3, $4, $5, NULLIF($6, ''))`

	_, err := q.Exec(ctx, query, event.ID, event.AggregateType, event.AggregateID, event.Type, event.Payload, event.TraceParent)
	if err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}
	return nil
}

// SetDevReleaseFailures is the write side of POST
// /dev/inventory/release-failures.
func (r *Repository) SetDevReleaseFailures(ctx context.Context, q Querier, count int) error {
	const query = `UPDATE inventory.dev_release_failures SET remaining = $1 WHERE id = 1`

	_, err := q.Exec(ctx, query, count)
	if err != nil {
		return fmt.Errorf("set dev release failures: %w", err)
	}
	return nil
}
