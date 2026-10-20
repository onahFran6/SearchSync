package outbox

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Querier is satisfied by both pgx.Tx and *pgxpool.Pool, so callers that
// already hold a transaction (as orders does, to keep the order insert and
// the outbox insert atomic) pass it straight through instead of this
// package opening its own.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type Repository struct{}

func NewRepository() *Repository {
	return &Repository{}
}

// Insert writes one outbox row. It never opens or commits a transaction
// itself; the caller controls the transaction boundary so this insert lands
// atomically with whatever domain write produced the event.
func (r *Repository) Insert(ctx context.Context, q Querier, event Event) error {
	const query = `
		INSERT INTO outbox_events (id, aggregatetype, aggregateid, type, payload)
		VALUES ($1, $2, $3, $4, $5)`

	_, err := q.Exec(ctx, query, event.ID, event.AggregateType, event.AggregateID, event.Type, event.Payload)
	if err != nil {
		return fmt.Errorf("insert outbox event: %w", err)
	}
	return nil
}
