package orders

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Querier is satisfied by both pgx.Tx and *pgxpool.Pool. Create always
// receives a pgx.Tx in practice, since the order insert must share a
// transaction with the outbox insert (see Service.CreateOrder); the
// interface stays pool-compatible too so tests can exercise it without a
// transaction.
type Querier interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type Repository struct{}

func NewRepository() *Repository {
	return &Repository{}
}

func (r *Repository) Create(ctx context.Context, q Querier, order Order) (Order, error) {
	const query = `
		INSERT INTO orders (id, customer_id, status, total_cents)
		VALUES ($1, $2, $3, $4)
		RETURNING created_at`

	err := q.QueryRow(ctx, query, order.ID, order.CustomerID, order.Status, order.TotalCents).
		Scan(&order.CreatedAt)
	if err != nil {
		return Order{}, fmt.Errorf("insert order: %w", err)
	}
	return order, nil
}
