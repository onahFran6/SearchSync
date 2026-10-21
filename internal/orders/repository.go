package orders

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("order not found")

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
		INSERT INTO orders (id, customer_id, status, total_cents, product_sku, quantity)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING created_at`

	err := q.QueryRow(ctx, query, order.ID, order.CustomerID, order.Status, order.TotalCents, order.ProductSKU, order.Quantity).
		Scan(&order.CreatedAt)
	if err != nil {
		return Order{}, fmt.Errorf("insert order: %w", err)
	}
	return order, nil
}

// Get reads one order by id, for GET /orders/:id. Takes *pgxpool.Pool
// directly rather than the Querier interface: unlike Create, this is
// never called inside a shared transaction.
func (r *Repository) Get(ctx context.Context, pool *pgxpool.Pool, id string) (Order, error) {
	const query = `
		SELECT id, customer_id, status, total_cents, product_sku, quantity, created_at
		FROM orders WHERE id = $1`

	var order Order
	err := pool.QueryRow(ctx, query, id).Scan(
		&order.ID, &order.CustomerID, &order.Status, &order.TotalCents,
		&order.ProductSKU, &order.Quantity, &order.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Order{}, ErrNotFound
	}
	if err != nil {
		return Order{}, fmt.Errorf("get order: %w", err)
	}
	return order, nil
}
