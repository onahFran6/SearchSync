package products

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var ErrNotFound = errors.New("product not found")

type Repository struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

func (r *Repository) Create(ctx context.Context, req CreateRequest) (Product, error) {
	const q = `
		INSERT INTO products (sku, name, description, category, price_cents)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING id, sku, name, description, category, price_cents, updated_at`

	var p Product
	err := r.pool.QueryRow(ctx, q, req.SKU, req.Name, req.Description, req.Category, req.PriceCents).
		Scan(&p.ID, &p.SKU, &p.Name, &p.Description, &p.Category, &p.PriceCents, &p.UpdatedAt)
	if err != nil {
		return Product{}, fmt.Errorf("insert product: %w", err)
	}
	return p, nil
}

func (r *Repository) Get(ctx context.Context, id int64) (Product, error) {
	const q = `SELECT id, sku, name, description, category, price_cents, updated_at FROM products WHERE id = $1`

	var p Product
	err := r.pool.QueryRow(ctx, q, id).
		Scan(&p.ID, &p.SKU, &p.Name, &p.Description, &p.Category, &p.PriceCents, &p.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Product{}, ErrNotFound
	}
	if err != nil {
		return Product{}, fmt.Errorf("get product: %w", err)
	}
	return p, nil
}

func (r *Repository) Update(ctx context.Context, id int64, req UpdateRequest) (Product, error) {
	const q = `
		UPDATE products SET
			name = COALESCE($2, name),
			description = COALESCE($3, description),
			category = COALESCE($4, category),
			price_cents = COALESCE($5, price_cents),
			updated_at = now()
		WHERE id = $1
		RETURNING id, sku, name, description, category, price_cents, updated_at`

	var p Product
	err := r.pool.QueryRow(ctx, q, id, req.Name, req.Description, req.Category, req.PriceCents).
		Scan(&p.ID, &p.SKU, &p.Name, &p.Description, &p.Category, &p.PriceCents, &p.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return Product{}, ErrNotFound
	}
	if err != nil {
		return Product{}, fmt.Errorf("update product: %w", err)
	}
	return p, nil
}

func (r *Repository) Delete(ctx context.Context, id int64) error {
	const q = `DELETE FROM products WHERE id = $1`

	tag, err := r.pool.Exec(ctx, q, id)
	if err != nil {
		return fmt.Errorf("delete product: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}
	return nil
}
