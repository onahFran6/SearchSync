package products

import "time"

type Product struct {
	ID          int64     `json:"id"`
	SKU         string    `json:"sku"`
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Category    string    `json:"category"`
	PriceCents  int       `json:"price_cents"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type CreateRequest struct {
	SKU         string `json:"sku"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Category    string `json:"category"`
	PriceCents  int    `json:"price_cents"`
}

// Pointer fields distinguish "not supplied" from "set to zero value" for PATCH semantics.
type UpdateRequest struct {
	Name        *string `json:"name"`
	Description *string `json:"description"`
	Category    *string `json:"category"`
	PriceCents  *int    `json:"price_cents"`
}
