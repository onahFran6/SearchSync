package orders

import "time"

type Order struct {
	ID         string    `json:"id"`
	CustomerID string    `json:"customer_id"`
	Status     string    `json:"status"`
	TotalCents int64     `json:"total_cents"`
	CreatedAt  time.Time `json:"created_at"`
}

type CreateRequest struct {
	CustomerID string `json:"customer_id"`
	TotalCents int64  `json:"total_cents"`
}

// OrderCreated is the business event published through the outbox, distinct
// from the low-level row-change event Debezium would emit for a plain
// INSERT: it carries domain intent and an explicit schema version so
// consumers can evolve independently of the orders table's columns. Never
// add payment details or other sensitive customer data here; it travels
// through Kafka in plaintext.
type OrderCreated struct {
	EventID    string `json:"eventId"`
	OrderID    string `json:"orderId"`
	CustomerID string `json:"customerId"`
	TotalCents int64  `json:"totalCents"`
	Version    int    `json:"version"`
}
