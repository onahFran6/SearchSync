package orders

import "time"

type Order struct {
	ID         string    `json:"id"`
	CustomerID string    `json:"customer_id"`
	Status     string    `json:"status"`
	TotalCents int64     `json:"total_cents"`
	ProductSKU string    `json:"product_sku"`
	Quantity   int       `json:"quantity"`
	CreatedAt  time.Time `json:"created_at"`
}

// CreateRequest's ProductSKU/Quantity are Stage 4's addition: the saga
// needs something concrete to reserve. This stage models the
// implementation doc's "simple single-SKU case" - one SKU, one quantity
// per order - not a line-items table.
type CreateRequest struct {
	CustomerID string `json:"customer_id"`
	TotalCents int64  `json:"total_cents"`
	ProductSKU string `json:"product_sku"`
	Quantity   int    `json:"quantity"`
}

// OrderCreated is the business event published through the outbox, distinct
// from the low-level row-change event Debezium would emit for a plain
// INSERT: it carries domain intent and an explicit schema version so
// consumers can evolve independently of the orders table's columns. Never
// add payment details or other sensitive customer data here; it travels
// through Kafka in plaintext.
//
// internal/saga deliberately does NOT import this struct (it decodes its
// own minimal local copy instead, see internal/saga/consumer.go) - this
// package imports internal/saga for SagaTransitioned instead, and Go
// import cycles go one direction only. The JSON field names here
// (productSku, quantity) must stay in sync with that local copy by hand.
type OrderCreated struct {
	EventID    string `json:"eventId"`
	OrderID    string `json:"orderId"`
	CustomerID string `json:"customerId"`
	TotalCents int64  `json:"totalCents"`
	ProductSKU string `json:"productSku"`
	Quantity   int    `json:"quantity"`
	Version    int    `json:"version"`
}
