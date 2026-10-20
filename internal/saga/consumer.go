package saga

// orderCreatedEvent is a deliberately minimal, decode-only duplicate of
// internal/orders.OrderCreated's wire shape - not an import of that type.
//
// Why: internal/orders needs to import this package to decode
// SagaTransitioned (see internal/orders/sagaconsumer.go). If this package
// also imported internal/orders for OrderCreated, the two packages would
// import each other - not possible in Go. One side of this particular
// cycle has to duplicate a DTO rather than import it; this is the smaller
// side (four fields, a stable Stage 2 contract unlikely to change without
// everyone needing to know anyway) compared to orders needing this
// package's entire multi-message protocol. See state.go's package
// comment and docs/clarification/stage-04-ownership-decision.md for the
// full reasoning - this is a deliberate, narrow exception to the "import
// the producer's type, never duplicate" rule Stage 2 established, made
// necessary by Stage 4 being a genuinely multi-directional protocol where
// Stage 2's single-producer/single-consumer shape no longer applies.
type orderCreatedEvent struct {
	EventID    string `json:"eventId"`
	OrderID    string `json:"orderId"`
	TotalCents int64  `json:"totalCents"`
	ProductSKU string `json:"productSku"`
	Quantity   int    `json:"quantity"`
}
