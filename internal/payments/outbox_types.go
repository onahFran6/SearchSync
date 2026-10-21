package payments

// AggregateType values are payments.outbox_events.aggregatetype column
// values, each routing to its own topic via
// infrastructure/debezium/payments-connector.json - see
// internal/saga/outbox_types.go for the full reasoning.
const (
	AggregateTypePaymentAuthorized = "payment_authorized"
	AggregateTypePaymentDeclined   = "payment_declined"
)

const (
	TypePaymentAuthorized = "PaymentAuthorized"
	TypePaymentDeclined   = "PaymentDeclined"
)
