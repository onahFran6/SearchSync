package saga

// Envelope carries the identity every command and event in this protocol
// needs for deduplication and traceability, per the implementation doc's
// hard constraint. Every concrete message type embeds it.
//
//   - MessageID is unique per message and is what each receiving service's
//     processed_commands/processed_messages unique constraint dedupes on -
//     the same mechanism as processed_events, one level up.
//   - CorrelationID and SagaID are both the order's id for this stage (one
//     saga per order; no separate saga id is minted). They are still carried as
//     distinct fields, not collapsed into OrderID, because CorrelationID is
//     the generic "which business process" tracing concept while SagaID is
//     specifically "which row in saga.sagas" - a later stage could split
//     them without changing every message's shape.
//   - CausationID is the MessageID of whatever message caused this one to
//     be produced, so a chain of commands/events can be walked backward.
//   - EventVersion is saga.sagas.version at the moment this message was
//     produced - the same optimistic-concurrency number the guarded UPDATE
//     in repository.go checks, included here so a receiver can order
//     messages about the same saga even if Kafka redelivers them
//     out of their original send order.
type Envelope struct {
	MessageID     string `json:"messageId"`
	CorrelationID string `json:"correlationId"`
	CausationID   string `json:"causationId"`
	SagaID        string `json:"sagaId"`
	OrderID       string `json:"orderId"`
	EventVersion  int    `json:"eventVersion"`
}
