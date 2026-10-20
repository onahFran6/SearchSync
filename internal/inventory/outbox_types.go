package inventory

// AggregateType values are inventory.outbox_events.aggregatetype column
// values, each routing to its own topic via
// infrastructure/debezium/inventory-connector.json - see
// internal/saga/outbox_types.go for the full reasoning (one message type
// per topic, no header inspection needed by the consumer).
const (
	AggregateTypeInventoryReserved      = "inventory_reserved"
	AggregateTypeInventoryRejected      = "inventory_rejected"
	AggregateTypeInventoryReleased      = "inventory_released"
	AggregateTypeInventoryReleaseFailed = "inventory_release_failed"
)

const (
	TypeInventoryReserved      = "InventoryReserved"
	TypeInventoryRejected      = "InventoryRejected"
	TypeInventoryReleased      = "InventoryReleased"
	TypeInventoryReleaseFailed = "InventoryReleaseFailed"
)
