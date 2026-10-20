package saga

// AggregateType values are the saga schema's outbox_events.aggregatetype
// column values. Each one routes to its own, single-message-type Kafka
// topic through the Debezium Outbox Event Router's
// route.by.field=aggregatetype (infrastructure/debezium/saga-connector.json)
// - the same mechanism, and the same one-type-per-topic granularity, Stage
// 2 established for OrderCreated. A consumer that subscribes to one of
// these topics never needs to inspect a Kafka header to know which struct
// to decode into; the topic itself says so, exactly like
// searchsync.outbox.event.orders only ever carries OrderCreated.
const (
	// AggregateTypeReserveInventory routes to
	// searchsync.outbox.event.reserve_inventory, consumed only by
	// internal/inventory.
	AggregateTypeReserveInventory = "reserve_inventory"
	// AggregateTypeReleaseInventory routes to
	// searchsync.outbox.event.release_inventory, consumed only by
	// internal/inventory.
	AggregateTypeReleaseInventory = "release_inventory"
	// AggregateTypeAuthorizePayment routes to
	// searchsync.outbox.event.authorize_payment, consumed only by
	// internal/payments.
	AggregateTypeAuthorizePayment = "authorize_payment"
	// AggregateTypeSagaTransitioned routes to
	// searchsync.outbox.event.saga_transitioned, consumed only by
	// internal/orders. Deliberately a distinct topic from
	// searchsync.outbox.event.orders (OrderCreated's topic, produced by a
	// different connector reading the orders schema's own outbox) so the
	// existing notifications consumer group never sees a message shape it
	// doesn't understand.
	AggregateTypeSagaTransitioned = "saga_transitioned"
)

// Type values are the outbox_events.type column values - diagnostic labels
// only (they still land on the Kafka record as the eventType header via
// transforms.outbox.table.fields.additional.placement, useful when reading
// raw records by hand), since AggregateType above is what actually
// determines which topic, and therefore which single message shape, a
// consumer ever sees.
const (
	TypeReserveInventory = "ReserveInventory"
	TypeReleaseInventory = "ReleaseInventory"
	TypeAuthorizePayment = "AuthorizePayment"
	TypeSagaTransitioned = "SagaTransitioned"
)
