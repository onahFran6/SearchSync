package outbox

import "encoding/json"

// Event mirrors the outbox_events columns, named to match the Debezium
// Outbox Event Router's defaults (table.field.event.id=id,
// table.field.event.key=aggregateid, route.by.field=aggregatetype) so the
// connector config needs no field-name overrides.
type Event struct {
	ID            string
	AggregateType string
	AggregateID   string
	Type          string
	Payload       json.RawMessage
	// TraceParent is a W3C traceparent string (or "" if the write happened
	// outside any traced span), persisted so Debezium's outbox EventRouter
	// can place it as a literal Kafka header. Populate it with
	// internal/observability.TraceParentFromContext(ctx).
	TraceParent string
}
