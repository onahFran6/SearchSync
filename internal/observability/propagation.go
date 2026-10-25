package observability

import (
	"context"

	"go.opentelemetry.io/otel"
)

// mapCarrier adapts a single string value to propagation.TextMapCarrier.
// It is used in both directions: serializing the current span into a
// traceparent string for storage in an outbox row's trace_parent column
// (write side, TraceParentFromContext), and reconstructing a context from
// a stored/propagated traceparent string (read side,
// ContextFromTraceParent - used both here for outbox-table propagation and
// by internal/platform.ConsumerLoop for the Kafka-header carrier Debezium
// populates from that same column.
type mapCarrier map[string]string

func (c mapCarrier) Get(key string) string { return c[key] }
func (c mapCarrier) Set(key, value string) { c[key] = value }
func (c mapCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// TraceParentFromContext serializes ctx's current span into a W3C
// traceparent string, suitable for storing in an outbox row's
// trace_parent column. Returns "" when ctx carries no span context
// (tracing disabled, or called outside any span) - callers must write
// NULL in that case, not an empty-string column value a later Extract
// would fail to parse.
func TraceParentFromContext(ctx context.Context) string {
	carrier := mapCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return carrier["traceparent"]
}

// ContextFromTraceParent reconstructs a remote span context from a W3C
// traceparent string (as read from an outbox row or a Kafka header) and
// returns ctx with that span installed as the parent for the next span
// started from it. An empty or malformed traceParent leaves ctx
// unchanged, so the caller still gets a valid (root) trace rather than an
// error.
func ContextFromTraceParent(ctx context.Context, traceParent string) context.Context {
	if traceParent == "" {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, mapCarrier{"traceparent": traceParent})
}
