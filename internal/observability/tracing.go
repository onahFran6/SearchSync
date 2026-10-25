package observability

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// NewTracerProvider wires a batched OTLP/gRPC exporter to otlpEndpoint
// (host:port, no scheme - this stack always runs the collector in the same
// Compose network, never over TLS) and installs it as the process-wide
// TracerProvider with a W3C traceparent propagator. Installing both
// globally means every otel.Tracer(...) call site in this codebase - and
// otelhttp's middleware - share one provider without threading it through
// every constructor by hand.
//
// Call sites must defer the returned shutdown; it flushes any buffered
// spans and closes the exporter connection. If otlpEndpoint is empty,
// tracing is left on the SDK's no-op default (otel.Tracer calls become
// free no-ops) and shutdown is a no-op - this is how tracing stays
// optional for a binary that hasn't set an OTLP endpoint yet.
func NewTracerProvider(ctx context.Context, serviceName, otlpEndpoint string) (shutdown func(context.Context) error, err error) {
	if otlpEndpoint == "" {
		return func(context.Context) error { return nil }, nil
	}

	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(otlpEndpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		return nil, fmt.Errorf("create otlp trace exporter: %w", err)
	}

	res, err := resource.Merge(resource.Default(),
		resource.NewSchemaless(attribute.String("service.name", serviceName)))
	if err != nil {
		return nil, fmt.Errorf("build otel resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp.Shutdown, nil
}
