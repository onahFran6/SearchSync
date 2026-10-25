package indexer

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"go.opentelemetry.io/otel"

	"github.com/onahFran6/SearchSync/internal/cdc"
	"github.com/onahFran6/SearchSync/internal/platform"
	"github.com/onahFran6/SearchSync/internal/search"
)

const tracerName = "github.com/onahFran6/SearchSync/internal/indexer"

// ErrPermanent marks a failure that will never succeed on retry (a
// malformed envelope, an unsupported operation). classify checks for it
// with errors.Is before falling back to status-code-based classification,
// so the retry loop never burns its attempt budget on input the
// consumer can never parse.
var ErrPermanent = errors.New("indexer: permanent failure")

type Handler struct {
	client *search.Client
}

func NewHandler(client *search.Client) *Handler {
	return &Handler{client: client}
}

// HandleMessage applies one Kafka record to Elasticsearch. A nil value is a
// Debezium tombstone that always follows a delete event and carries no new
// information beyond it; it is ignored, not treated as an error.
func (h *Handler) HandleMessage(ctx context.Context, value []byte) error {
	if value == nil {
		return nil
	}

	env, err := cdc.DecodeEnvelope(value)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrPermanent, err)
	}

	ctx, span := otel.Tracer(tracerName).Start(ctx, "es.index")
	defer span.End()

	switch env.Op {
	case cdc.OpCreate, cdc.OpUpdate, cdc.OpSnapshotRead:
		row, err := env.Row()
		if err != nil {
			return fmt.Errorf("%w: %v", ErrPermanent, err)
		}
		doc := search.Document{
			SKU:         row.SKU,
			Name:        row.Name,
			Description: row.Description,
			Category:    row.Category,
			PriceCents:  row.PriceCents,
		}
		return h.client.IndexDocument(ctx, strconv.FormatInt(row.ID, 10), doc, env.Version())

	case cdc.OpDelete:
		id, err := env.DeletedID()
		if err != nil {
			return fmt.Errorf("%w: %v", ErrPermanent, err)
		}
		return h.client.DeleteDocument(ctx, strconv.FormatInt(id, 10), env.Version())

	default:
		return fmt.Errorf("%w: unsupported debezium op %q", ErrPermanent, env.Op)
	}
}

// classify distinguishes retryable Elasticsearch/network failures from
// permanent ones. A *search.StatusError's own HTTP status code decides
// retryability for ES-reported errors; anything else that isn't ErrPermanent
// (a connection refusal, a DNS failure, context.DeadlineExceeded) is treated
// as transient, since it says nothing about the request's validity.
func classify(err error) platform.FailureKind {
	if errors.Is(err, ErrPermanent) {
		return platform.FailurePermanent
	}
	var statusErr *search.StatusError
	if errors.As(err, &statusErr) {
		if statusErr.Temporary() {
			return platform.FailureTransient
		}
		return platform.FailurePermanent
	}
	return platform.FailureTransient
}
