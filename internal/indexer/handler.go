package indexer

import (
	"context"
	"fmt"
	"strconv"

	"github.com/francisonah/searchsync/internal/cdc"
	"github.com/francisonah/searchsync/internal/search"
)

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
		return err
	}

	switch env.Op {
	case cdc.OpCreate, cdc.OpUpdate, cdc.OpSnapshotRead:
		row, err := env.Row()
		if err != nil {
			return err
		}
		doc := search.Document{
			SKU:         row.SKU,
			Name:        row.Name,
			Description: row.Description,
			Category:    row.Category,
			PriceCents:  row.PriceCents,
		}
		return h.client.IndexDocument(ctx, strconv.FormatInt(row.ID, 10), doc)

	case cdc.OpDelete:
		id, err := env.DeletedID()
		if err != nil {
			return err
		}
		return h.client.DeleteDocument(ctx, strconv.FormatInt(id, 10))

	default:
		return fmt.Errorf("unsupported debezium op %q", env.Op)
	}
}
