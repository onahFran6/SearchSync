package cdc

import (
	"encoding/json"
	"fmt"
)

// ProductRow is the subset of the products table columns the indexer needs,
// decoded from Debezium's before/after representation.
type ProductRow struct {
	ID          int64  `json:"id"`
	SKU         string `json:"sku"`
	Name        string `json:"name"`
	Description string `json:"description"`
	Category    string `json:"category"`
	PriceCents  int    `json:"price_cents"`
}

// DecodeEnvelope parses a Kafka record value into an Envelope. A nil value
// (Debezium tombstone) is not an envelope and must be checked by the caller
// before calling this function.
func DecodeEnvelope(raw []byte) (Envelope, error) {
	var env Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return Envelope{}, fmt.Errorf("decode debezium envelope: %w", err)
	}
	return env, nil
}

// Row decodes the row to index for create/update/snapshot-read events.
func (e Envelope) Row() (ProductRow, error) {
	switch e.Op {
	case OpCreate, OpUpdate, OpSnapshotRead:
		var row ProductRow
		if err := json.Unmarshal(e.After, &row); err != nil {
			return ProductRow{}, fmt.Errorf("decode after state: %w", err)
		}
		return row, nil
	default:
		return ProductRow{}, fmt.Errorf("no row to index for op %q", e.Op)
	}
}

// DeletedID decodes the primary key of a deleted row from the before state.
// Debezium's default REPLICA IDENTITY only guarantees the key column(s) are
// present in "before", not the full prior row.
func (e Envelope) DeletedID() (int64, error) {
	var before struct {
		ID int64 `json:"id"`
	}
	if err := json.Unmarshal(e.Before, &before); err != nil {
		return 0, fmt.Errorf("decode before state: %w", err)
	}
	return before.ID, nil
}
