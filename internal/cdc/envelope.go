package cdc

import "encoding/json"

// Envelope mirrors Debezium's per-table change-event shape. before/after are
// kept as json.RawMessage so the indexer can defer decoding them until it
// knows which operation it's handling.
type Envelope struct {
	Before json.RawMessage `json:"before"`
	After  json.RawMessage `json:"after"`
	Op     string          `json:"op"`
}

const (
	OpCreate       = "c"
	OpUpdate       = "u"
	OpDelete       = "d"
	OpSnapshotRead = "r"
)
