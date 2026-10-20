package cdc

import "encoding/json"

// Envelope mirrors Debezium's per-table change-event shape. before/after are
// kept as json.RawMessage so the indexer can defer decoding them until it
// knows which operation it's handling.
type Envelope struct {
	Before json.RawMessage `json:"before"`
	After  json.RawMessage `json:"after"`
	Source Source          `json:"source"`
	Op     string          `json:"op"`
}

// Source is the subset of Debezium's per-record source metadata block the
// indexer needs for stale-replay protection. LSN is the PostgreSQL
// write-ahead-log sequence number for this change: monotonically
// increasing for the whole database, so it is a correct "did this already
// happen" ordering key across restarts, replays, and out-of-order
// redelivery, unlike wall-clock time (ts_ms) which only has millisecond
// resolution and is not guaranteed monotonic across snapshot vs streaming
// phases.
type Source struct {
	LSN int64 `json:"lsn"`
}

const (
	OpCreate       = "c"
	OpUpdate       = "u"
	OpDelete       = "d"
	OpSnapshotRead = "r"
)
