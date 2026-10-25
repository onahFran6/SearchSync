package observability

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// RunReplicationSlotLagPoller periodically reads every logical replication
// slot's retained WAL (current WAL position minus the slot's restart_lsn)
// and feeds it into metrics as postgres_replication_slot_retained_bytes.
// One binary polling this is enough - it's a database-wide view, not
// per-service state - so only cmd/api runs it, not every binary with a
// Postgres connection.
func RunReplicationSlotLagPoller(ctx context.Context, pool *pgxpool.Pool, metrics *Metrics, interval time.Duration, logger *slog.Logger) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pollReplicationSlotsOnce(ctx, pool, metrics, logger)
		}
	}
}

func pollReplicationSlotsOnce(ctx context.Context, pool *pgxpool.Pool, metrics *Metrics, logger *slog.Logger) {
	const query = `
		SELECT slot_name, pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)
		FROM pg_replication_slots
		WHERE restart_lsn IS NOT NULL`

	rows, err := pool.Query(ctx, query)
	if err != nil {
		logger.Warn("replication slot lag poll failed", "error", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var slotName string
		var retainedBytes float64
		if err := rows.Scan(&slotName, &retainedBytes); err != nil {
			logger.Warn("scan replication slot row failed", "error", err)
			return
		}
		metrics.SetReplicationSlotBytes(slotName, retainedBytes)
	}
	if err := rows.Err(); err != nil {
		logger.Warn("iterate replication slot rows failed", "error", err)
	}
}
