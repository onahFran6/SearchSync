package saga

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// RunPoller is the durable timeout mechanism the implementation doc
// requires: every tick, it checks Postgres for sagas whose deadline has
// passed, never a process-local timer. A crash or restart loses nothing,
// because the deadline a tick acts on was persisted by whichever
// transaction set it (HandleInventoryReserved for payment deadlines,
// beginCompensation/this poller itself for compensation deadlines) - the
// next process to run this loop, on any host, finds the exact same due
// work by querying the same table.
func (s *Service) RunPoller(ctx context.Context, logger *slog.Logger) error {
	ticker := time.NewTicker(s.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := s.pollOnce(ctx, logger); err != nil {
				logger.Error("saga poller tick failed", "error", err)
			}
		}
	}
}

func (s *Service) pollOnce(ctx context.Context, logger *slog.Logger) error {
	now := time.Now()

	timedOut, err := s.repo.ListDuePaymentTimeouts(ctx, s.pool, now)
	if err != nil {
		return fmt.Errorf("list due payment timeouts: %w", err)
	}
	for _, row := range timedOut {
		if err := s.handlePaymentTimeout(ctx, row); err != nil {
			logger.Error("handle payment timeout", "order_id", row.OrderID, "error", err)
		}
	}

	dueCompensation, err := s.repo.ListDueCompensationRetries(ctx, s.pool, now)
	if err != nil {
		return fmt.Errorf("list due compensation retries: %w", err)
	}
	for _, row := range dueCompensation {
		if err := s.handleCompensationDue(ctx, row, logger); err != nil {
			logger.Error("handle compensation retry", "order_id", row.OrderID, "error", err)
		}
	}
	return nil
}

// handlePaymentTimeout is the TriggerPaymentTimedOut path: no Kafka
// message caused this, so there is no incoming messageID to dedup by -
// the dedup here is GuardedTransition's own WHERE clause (it only applies
// if the saga is still exactly STOCK_RESERVED), which is why a second
// poll tick finding the same row again (if it somehow still matched the
// now-stale deadline predicate) is still safe: the first tick's
// transition already moved the state away from STOCK_RESERVED.
func (s *Service) handlePaymentTimeout(ctx context.Context, row Row) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	cause := Envelope{
		MessageID:     uuid.New().String(),
		CorrelationID: row.OrderID,
		SagaID:        row.OrderID,
		OrderID:       row.OrderID,
		EventVersion:  row.Version,
	}
	if err := s.beginCompensation(ctx, tx, cause, "payment_timed_out"); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// handleCompensationDue is Experiment #6's other half: retry
// ReleaseInventory, or escalate to NEEDS_ATTENTION once
// cfg.MaxCompensationAttempts is reached. AttemptCompensationRetry
// decides which, atomically, in Postgres; this function only needs to act
// on what it reports.
func (s *Service) handleCompensationDue(ctx context.Context, row Row, logger *slog.Logger) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	updated, escalated, found, err := s.repo.AttemptCompensationRetry(
		ctx, tx, row.OrderID, time.Now(), time.Now().Add(s.cfg.CompensationRetryInterval), s.cfg.MaxCompensationAttempts)
	if err != nil {
		return err
	}
	if !found {
		// Raced with a Kafka-driven InventoryReleased/InventoryReleaseFailed
		// between the listing SELECT and this UPDATE - already resolved,
		// nothing to do.
		return tx.Commit(ctx)
	}

	cause := Envelope{
		MessageID:     uuid.New().String(),
		CorrelationID: updated.OrderID,
		SagaID:        updated.OrderID,
		OrderID:       updated.OrderID,
		EventVersion:  updated.Version,
	}

	if escalated {
		logger.Error("compensation exhausted, escalating to needs_attention",
			"order_id", updated.OrderID, "attempts", updated.CompensationAttempts)
		if err := s.notify(ctx, tx, cause, updated.Version, StateCompensating, StateNeedsAttention,
			fmt.Sprintf("compensation_exhausted_after_%d_attempts", updated.CompensationAttempts)); err != nil {
			return err
		}
	} else {
		logger.Warn("retrying compensation", "order_id", updated.OrderID, "attempt", updated.CompensationAttempts)
		cmd := ReleaseInventory{
			Envelope: replyEnvelope(cause, updated.Version),
			SKU:      updated.SKU, Quantity: updated.Quantity, Reason: "compensation_retry",
		}
		if err := s.publish(ctx, tx, updated.OrderID, AggregateTypeReleaseInventory, TypeReleaseInventory, cmd); err != nil {
			return err
		}
	}
	return tx.Commit(ctx)
}
