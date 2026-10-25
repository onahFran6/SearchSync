package inventory

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/francisonah/searchsync/internal/observability"
	"github.com/francisonah/searchsync/internal/outbox"
	"github.com/francisonah/searchsync/internal/saga"
)

// ErrPermanent marks a failure that will never succeed on retry (a
// malformed command payload) - classified by Classify in worker.go ahead
// of any Postgres-error-code inspection, same convention as
// internal/notifications.
var ErrPermanent = errors.New("inventory: permanent failure")

// errDevInjectedFailure is deliberately transient: it exists only so
// Experiment #6 can demonstrate ConsumerLoop's retry behavior and, with
// enough injected failures, the saga's own compensation-retry-then-
// NEEDS_ATTENTION path. See Repository.ConsumeDevReleaseFailure.
var errDevInjectedFailure = errors.New("inventory: dev-injected release failure")

type Service struct {
	pool *pgxpool.Pool
	repo *Repository
}

func NewService(pool *pgxpool.Pool, repo *Repository) *Service {
	return &Service{pool: pool, repo: repo}
}

// HandleReserveInventory applies one ReserveInventory command. Dedup,
// reservation lookup, the conditional stock decrement, and the outgoing
// outbox event are all one local transaction - the same
// "consumed-message-id + state-change + next-event" atomicity
// established for orders, applied here to Inventory's own schema.
func (s *Service) HandleReserveInventory(ctx context.Context, value []byte) error {
	var cmd saga.ReserveInventory
	if err := json.Unmarshal(value, &cmd); err != nil {
		return fmt.Errorf("%w: decode ReserveInventory: %v", ErrPermanent, err)
	}
	if cmd.MessageID == "" || cmd.OrderID == "" || cmd.SKU == "" || cmd.Quantity <= 0 {
		return fmt.Errorf("%w: ReserveInventory missing required fields", ErrPermanent)
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	isNew, err := s.repo.InsertProcessedCommand(ctx, tx, cmd.MessageID)
	if err != nil {
		return err
	}
	if !isNew {
		// A unique-violation here already aborted tx at the Postgres
		// protocol level (any error inside a transaction poisons it until
		// ROLLBACK) - tx.Commit would itself fail with
		// pgx.ErrTxCommitRollback. Returning nil and letting the deferred
		// tx.Rollback discard the aborted transaction is the correct
		// no-op; there is nothing to persist for an already-applied
		// message anyway.
		return nil
	}

	_, found, err := s.repo.GetReservation(ctx, tx, cmd.OrderID)
	if err != nil {
		return err
	}

	var event saga.InventoryReserved
	if found {
		// A reservation already exists for this order under a different
		// message id than the one just deduplicated above - a duplicate
		// ReserveInventory command (Experiment #4), most likely the saga
		// retrying after never seeing the first ack. Re-acknowledge
		// without touching stock a second time: the unique PRIMARY KEY on
		// inventory.reservations.order_id is what actually enforces
		// "never reserve twice," this branch is just the idempotent
		// response to it.
		event = saga.InventoryReserved{Envelope: replyEnvelope(cmd.Envelope)}
	} else {
		ok, err := s.repo.TryReserveStock(ctx, tx, cmd.SKU, cmd.Quantity)
		if err != nil {
			return err
		}
		if !ok {
			if err := s.publish(ctx, tx, cmd.OrderID, AggregateTypeInventoryRejected, TypeInventoryRejected,
				saga.InventoryRejected{Envelope: replyEnvelope(cmd.Envelope), Reason: "out_of_stock"}); err != nil {
				return err
			}
			return tx.Commit(ctx)
		}
		if err := s.repo.InsertReservation(ctx, tx, Reservation{
			OrderID: cmd.OrderID, SKU: cmd.SKU, Quantity: cmd.Quantity, Status: ReservationReserved,
		}); err != nil {
			return err
		}
		event = saga.InventoryReserved{Envelope: replyEnvelope(cmd.Envelope)}
	}

	if err := s.publish(ctx, tx, cmd.OrderID, AggregateTypeInventoryReserved, TypeInventoryReserved, event); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// HandleReleaseInventory applies one ReleaseInventory command.
func (s *Service) HandleReleaseInventory(ctx context.Context, value []byte) error {
	var cmd saga.ReleaseInventory
	if err := json.Unmarshal(value, &cmd); err != nil {
		return fmt.Errorf("%w: decode ReleaseInventory: %v", ErrPermanent, err)
	}
	if cmd.MessageID == "" || cmd.OrderID == "" {
		return fmt.Errorf("%w: ReleaseInventory missing required fields", ErrPermanent)
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	isNew, err := s.repo.InsertProcessedCommand(ctx, tx, cmd.MessageID)
	if err != nil {
		return err
	}
	if !isNew {
		// A unique-violation here already aborted tx at the Postgres
		// protocol level (any error inside a transaction poisons it until
		// ROLLBACK) - tx.Commit would itself fail with
		// pgx.ErrTxCommitRollback. Returning nil and letting the deferred
		// tx.Rollback discard the aborted transaction is the correct
		// no-op; there is nothing to persist for an already-applied
		// message anyway.
		return nil
	}

	// Deliberately checked (and, if triggered, returned) before any other
	// write: rolling back must undo the processed_commands insert too, so
	// a retried delivery of the exact same message_id is seen as "new"
	// again and can consume another unit of the injected failure budget.
	shouldFail, err := s.repo.ConsumeDevReleaseFailure(ctx, tx)
	if err != nil {
		return err
	}
	if shouldFail {
		return errDevInjectedFailure
	}

	existing, found, err := s.repo.GetReservation(ctx, tx, cmd.OrderID)
	if err != nil {
		return err
	}
	if !found {
		if err := s.publish(ctx, tx, cmd.OrderID, AggregateTypeInventoryReleaseFailed, TypeInventoryReleaseFailed,
			saga.InventoryReleaseFailed{Envelope: replyEnvelope(cmd.Envelope), Reason: "no reservation found for order"}); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}

	if existing.Status == ReservationReleased {
		// Already released under a different message id - idempotent
		// re-ack, no second stock credit.
		if err := s.publish(ctx, tx, cmd.OrderID, AggregateTypeInventoryReleased, TypeInventoryReleased,
			saga.InventoryReleased{Envelope: replyEnvelope(cmd.Envelope)}); err != nil {
			return err
		}
		return tx.Commit(ctx)
	}

	if err := s.repo.CreditStock(ctx, tx, existing.SKU, existing.Quantity); err != nil {
		return err
	}
	if err := s.repo.MarkReleased(ctx, tx, cmd.OrderID); err != nil {
		return err
	}
	if err := s.publish(ctx, tx, cmd.OrderID, AggregateTypeInventoryReleased, TypeInventoryReleased,
		saga.InventoryReleased{Envelope: replyEnvelope(cmd.Envelope)}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Service) publish(ctx context.Context, tx pgx.Tx, orderID, aggregateType, eventType string, event any) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", eventType, err)
	}
	return s.repo.InsertOutboxEvent(ctx, tx, outbox.Event{
		ID:            uuid.New().String(),
		AggregateType: aggregateType,
		AggregateID:   orderID,
		Type:          eventType,
		Payload:       payload,
		TraceParent:   observability.TraceParentFromContext(ctx),
	})
}

// replyEnvelope builds the envelope for a reply event: a fresh MessageID
// (this is a new message), CausationID set to the command that caused it,
// everything else (CorrelationID/SagaID/OrderID/EventVersion) carried
// through unchanged so the coordinator can match the reply back to the
// saga state it was in when it issued the command.
func replyEnvelope(cause saga.Envelope) saga.Envelope {
	return saga.Envelope{
		MessageID:     uuid.New().String(),
		CorrelationID: cause.CorrelationID,
		CausationID:   cause.MessageID,
		SagaID:        cause.SagaID,
		OrderID:       cause.OrderID,
		EventVersion:  cause.EventVersion,
	}
}
