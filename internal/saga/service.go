package saga

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/francisonah/searchsync/internal/outbox"
)

// ErrPermanent marks a failure that will never succeed on retry (a
// malformed message payload).
var ErrPermanent = errors.New("saga: permanent failure")

// Config bounds the coordinator's two durable timeouts. Both are
// deliberately short defaults suited to local experiments, not production
// SLAs - see cmd/saga-coordinator/main.go for how to override them.
type Config struct {
	// PaymentTimeout is how long a saga waits in STOCK_RESERVED for a
	// PaymentAuthorized/PaymentDeclined event before the poller treats
	// payment as timed out.
	PaymentTimeout time.Duration
	// CompensationRetryInterval is how long the poller waits between
	// ReleaseInventory retry attempts while COMPENSATING.
	CompensationRetryInterval time.Duration
	// MaxCompensationAttempts bounds compensation retries before
	// escalating to NEEDS_ATTENTION (Experiment #6).
	MaxCompensationAttempts int
	// PollInterval is how often the poller checks for due deadlines.
	PollInterval time.Duration
}

// DefaultConfig is tuned for local demos and experiments: short enough to
// observe a timeout within a test run, long enough that a normal
// request/reply round trip through Kafka never spuriously times out.
var DefaultConfig = Config{
	PaymentTimeout:            10 * time.Second,
	CompensationRetryInterval: 5 * time.Second,
	MaxCompensationAttempts:   3,
	PollInterval:              2 * time.Second,
}

type Service struct {
	pool *pgxpool.Pool
	repo *Repository
	cfg  Config
}

func NewService(pool *pgxpool.Pool, repo *Repository, cfg Config) *Service {
	return &Service{pool: pool, repo: repo, cfg: cfg}
}

// HandleOrderCreated starts a new saga. It is the only handler that
// inserts a saga.sagas row rather than transitioning an existing one.
func (s *Service) HandleOrderCreated(ctx context.Context, value []byte) error {
	var evt orderCreatedEvent
	if err := json.Unmarshal(value, &evt); err != nil {
		return fmt.Errorf("%w: decode OrderCreated: %v", ErrPermanent, err)
	}
	if evt.EventID == "" || evt.OrderID == "" || evt.ProductSKU == "" || evt.Quantity <= 0 {
		return fmt.Errorf("%w: OrderCreated missing required fields", ErrPermanent)
	}

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	isNew, err := s.repo.InsertProcessedMessage(ctx, tx, evt.EventID)
	if err != nil {
		return err
	}
	if !isNew {
		return tx.Commit(ctx)
	}

	if err := s.repo.InsertSaga(ctx, tx, Row{
		OrderID: evt.OrderID, State: StatePending,
		SKU: evt.ProductSKU, Quantity: evt.Quantity, AmountCents: evt.TotalCents,
	}); err != nil {
		return err
	}

	cmd := ReserveInventory{
		Envelope: Envelope{
			MessageID: uuid.New().String(), CorrelationID: evt.OrderID, CausationID: evt.EventID,
			SagaID: evt.OrderID, OrderID: evt.OrderID, EventVersion: 0,
		},
		SKU: evt.ProductSKU, Quantity: evt.Quantity,
	}
	if err := s.publish(ctx, tx, evt.OrderID, AggregateTypeReserveInventory, TypeReserveInventory, cmd); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// HandleInventoryReserved transitions PENDING -> STOCK_RESERVED, sets the
// payment deadline, and dispatches AuthorizePayment.
func (s *Service) HandleInventoryReserved(ctx context.Context, value []byte) error {
	var evt InventoryReserved
	if err := json.Unmarshal(value, &evt); err != nil {
		return fmt.Errorf("%w: decode InventoryReserved: %v", ErrPermanent, err)
	}

	return s.withTx(ctx, evt.MessageID, func(tx pgx.Tx) error {
		row, found, err := s.repo.Get(ctx, tx, evt.OrderID)
		if err != nil || !found {
			return err
		}

		newVersion, ok, err := s.repo.GuardedTransition(ctx, tx, evt.OrderID, StatePending, StateStockReserved)
		if err != nil || !ok {
			return err
		}

		if err := s.repo.SetPaymentDeadline(ctx, tx, evt.OrderID, time.Now().Add(s.cfg.PaymentTimeout)); err != nil {
			return err
		}

		cmd := AuthorizePayment{
			Envelope:    replyEnvelope(evt.Envelope, newVersion),
			AmountCents: row.AmountCents,
		}
		if err := s.publish(ctx, tx, evt.OrderID, AggregateTypeAuthorizePayment, TypeAuthorizePayment, cmd); err != nil {
			return err
		}
		return s.notify(ctx, tx, evt.Envelope, newVersion, StatePending, StateStockReserved, "inventory_reserved")
	})
}

// HandleInventoryRejected transitions PENDING -> REJECTED. Terminal: no
// payment is ever attempted for an order whose stock could not be
// reserved (Experiment #2).
func (s *Service) HandleInventoryRejected(ctx context.Context, value []byte) error {
	var evt InventoryRejected
	if err := json.Unmarshal(value, &evt); err != nil {
		return fmt.Errorf("%w: decode InventoryRejected: %v", ErrPermanent, err)
	}

	return s.withTx(ctx, evt.MessageID, func(tx pgx.Tx) error {
		newVersion, ok, err := s.repo.GuardedTransition(ctx, tx, evt.OrderID, StatePending, StateRejected)
		if err != nil || !ok {
			return err
		}
		return s.notify(ctx, tx, evt.Envelope, newVersion, StatePending, StateRejected, evt.Reason)
	})
}

// HandlePaymentAuthorized transitions STOCK_RESERVED -> CONFIRMED.
// Terminal success path (Experiment #1).
func (s *Service) HandlePaymentAuthorized(ctx context.Context, value []byte) error {
	var evt PaymentAuthorized
	if err := json.Unmarshal(value, &evt); err != nil {
		return fmt.Errorf("%w: decode PaymentAuthorized: %v", ErrPermanent, err)
	}

	return s.withTx(ctx, evt.MessageID, func(tx pgx.Tx) error {
		newVersion, ok, err := s.repo.GuardedTransition(ctx, tx, evt.OrderID, StateStockReserved, StateConfirmed)
		if err != nil || !ok {
			return err
		}
		if err := s.repo.ClearPaymentDeadline(ctx, tx, evt.OrderID); err != nil {
			return err
		}
		return s.notify(ctx, tx, evt.Envelope, newVersion, StateStockReserved, StateConfirmed, "payment_authorized")
	})
}

// HandlePaymentDeclined transitions STOCK_RESERVED -> COMPENSATING and
// dispatches the first ReleaseInventory attempt (Experiment #3).
func (s *Service) HandlePaymentDeclined(ctx context.Context, value []byte) error {
	var evt PaymentDeclined
	if err := json.Unmarshal(value, &evt); err != nil {
		return fmt.Errorf("%w: decode PaymentDeclined: %v", ErrPermanent, err)
	}

	return s.withTx(ctx, evt.MessageID, func(tx pgx.Tx) error {
		return s.beginCompensation(ctx, tx, evt.Envelope, "payment_declined")
	})
}

// HandleInventoryReleased transitions COMPENSATING -> CANCELLED.
func (s *Service) HandleInventoryReleased(ctx context.Context, value []byte) error {
	var evt InventoryReleased
	if err := json.Unmarshal(value, &evt); err != nil {
		return fmt.Errorf("%w: decode InventoryReleased: %v", ErrPermanent, err)
	}

	return s.withTx(ctx, evt.MessageID, func(tx pgx.Tx) error {
		newVersion, ok, err := s.repo.GuardedTransition(ctx, tx, evt.OrderID, StateCompensating, StateCancelled)
		if err != nil || !ok {
			return err
		}
		if err := s.repo.ClearCompensation(ctx, tx, evt.OrderID); err != nil {
			return err
		}
		return s.notify(ctx, tx, evt.Envelope, newVersion, StateCompensating, StateCancelled, "inventory_released")
	})
}

// HandleInventoryReleaseFailed transitions COMPENSATING -> NEEDS_ATTENTION
// immediately - Inventory's explicit "this will not succeed" signal, as
// opposed to silence (which the poller's AttemptCompensationRetry
// exhaustion path handles instead).
func (s *Service) HandleInventoryReleaseFailed(ctx context.Context, value []byte) error {
	var evt InventoryReleaseFailed
	if err := json.Unmarshal(value, &evt); err != nil {
		return fmt.Errorf("%w: decode InventoryReleaseFailed: %v", ErrPermanent, err)
	}

	return s.withTx(ctx, evt.MessageID, func(tx pgx.Tx) error {
		newVersion, ok, err := s.repo.GuardedTransition(ctx, tx, evt.OrderID, StateCompensating, StateNeedsAttention)
		if err != nil || !ok {
			return err
		}
		if err := s.repo.ClearCompensation(ctx, tx, evt.OrderID); err != nil {
			return err
		}
		return s.notify(ctx, tx, evt.Envelope, newVersion, StateCompensating, StateNeedsAttention, evt.Reason)
	})
}

// beginCompensation is shared by HandlePaymentDeclined and the poller's
// payment-timeout path (poller.go): both move STOCK_RESERVED ->
// COMPENSATING and dispatch the first ReleaseInventory the same way, only
// the triggering reason differs.
func (s *Service) beginCompensation(ctx context.Context, tx pgx.Tx, cause Envelope, reason string) error {
	row, found, err := s.repo.Get(ctx, tx, cause.OrderID)
	if err != nil || !found {
		return err
	}

	newVersion, ok, err := s.repo.GuardedTransition(ctx, tx, cause.OrderID, StateStockReserved, StateCompensating)
	if err != nil || !ok {
		return err
	}
	if err := s.repo.ClearPaymentDeadline(ctx, tx, cause.OrderID); err != nil {
		return err
	}
	if err := s.repo.SetCompensation(ctx, tx, cause.OrderID, time.Now().Add(s.cfg.CompensationRetryInterval), 1); err != nil {
		return err
	}

	cmd := ReleaseInventory{
		Envelope: replyEnvelope(cause, newVersion),
		SKU:      row.SKU, Quantity: row.Quantity, Reason: reason,
	}
	if err := s.publish(ctx, tx, cause.OrderID, AggregateTypeReleaseInventory, TypeReleaseInventory, cmd); err != nil {
		return err
	}
	return s.notify(ctx, tx, cause, newVersion, StateStockReserved, StateCompensating, reason)
}

// withTx wraps the common "begin tx, dedup by messageID, run fn, commit"
// shape every event handler above shares.
func (s *Service) withTx(ctx context.Context, messageID string, fn func(tx pgx.Tx) error) error {
	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	isNew, err := s.repo.InsertProcessedMessage(ctx, tx, messageID)
	if err != nil {
		return err
	}
	if !isNew {
		return tx.Commit(ctx)
	}

	if err := fn(tx); err != nil {
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
	})
}

func (s *Service) notify(ctx context.Context, tx pgx.Tx, cause Envelope, newVersion int, from, to State, reason string) error {
	notif := SagaTransitioned{
		Envelope:  replyEnvelope(cause, newVersion),
		FromState: from, ToState: to, Reason: reason,
	}
	return s.publish(ctx, tx, cause.OrderID, AggregateTypeSagaTransitioned, TypeSagaTransitioned, notif)
}

// replyEnvelope builds the envelope for a message this coordinator emits
// in reaction to `cause`: a fresh MessageID, CausationID set to cause's
// own MessageID, everything else carried through, EventVersion set to the
// version the transition just produced (not cause.EventVersion) so a
// downstream consumer can tell messages about the same saga apart even if
// Kafka redelivers them out of order.
func replyEnvelope(cause Envelope, newVersion int) Envelope {
	return Envelope{
		MessageID:     uuid.New().String(),
		CorrelationID: cause.CorrelationID,
		CausationID:   cause.MessageID,
		SagaID:        cause.SagaID,
		OrderID:       cause.OrderID,
		EventVersion:  newVersion,
	}
}
