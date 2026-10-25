package payments

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

// ErrPermanent marks a failure that will never succeed on retry.
var ErrPermanent = errors.New("payments: permanent failure")

type Service struct {
	pool *pgxpool.Pool
	repo *Repository
}

func NewService(pool *pgxpool.Pool, repo *Repository) *Service {
	return &Service{pool: pool, repo: repo}
}

// HandleAuthorizePayment applies one AuthorizePayment command. Simulation
// only, per the implementation doc's hard constraint - no real card data
// or external processor is ever involved, and the outcome is entirely
// determined by the locally configured mode (POST /dev/payments/mode),
// never by AmountCents or any other field of the command.
//
// In "timeout" mode, this deliberately commits the dedup row and produces
// no outbox event at all - simulating a payment processor that never
// responds. There is no PaymentTimedOut event; from the saga
// coordinator's point of view, this looks exactly like silence, and
// silence is what its persisted-deadline poller exists to detect (see
// internal/saga/poller.go), not a third response type.
func (s *Service) HandleAuthorizePayment(ctx context.Context, value []byte) error {
	var cmd saga.AuthorizePayment
	if err := json.Unmarshal(value, &cmd); err != nil {
		return fmt.Errorf("%w: decode AuthorizePayment: %v", ErrPermanent, err)
	}
	if cmd.MessageID == "" || cmd.OrderID == "" {
		return fmt.Errorf("%w: AuthorizePayment missing required fields", ErrPermanent)
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
		// protocol level - tx.Commit would itself fail with
		// pgx.ErrTxCommitRollback. Returning nil lets the deferred
		// tx.Rollback discard the aborted transaction, the correct no-op
		// for an already-applied message.
		return nil
	}

	existingStatus, found, err := s.repo.GetAuthorization(ctx, tx, cmd.OrderID)
	if err != nil {
		return err
	}
	if found {
		// Duplicate AuthorizePayment under a different message id - replay
		// the original decision. Never re-roll against the current mode:
		// the mode may have changed since the first attempt, and a real
		// payment processor would not re-decide an already-settled
		// transaction either.
		return s.replyAndCommit(ctx, tx, cmd.Envelope, existingStatus)
	}

	mode, err := s.repo.GetMode(ctx, tx)
	if err != nil {
		return err
	}

	switch mode {
	case ModeSuccess:
		if err := s.repo.InsertAuthorization(ctx, tx, cmd.OrderID, StatusAuthorized, mode); err != nil {
			return err
		}
		return s.replyAndCommit(ctx, tx, cmd.Envelope, StatusAuthorized)
	case ModeDecline:
		if err := s.repo.InsertAuthorization(ctx, tx, cmd.OrderID, StatusDeclined, mode); err != nil {
			return err
		}
		return s.replyAndCommit(ctx, tx, cmd.Envelope, StatusDeclined)
	case ModeTimeout:
		return tx.Commit(ctx)
	default:
		return fmt.Errorf("%w: unknown payments mode %q", ErrPermanent, mode)
	}
}

func (s *Service) replyAndCommit(ctx context.Context, tx pgx.Tx, cause saga.Envelope, status AuthorizationStatus) error {
	reply := saga.Envelope{
		MessageID:     uuid.New().String(),
		CorrelationID: cause.CorrelationID,
		CausationID:   cause.MessageID,
		SagaID:        cause.SagaID,
		OrderID:       cause.OrderID,
		EventVersion:  cause.EventVersion,
	}

	var (
		aggregateType, eventType string
		event                    any
	)
	switch status {
	case StatusAuthorized:
		aggregateType, eventType = AggregateTypePaymentAuthorized, TypePaymentAuthorized
		event = saga.PaymentAuthorized{Envelope: reply}
	case StatusDeclined:
		aggregateType, eventType = AggregateTypePaymentDeclined, TypePaymentDeclined
		event = saga.PaymentDeclined{Envelope: reply, Reason: "declined"}
	default:
		return fmt.Errorf("%w: unknown authorization status %q", ErrPermanent, status)
	}

	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal %s: %w", eventType, err)
	}
	if err := s.repo.InsertOutboxEvent(ctx, tx, outbox.Event{
		ID:            uuid.New().String(),
		AggregateType: aggregateType,
		AggregateID:   cause.OrderID,
		Type:          eventType,
		Payload:       payload,
		TraceParent:   observability.TraceParentFromContext(ctx),
	}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
