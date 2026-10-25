package orders

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.opentelemetry.io/otel"

	"github.com/francisonah/searchsync/internal/observability"
	"github.com/francisonah/searchsync/internal/outbox"
)

const pgUniqueViolationCode = "23505"

const tracerName = "github.com/francisonah/searchsync/internal/orders"

var (
	ErrValidation       = errors.New("validation error")
	ErrDuplicateRequest = errors.New("duplicate request")
)

type Service struct {
	pool       *pgxpool.Pool
	orders     *Repository
	outboxRepo *outbox.Repository
}

func NewService(pool *pgxpool.Pool, orders *Repository, outboxRepo *outbox.Repository) *Service {
	return &Service{pool: pool, orders: orders, outboxRepo: outboxRepo}
}

// CreateOrder inserts the order and its OrderCreated outbox event in one
// PostgreSQL transaction: either both commit or neither does. Kafka is
// never touched here; the Debezium outbox router is the only path from this
// commit to a published event.
//
// eventID is derived deterministically from idempotencyKey (not a fresh
// uuid.New() per call) so that retrying the same logical request with the
// same Idempotency-Key header produces the same outbox event id. A retry
// therefore collides with the outbox primary key and is reported as
// ErrDuplicateRequest instead of silently creating a second event with a
// different id that downstream dedup-by-event-id could not catch.
func (s *Service) CreateOrder(ctx context.Context, req CreateRequest, idempotencyKey string) (Order, error) {
	if err := validateCreate(req, idempotencyKey); err != nil {
		return Order{}, err
	}

	eventID := uuid.NewSHA1(uuid.NameSpaceOID, []byte(idempotencyKey)).String()
	order := Order{
		ID:         uuid.New().String(),
		CustomerID: req.CustomerID,
		Status:     "PENDING",
		TotalCents: req.TotalCents,
		ProductSKU: req.ProductSKU,
		Quantity:   req.Quantity,
	}

	payload, err := json.Marshal(OrderCreated{
		EventID:    eventID,
		OrderID:    order.ID,
		CustomerID: order.CustomerID,
		TotalCents: order.TotalCents,
		ProductSKU: order.ProductSKU,
		Quantity:   order.Quantity,
		Version:    1,
	})
	if err != nil {
		return Order{}, fmt.Errorf("marshal order created event: %w", err)
	}

	ctx, span := otel.Tracer(tracerName).Start(ctx, "orders.CreateOrder db.transaction")
	defer span.End()

	tx, err := s.pool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return Order{}, fmt.Errorf("begin transaction: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck // no-op once committed

	order, err = s.orders.Create(ctx, tx, order)
	if err != nil {
		return Order{}, err
	}

	err = s.outboxRepo.Insert(ctx, tx, outbox.Event{
		ID:            eventID,
		AggregateType: "orders",
		AggregateID:   order.ID,
		Type:          "OrderCreated",
		Payload:       payload,
		TraceParent:   observability.TraceParentFromContext(ctx),
	})
	if err != nil {
		if isUniqueViolation(err) {
			return Order{}, ErrDuplicateRequest
		}
		return Order{}, err
	}

	if err := tx.Commit(ctx); err != nil {
		return Order{}, fmt.Errorf("commit transaction: %w", err)
	}
	return order, nil
}

// GetOrder reads one order by id, for GET /orders/:id.
func (s *Service) GetOrder(ctx context.Context, id string) (Order, error) {
	return s.orders.Get(ctx, s.pool, id)
}

// GetTimeline reads an order's saga transition history, for GET
// /orders/:id/timeline. Returns ErrNotFound if the order itself doesn't
// exist, distinguishing that from "order exists, saga hasn't transitioned
// yet" (an empty slice, nil error).
func (s *Service) GetTimeline(ctx context.Context, id string) ([]Transition, error) {
	if _, err := s.orders.Get(ctx, s.pool, id); err != nil {
		return nil, err
	}
	return s.orders.GetTimeline(ctx, s.pool, id)
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolationCode
}

func validateCreate(req CreateRequest, idempotencyKey string) error {
	if strings.TrimSpace(idempotencyKey) == "" {
		return fmt.Errorf("%w: Idempotency-Key header is required", ErrValidation)
	}
	if _, err := uuid.Parse(req.CustomerID); err != nil {
		return fmt.Errorf("%w: customer_id must be a UUID", ErrValidation)
	}
	if req.TotalCents < 0 {
		return fmt.Errorf("%w: total_cents must be >= 0", ErrValidation)
	}
	if strings.TrimSpace(req.ProductSKU) == "" {
		return fmt.Errorf("%w: product_sku is required", ErrValidation)
	}
	if req.Quantity <= 0 {
		return fmt.Errorf("%w: quantity must be > 0", ErrValidation)
	}
	return nil
}
