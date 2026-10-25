package orders

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Transition is one row of the saga_transitions projection
// (migrations/0008_extend_orders_for_saga.sql), built by SagaHandler from
// saga.SagaTransitioned events - the same "consume an event, write a
// local projection" pattern used earlier for order_notifications, applied
// here to the saga's own transition history instead of the order's
// creation.
type Transition struct {
	OrderID        string    `json:"order_id"`
	FromState      string    `json:"from_state"`
	ToState        string    `json:"to_state"`
	Reason         string    `json:"reason"`
	EventVersion   int       `json:"event_version"`
	TransitionedAt time.Time `json:"transitioned_at"`
}

// GetTimeline returns every transition recorded for an order, oldest
// first - what GET /orders/:id/timeline exposes. An empty, non-error
// result means the order exists but its saga hasn't reached
// STOCK_RESERVED yet (PENDING is the only state with no transition *into*
// it), not that the order doesn't exist; callers needing to distinguish
// those should call Repository.Get first.
func (r *Repository) GetTimeline(ctx context.Context, pool *pgxpool.Pool, orderID string) ([]Transition, error) {
	const query = `
		SELECT order_id, from_state, to_state, reason, event_version, transitioned_at
		FROM saga_transitions WHERE order_id = $1 ORDER BY event_version ASC`

	rows, err := pool.Query(ctx, query, orderID)
	if err != nil {
		return nil, fmt.Errorf("query timeline: %w", err)
	}
	defer rows.Close()

	var out []Transition
	for rows.Next() {
		var t Transition
		if err := rows.Scan(&t.OrderID, &t.FromState, &t.ToState, &t.Reason, &t.EventVersion, &t.TransitionedAt); err != nil {
			return nil, fmt.Errorf("scan transition: %w", err)
		}
		out = append(out, t)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate transitions: %w", err)
	}
	return out, nil
}
