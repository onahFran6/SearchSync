// Package saga models the order-fulfilment saga's state machine and the
// wire contracts (commands and events) that drive it. It is the single
// owner of every message shape in the protocol - both the commands the
// coordinator issues to Inventory/Payments and the events they report back
// - deliberately, not by oversight: Inventory and Payments each need the
// coordinator's command types to decode what they receive, and the
// coordinator needs their event types to decode what it receives, which
// means whichever side does NOT own a given type must import the side that
// does. Splitting ownership down the middle (each service owns its own
// outgoing type) would make saga depend on inventory/payments and
// inventory/payments depend on saga at the same time - an import cycle.
// Concentrating ownership here, in the one package that already has to
// understand the whole protocol to drive the state machine, avoids that
// without duplicating any message shape.
//
// The one type this package does NOT own is OrderCreated - that is, and
// stays, internal/orders' type (it predates the saga and orders remains
// its sole producer). Importing it here would reintroduce the same cycle
// from the other side, because internal/orders needs to import this
// package for SagaTransitioned. See consumer.go for the deliberately
// duplicated, minimal decode-only shape this package uses instead.
package saga

import "fmt"

// State is the saga's persisted lifecycle state (saga.sagas.state). Every
// value here is a legal value of that column's CHECK constraint
// (migrations/0004_create_saga_schema.sql) - the two must be kept in sync.
type State string

const (
	StatePending        State = "PENDING"
	StateStockReserved  State = "STOCK_RESERVED"
	StateConfirmed      State = "CONFIRMED"
	StateRejected       State = "REJECTED"
	StateCompensating   State = "COMPENSATING"
	StateCancelled      State = "CANCELLED"
	StateNeedsAttention State = "NEEDS_ATTENTION"
)

// Trigger identifies what caused a transition attempt. Most come from a
// Kafka event the coordinator consumed; PaymentTimedOut and
// CompensationExhausted come from the persisted-deadline poller
// (poller.go) instead - there is no Kafka message behind them, which is
// exactly the point: a timeout is detected from Postgres state, never from
// an in-memory timer.
type Trigger string

const (
	TriggerInventoryReserved      Trigger = "InventoryReserved"
	TriggerInventoryRejected      Trigger = "InventoryRejected"
	TriggerPaymentAuthorized      Trigger = "PaymentAuthorized"
	TriggerPaymentDeclined        Trigger = "PaymentDeclined"
	TriggerPaymentTimedOut        Trigger = "PaymentTimedOut"
	TriggerInventoryReleased      Trigger = "InventoryReleased"
	TriggerInventoryReleaseFailed Trigger = "InventoryReleaseFailed"
	TriggerCompensationExhausted  Trigger = "CompensationExhausted"
)

// ErrIllegalTransition means the trigger does not apply to the state the
// saga is currently in - either a genuinely invalid sequence, or (far more
// commonly in practice) a duplicate/stale message arriving after the saga
// already moved on. Callers must treat the two the same way: do not
// transition, do not error the whole handler, just leave the persisted
// state as it is. See repository.go's guarded UPDATE, which turns this
// into "zero rows affected" rather than a Go-level error in the hot path.
var ErrIllegalTransition = fmt.Errorf("saga: illegal transition")

// transitions is the only place "what can happen next" is decided. Every
// legal (state, trigger) pair the implementation doc's state diagram
// describes appears here exactly once; anything else is illegal by
// omission, not by an exhaustive negative list.
var transitions = map[State]map[Trigger]State{
	StatePending: {
		TriggerInventoryReserved: StateStockReserved,
		TriggerInventoryRejected: StateRejected,
	},
	StateStockReserved: {
		TriggerPaymentAuthorized: StateConfirmed,
		TriggerPaymentDeclined:   StateCompensating,
		TriggerPaymentTimedOut:   StateCompensating,
	},
	StateCompensating: {
		TriggerInventoryReleased:      StateCancelled,
		TriggerInventoryReleaseFailed: StateNeedsAttention,
		TriggerCompensationExhausted:  StateNeedsAttention,
	},
	// StateConfirmed, StateRejected, StateCancelled, StateNeedsAttention
	// are terminal: no entry means no trigger ever transitions out of them.
}

// Next returns the state `from` moves to when `trigger` fires, or
// ErrIllegalTransition if that trigger does not apply in that state.
func Next(from State, trigger Trigger) (State, error) {
	byTrigger, ok := transitions[from]
	if !ok {
		return "", fmt.Errorf("%w: %s has no legal transitions (terminal state)", ErrIllegalTransition, from)
	}
	to, ok := byTrigger[trigger]
	if !ok {
		return "", fmt.Errorf("%w: %s does not apply in state %s", ErrIllegalTransition, trigger, from)
	}
	return to, nil
}

// IsTerminal reports whether a saga in this state will never transition
// again through the ordinary event-driven path. NEEDS_ATTENTION is
// terminal in this sense - nothing in this codebase automatically resolves
// it - even though it represents a failure needing a human, not a success.
func IsTerminal(s State) bool {
	_, ok := transitions[s]
	return !ok
}
