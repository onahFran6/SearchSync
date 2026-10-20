package saga

import (
	"errors"
	"testing"
)

var allStates = []State{
	StatePending, StateStockReserved, StateConfirmed, StateRejected,
	StateCompensating, StateCancelled, StateNeedsAttention,
}

var allTriggers = []Trigger{
	TriggerInventoryReserved, TriggerInventoryRejected, TriggerPaymentAuthorized,
	TriggerPaymentDeclined, TriggerPaymentTimedOut, TriggerInventoryReleased,
	TriggerInventoryReleaseFailed, TriggerCompensationExhausted,
}

// expectedLegal is the state diagram from docs/04_SAGA_IMPLEMENTATION.md,
// transcribed independently of the transitions map in state.go so this
// test actually checks the implementation against the spec rather than
// against itself.
var expectedLegal = map[State]map[Trigger]State{
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
}

func TestNext_EveryStateTriggerCombination(t *testing.T) {
	for _, from := range allStates {
		for _, trigger := range allTriggers {
			wantTo, legal := expectedLegal[from][trigger]
			gotTo, err := Next(from, trigger)

			if legal {
				if err != nil {
					t.Errorf("Next(%s, %s): expected legal transition to %s, got error: %v", from, trigger, wantTo, err)
					continue
				}
				if gotTo != wantTo {
					t.Errorf("Next(%s, %s) = %s, want %s", from, trigger, gotTo, wantTo)
				}
				continue
			}

			if err == nil {
				t.Errorf("Next(%s, %s) = %s, want ErrIllegalTransition", from, trigger, gotTo)
				continue
			}
			if !errors.Is(err, ErrIllegalTransition) {
				t.Errorf("Next(%s, %s) returned %v, want an error wrapping ErrIllegalTransition", from, trigger, err)
			}
		}
	}
}

func TestIsTerminal(t *testing.T) {
	tests := []struct {
		state State
		want  bool
	}{
		{StatePending, false},
		{StateStockReserved, false},
		{StateCompensating, false},
		{StateConfirmed, true},
		{StateRejected, true},
		{StateCancelled, true},
		{StateNeedsAttention, true},
	}
	for _, tt := range tests {
		if got := IsTerminal(tt.state); got != tt.want {
			t.Errorf("IsTerminal(%s) = %v, want %v", tt.state, got, tt.want)
		}
	}
}
