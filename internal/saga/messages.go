package saga

// Commands flow coordinator -> Inventory/Payments. Events flow
// Inventory/Payments -> coordinator, and the coordinator's own
// SagaTransitioned flows coordinator -> Orders. See the package doc
// comment in state.go for why every one of these is defined here instead
// of split across internal/inventory and internal/payments.

// ReserveInventory asks Inventory to reserve stock for one order's single
// SKU. Quantity is always > 0 (validated at the orders API boundary).
type ReserveInventory struct {
	Envelope
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

// ReleaseInventory asks Inventory to undo a reservation during
// compensation. Reason is diagnostic only (e.g. "payment_declined",
// "payment_timed_out") - Inventory's release logic does not branch on it.
type ReleaseInventory struct {
	Envelope
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
	Reason   string `json:"reason"`
}

// InventoryReserved confirms Inventory committed a reservation row and
// decremented available stock.
type InventoryReserved struct {
	Envelope
}

// InventoryRejected means Inventory's conditional stock update affected
// zero rows (available < quantity) - the SKU is out of stock for this
// order's requested quantity. No reservation row was created; no stock was
// touched.
type InventoryRejected struct {
	Envelope
	Reason string `json:"reason"`
}

// InventoryReleased confirms a previously RESERVED row was marked RELEASED
// and the quantity was credited back to available stock.
type InventoryReleased struct {
	Envelope
}

// InventoryReleaseFailed is Inventory's explicit "this will not succeed"
// signal (as opposed to silence, which the coordinator's deadline poller
// detects instead) - e.g. no reservation row exists at all for this order,
// which should never happen in a correct run and is reported rather than
// silently retried forever.
type InventoryReleaseFailed struct {
	Envelope
	Reason string `json:"reason"`
}

// AuthorizePayment asks the payments simulator to decide an outcome for
// one order. AmountCents is carried so Payments never needs to query
// Orders to know what it is authorizing.
type AuthorizePayment struct {
	Envelope
	AmountCents int64 `json:"amountCents"`
}

// PaymentAuthorized and PaymentDeclined are Payments' two possible
// responses. There is deliberately no PaymentTimedOut event: the
// simulator's "timeout" mode produces neither of these (see
// internal/payments/handler.go) - from the coordinator's point of view a
// timed-out payment looks like silence, detected by the deadline poller
// (poller.go), not by a third event type.
type PaymentAuthorized struct {
	Envelope
}

type PaymentDeclined struct {
	Envelope
	Reason string `json:"reason"`
}

// SagaTransitioned is the coordinator's own notification, published every
// time saga.sagas.state changes, so Orders can mirror the change onto its
// own orders.status and append to its own timeline projection without ever
// querying the saga schema directly. FromState/ToState/Reason make the
// transition human-readable in internal/orders' timeline API without it
// needing to know this package's Trigger vocabulary.
type SagaTransitioned struct {
	Envelope
	FromState State  `json:"fromState"`
	ToState   State  `json:"toState"`
	Reason    string `json:"reason"`
}
