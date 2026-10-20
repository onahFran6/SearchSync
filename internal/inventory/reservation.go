package inventory

// ReservationStatus mirrors inventory.reservations.status's CHECK
// constraint (migrations/0005_create_inventory_schema.sql).
type ReservationStatus string

const (
	ReservationReserved ReservationStatus = "RESERVED"
	ReservationReleased ReservationStatus = "RELEASED"
)

// Reservation is one row of inventory.reservations. OrderID is the
// primary key: there is at most one reservation per order, ever, which is
// what makes a duplicate ReserveInventory command idempotent (see
// service.go).
type Reservation struct {
	OrderID  string
	SKU      string
	Quantity int
	Status   ReservationStatus
}
