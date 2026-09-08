// Package events defines the events the demo services exchange.
//
// Topics are per aggregate (see docs/adr/0001), so a single topic carries
// several event types and every consumer has to route on the event type
// instead of relying on the topic name.
package events

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Type identifies what happened. It is the routing key of the whole demo.
type Type string

const (
	OrderCreated   Type = "OrderCreated"
	OrderCancelled Type = "OrderCancelled"
	OrderCompleted Type = "OrderCompleted"

	PaymentApproved Type = "PaymentApproved"
	PaymentRejected Type = "PaymentRejected"

	InventoryReserved Type = "InventoryReserved"

	ShipmentDispatched Type = "ShipmentDispatched"
)

// Aggregate topics. One per aggregate, not one per event type.
const (
	TopicOrders    = "orders"
	TopicPayments  = "payments"
	TopicInventory = "inventory"
	TopicShipments = "shipments"
)

// AllTopics is what the notification service subscribes to and what the demo
// creates on start-up.
func AllTopics() []string {
	return []string{TopicOrders, TopicPayments, TopicInventory, TopicShipments}
}

// HeaderEventType carries Envelope.EventType as a record header so a consumer
// can decide whether a record is for it without decoding the value. The
// envelope mirrors it on purpose: the header serves routing, the field keeps
// the record self-describing once it leaves Kafka.
const HeaderEventType = "event_type"

// Envelope is the shape of every record value. EventID, OrderID and OccurredAt
// are the same for all event types; Data varies and is decoded lazily by
// whoever cares about that type.
type Envelope struct {
	EventID    string          `json:"event_id"`
	EventType  Type            `json:"event_type"`
	OrderID    string          `json:"order_id"`
	OccurredAt time.Time       `json:"occurred_at"`
	Data       json.RawMessage `json:"data"`
}

// Item is a line of an order. Money is always in cents, never a float.
type Item struct {
	SKU       string `json:"sku"`
	Name      string `json:"name"`
	Quantity  int    `json:"quantity"`
	UnitCents int64  `json:"unit_cents"`
}

// Order is the payload of OrderCreated.
type Order struct {
	CustomerID string `json:"customer_id"`
	Items      []Item `json:"items"`
	TotalCents int64  `json:"total_cents"`
}

// Payment is the payload of PaymentApproved and PaymentRejected. Reason is
// only set when the payment was rejected.
type Payment struct {
	Method      string `json:"method"`
	AmountCents int64  `json:"amount_cents"`
	Reason      string `json:"reason,omitempty"`
}

// Reservation is the payload of InventoryReserved. It does not repeat the
// order lines: a real inventory service looks them up by order id in its own
// store rather than trusting an upstream copy.
type Reservation struct {
	WarehouseID string `json:"warehouse_id"`
}

// Shipment is the payload of ShipmentDispatched.
type Shipment struct {
	Carrier      string `json:"carrier"`
	TrackingCode string `json:"tracking_code"`
}

// Outcome is the payload of the two terminal order events.
type Outcome struct {
	Reason string `json:"reason"`
}

// BRL renders cents as currency. Money is stored and carried as an integer
// number of cents, and only ever becomes a string for a human to read.
func BRL(cents int64) string {
	sign := ""
	if cents < 0 {
		sign, cents = "-", -cents
	}
	return fmt.Sprintf("%sBRL %d.%02d", sign, cents/100, cents%100)
}

// TotalCents sums the lines of an order.
func TotalCents(items []Item) int64 {
	var total int64
	for _, it := range items {
		total += int64(it.Quantity) * it.UnitCents
	}
	return total
}

// New wraps a payload in an envelope stamped with a fresh id and the current
// time.
func New(t Type, orderID string, data any) (Envelope, error) {
	raw, err := json.Marshal(data)
	if err != nil {
		return Envelope{}, fmt.Errorf("encode %s payload: %w", t, err)
	}
	return Envelope{
		EventID:    newEventID(),
		EventType:  t,
		OrderID:    orderID,
		OccurredAt: time.Now().UTC(),
		Data:       raw,
	}, nil
}

// Decode reads the payload of an envelope into the type the caller expects.
func Decode[T any](env Envelope) (T, error) {
	var v T
	if err := json.Unmarshal(env.Data, &v); err != nil {
		return v, fmt.Errorf("decode %s payload: %w", env.EventType, err)
	}
	return v, nil
}

func newEventID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand does not fail on any platform we run on; if it ever
		// does, an event without an id is worse than a crash.
		panic(fmt.Sprintf("events: cannot generate event id: %v", err))
	}
	return "evt_" + hex.EncodeToString(b[:])
}

// Status is where an order sits in its lifecycle. Only the order's owner and
// the projection record it; every other service knows its own aggregate.
type Status string

const (
	StatusPlaced     Status = "PLACED"
	StatusApproved   Status = "APPROVED"
	StatusRejected   Status = "REJECTED"
	StatusReserved   Status = "RESERVED"
	StatusDispatched Status = "DISPATCHED"
	StatusCompleted  Status = "COMPLETED"
	StatusCancelled  Status = "CANCELLED"
)

// statusOf is the status each event puts an order in. Types absent from the
// map move nothing.
var statusOf = map[Type]Status{
	OrderCreated:       StatusPlaced,
	PaymentApproved:    StatusApproved,
	PaymentRejected:    StatusRejected,
	InventoryReserved:  StatusReserved,
	ShipmentDispatched: StatusDispatched,
	OrderCompleted:     StatusCompleted,
	OrderCancelled:     StatusCancelled,
}

// StatusFor reports the status t moves an order to, if any.
func StatusFor(t Type) (Status, bool) {
	s, ok := statusOf[t]
	return s, ok
}

// rank orders the lifecycle. Approved and rejected share a rank because they
// are the two outcomes of the same step, as do the two terminal statuses.
var rank = map[Status]int{
	StatusPlaced: 1, StatusApproved: 2, StatusRejected: 2,
	StatusReserved: 3, StatusDispatched: 4,
	StatusCompleted: 5, StatusCancelled: 5,
}

// Advances reports whether s is a step forward from current. Delivery is
// at-least-once and topics are ordered only per partition, so a projection can
// see a late event for a step it has already passed; this is what keeps it
// from moving an order backwards.
func (s Status) Advances(current Status) bool {
	return rank[s] > rank[current]
}

// Terminal reports whether an order in this status will produce nothing
// further.
func (s Status) Terminal() bool {
	return s == StatusCompleted || s == StatusCancelled
}

// NewOrderID mints the identifier of an order. UUIDv7 is time-ordered, so ids
// sort by when the order was placed, and unique across runs by construction --
// which is why nothing has to label the run that placed an order.
func NewOrderID() string {
	id, err := uuid.NewV7()
	if err != nil {
		// Same reasoning as newEventID: an order without an id is worse than
		// a crash.
		panic(fmt.Sprintf("events: cannot generate order id: %v", err))
	}
	return id.String()
}

// ShortID is the tail of an id, for a terminal line that has to stay narrow.
// The tail and not the head: the head of a UUIDv7 is a timestamp, so two
// orders placed a minute apart share it.
func ShortID(id string) string {
	const n = 12
	if len(id) <= n {
		return id
	}
	return id[len(id)-n:]
}
