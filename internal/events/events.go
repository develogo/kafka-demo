// Package events defines the messages the demo services exchange.
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
// the message self-describing once it leaves Kafka.
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
