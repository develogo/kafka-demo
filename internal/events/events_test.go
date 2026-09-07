package events_test

import (
	"encoding/json"
	"testing"

	"github.com/develogo/kafka-demo/internal/events"
)

func TestNewStampsTheEnvelopeAndKeepsThePayload(t *testing.T) {
	order := events.Order{
		CustomerID: "CUST-0001",
		Items:      []events.Item{{SKU: "SKU-MOUSE", Quantity: 2, UnitCents: 34900}},
		TotalCents: 69800,
	}

	env, err := events.New(events.OrderCreated, "ORD-0001", order)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if env.EventType != events.OrderCreated || env.OrderID != "ORD-0001" {
		t.Fatalf("envelope not stamped: %+v", env)
	}
	if env.EventID == "" || env.OccurredAt.IsZero() {
		t.Fatalf("envelope missing id or timestamp: %+v", env)
	}

	// The envelope survives the round trip through Kafka as bytes.
	raw, err := json.Marshal(env)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var back events.Envelope
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	decoded, err := events.Decode[events.Order](back)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if decoded.CustomerID != order.CustomerID || decoded.TotalCents != order.TotalCents {
		t.Fatalf("payload changed: got %+v want %+v", decoded, order)
	}
	if len(decoded.Items) != 1 || decoded.Items[0] != order.Items[0] {
		t.Fatalf("items changed: got %+v want %+v", decoded.Items, order.Items)
	}
}

func TestDecodeRejectsAPayloadOfTheWrongShape(t *testing.T) {
	env := events.Envelope{EventType: events.PaymentApproved, Data: []byte(`"not an object"`)}
	if _, err := events.Decode[events.Payment](env); err == nil {
		t.Fatal("expected an error decoding a string into Payment")
	}
}

func TestTotalCentsSumsQuantities(t *testing.T) {
	items := []events.Item{
		{Quantity: 2, UnitCents: 34900},
		{Quantity: 1, UnitCents: 129900},
	}
	if got, want := events.TotalCents(items), int64(199700); got != want {
		t.Fatalf("TotalCents = %d, want %d", got, want)
	}
}
