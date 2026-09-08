package services

import (
	"testing"

	"github.com/develogo/kafka-demo/internal/events"
)

// The front has two buttons, and which branch of the saga each one takes has
// to be certain: that is the whole reason the expensive one exists.
func TestEachButtonAlwaysTakesItsOwnBranch(t *testing.T) {
	for i := 0; i < len(catalog)*2; i++ {
		standard := buildOrder(false)
		if outcome, _ := decidePayment(standard.TotalCents, DefaultRejectAboveCents); outcome != events.PaymentApproved {
			t.Fatalf("standard order %+v was rejected", standard)
		}
		expensive := buildOrder(true)
		if outcome, _ := decidePayment(expensive.TotalCents, DefaultRejectAboveCents); outcome != events.PaymentRejected {
			t.Fatalf("expensive order %+v was approved", expensive)
		}
	}
}

func TestOrderTotalAgreesWithItsItems(t *testing.T) {
	for _, expensive := range []bool{false, true} {
		order := buildOrder(expensive)
		if got, want := order.TotalCents, events.TotalCents(order.Items); got != want {
			t.Fatalf("total %d disagrees with items (%d)", got, want)
		}
	}
}

// Consecutive clicks should buy different things, or the demo shows the same
// order over and over.
func TestStandardOrdersWalkTheCatalog(t *testing.T) {
	seen := map[string]bool{}
	for i := 0; i < len(catalog); i++ {
		seen[buildOrder(false).Items[0].SKU] = true
	}
	if len(seen) != len(catalog) {
		t.Fatalf("saw %d distinct products over %d orders, want %d", len(seen), len(catalog), len(catalog))
	}
}

// The tracking code is derived from the order id rather than generated, so a
// replayed InventoryReserved cannot produce a second code for one shipment.
func TestTrackingCodeIsStableForAnOrder(t *testing.T) {
	id := events.NewOrderID()
	if trackingCode(id) != trackingCode(id) {
		t.Fatal("tracking code changed between calls")
	}
	if trackingCode(id) == trackingCode(events.NewOrderID()) {
		t.Fatal("two orders share a tracking code")
	}
}
