package services

import (
	"testing"

	"github.com/develogo/kafka-demo/internal/events"
)

// The demo is only worth watching if it always shows both branches, so pin the
// generator: in the default run of five orders, exactly one is rejected.
func TestDefaultRunProducesExactlyOneRejectedOrder(t *testing.T) {
	var rejected []int
	for i := 0; i < 5; i++ {
		order := buildOrder(i)
		if order.TotalCents != events.TotalCents(order.Items) {
			t.Fatalf("order %d total %d disagrees with its items", i, order.TotalCents)
		}
		if outcome, _ := decidePayment(order.TotalCents, DefaultRejectAboveCents); outcome == events.PaymentRejected {
			rejected = append(rejected, i)
		}
	}
	if len(rejected) != 1 || rejected[0] != bigTicketIndex {
		t.Fatalf("rejected orders = %v, want exactly [%d]", rejected, bigTicketIndex)
	}
}

func TestBuildOrderIsDeterministic(t *testing.T) {
	for i := 0; i < 12; i++ {
		first, second := buildOrder(i), buildOrder(i)
		if first.CustomerID != second.CustomerID || first.TotalCents != second.TotalCents {
			t.Fatalf("order %d changed between calls: %+v vs %+v", i, first, second)
		}
	}
}

func TestLedgerOnlyClosesOrdersThisRunPlaced(t *testing.T) {
	l := &ledger{ids: map[string]struct{}{}, total: 2}
	l.track("ORD-0001")
	l.track("ORD-0002")

	if l.owns("ORD-0009") {
		t.Fatal("ledger claims an order it never placed")
	}
	if l.settle("ORD-0009") {
		t.Fatal("settling a foreign order must not finish the run")
	}
	if l.settle("ORD-0001") {
		t.Fatal("run finished with one order still open")
	}
	if l.settle("ORD-0001") {
		t.Fatal("the same order settled twice")
	}
	if !l.settle("ORD-0002") {
		t.Fatal("run should be finished once both orders are closed")
	}
}
