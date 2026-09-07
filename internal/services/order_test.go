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

func TestOpenOrdersOnlyClosesOrdersThisRunPlaced(t *testing.T) {
	l := &openOrders{ids: map[string]struct{}{}, total: 2}
	l.track("ORD-0001")
	l.track("ORD-0002")

	if l.owns("ORD-0009") {
		t.Fatal("claimed an order it never placed")
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

// The bug this pins: order numbers restart at 1 every run, so if the id did
// not name the run, a terminal event replayed from an earlier run would be
// claimed by the current one and close the wrong order.
func TestOrderIDsOfDifferentRunsDoNotCollide(t *testing.T) {
	const earlier, current = "1a2b", "3c4d"

	if orderID(earlier, 0) == orderID(current, 0) {
		t.Fatalf("two runs produced the same id: %s", orderID(earlier, 0))
	}

	placed := &openOrders{ids: map[string]struct{}{}, total: 1}
	placed.track(orderID(current, 0))

	if placed.owns(orderID(earlier, 0)) {
		t.Fatal("claimed an order placed by an earlier run")
	}
	if placed.settle(orderID(earlier, 0)) {
		t.Fatal("a replay from an earlier run ended the current run")
	}
	if !placed.settle(orderID(current, 0)) {
		t.Fatal("the run's own order did not finish it")
	}
}

func TestRunIDHasAFixedWidthSoColumnsLineUp(t *testing.T) {
	if got := newRunID(); len(got) != 4 {
		t.Fatalf("newRunID() = %q, want 4 characters", got)
	}
}
