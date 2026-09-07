package services

import (
	"testing"

	"github.com/develogo/kafka-demo/internal/events"
)

func TestDecidePaymentApprovesUpToTheLimitAndRejectsAbove(t *testing.T) {
	const limit int64 = 500_000

	for _, tc := range []struct {
		name  string
		total int64
		want  events.Type
	}{
		{"well under the limit", 89_900, events.PaymentApproved},
		{"exactly at the limit", limit, events.PaymentApproved},
		{"one cent over the limit", limit + 1, events.PaymentRejected},
		{"far over the limit", 3_899_700, events.PaymentRejected},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, reason := decidePayment(tc.total, limit)
			if got != tc.want {
				t.Fatalf("decidePayment(%d, %d) = %s, want %s", tc.total, limit, got, tc.want)
			}
			// A rejection has to say why: the reason is what order-service
			// puts on OrderCancelled.
			if (reason != "") != (tc.want == events.PaymentRejected) {
				t.Fatalf("reason = %q for %s", reason, got)
			}
		})
	}
}
