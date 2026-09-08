package services

import (
	"context"
	"fmt"

	"github.com/develogo/kafka-demo/internal/console"
	"github.com/develogo/kafka-demo/internal/events"
	"github.com/develogo/kafka-demo/internal/kafkax"
	"github.com/develogo/kafka-demo/internal/outbox"
	"github.com/jackc/pgx/v5"
)

const paymentSchema = "payments"

// One row per order, so the primary key is the idempotency guard: a replayed
// OrderCreated cannot charge the same order twice.
const paymentDDL = `
CREATE TABLE IF NOT EXISTS payments.payments (
    order_id     uuid        PRIMARY KEY,
    method       text        NOT NULL,
    amount_cents bigint      NOT NULL,
    outcome      text        NOT NULL,
    reason       text        NOT NULL DEFAULT '',
    decided_at   timestamptz NOT NULL DEFAULT now()
);`

const paymentMethod = "credit-card"

// RunPayment charges orders. It subscribes to the orders topic, which also
// carries the two terminal event types it has no business reacting to.
func RunPayment(ctx context.Context, cfg Config) error {
	return participant{
		name:      "payment-service",
		schema:    paymentSchema,
		ddl:       paymentDDL + outbox.DDL(paymentSchema),
		topics:    []string{events.TopicOrders},
		publishes: true,
		handle: func(ctx context.Context, tx pgx.Tx, r kafkax.Record, log *console.Logger) ([]emission, error) {
			return decideOnOrder(ctx, tx, r, log, cfg.RejectAboveCents)
		},
	}.run(ctx, cfg)
}

func decideOnOrder(ctx context.Context, tx pgx.Tx, r kafkax.Record, log *console.Logger, limitCents int64) ([]emission, error) {
	if r.EventType != events.OrderCreated {
		log.Skipped(r, "no action for this event type")
		return nil, nil
	}
	order, err := events.Decode[events.Order](r.Envelope)
	if err != nil {
		return nil, err
	}
	if err := pause(ctx, paymentLatency); err != nil {
		return nil, nil
	}

	outcome, reason := decidePayment(order.TotalCents, limitCents)
	tag, err := tx.Exec(ctx, `
        INSERT INTO payments.payments (order_id, method, amount_cents, outcome, reason)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (order_id) DO NOTHING`,
		r.Envelope.OrderID, paymentMethod, order.TotalCents, string(outcome), reason)
	if err != nil {
		return nil, fmt.Errorf("record payment: %w", err)
	}
	if tag.RowsAffected() == 0 {
		log.Skipped(r, "this order was already charged")
		return nil, nil
	}
	log.Received(r, events.BRL(order.TotalCents))

	env, err := events.New(outcome, r.Envelope.OrderID, events.Payment{
		Method:      paymentMethod,
		AmountCents: order.TotalCents,
		Reason:      reason,
	})
	if err != nil {
		return nil, err
	}
	detail := reason
	if detail == "" {
		detail = events.BRL(order.TotalCents) + " charged"
	}
	return []emission{{topic: events.TopicPayments, env: env, detail: detail}}, nil
}

// decidePayment is the only business rule in the demo, and it is deterministic
// on purpose: an order above the limit is always rejected, which is what the
// front's second button relies on.
func decidePayment(totalCents, limitCents int64) (events.Type, string) {
	if totalCents > limitCents {
		return events.PaymentRejected, fmt.Sprintf("%s is above the %s limit",
			events.BRL(totalCents), events.BRL(limitCents))
	}
	return events.PaymentApproved, ""
}
