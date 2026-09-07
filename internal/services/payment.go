package services

import (
	"context"
	"fmt"

	"github.com/develogo/kafka-demo/internal/events"
	"github.com/develogo/kafka-demo/internal/kafkax"
)

// RunPayment charges orders. It subscribes to the orders topic, which also
// carries the two terminal event types it has no business reacting to.
func RunPayment(ctx context.Context, cfg Config) error {
	const name = "payment-service"
	log := cfg.Printer.For(name)

	producer, err := kafkax.NewProducer(cfg.Brokers)
	if err != nil {
		return err
	}
	defer producer.Close()

	consumer, err := kafkax.NewConsumer(cfg.Brokers, name, events.TopicOrders)
	if err != nil {
		return err
	}
	defer consumer.Close()

	return consumer.Run(ctx, func(ctx context.Context, r kafkax.Record) error {
		if r.EventType != events.OrderCreated {
			log.Skipped(r, "no action for this event type")
			return nil
		}
		order, err := events.Decode[events.Order](r.Envelope)
		if err != nil {
			return err
		}
		log.Received(r, events.BRL(order.TotalCents))

		if err := pause(ctx, paymentLatency); err != nil {
			return nil
		}
		outcome, reason := decidePayment(order.TotalCents, cfg.RejectAboveCents)
		env, err := events.New(outcome, r.Envelope.OrderID, events.Payment{
			Method:      "credit-card",
			AmountCents: order.TotalCents,
			Reason:      reason,
		})
		if err != nil {
			return err
		}
		published, err := producer.Publish(ctx, events.TopicPayments, env)
		if err != nil {
			return err
		}
		detail := reason
		if detail == "" {
			detail = events.BRL(order.TotalCents) + " charged"
		}
		log.Published(published, detail)
		return nil
	})
}

// decidePayment is the only business rule in the demo, and it is deterministic
// on purpose: an order above the limit is always rejected, so every run shows
// both the happy path and the failure branch.
func decidePayment(totalCents, limitCents int64) (events.Type, string) {
	if totalCents > limitCents {
		return events.PaymentRejected, fmt.Sprintf("%s is above the %s limit",
			events.BRL(totalCents), events.BRL(limitCents))
	}
	return events.PaymentApproved, ""
}
