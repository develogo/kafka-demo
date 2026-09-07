package services

import (
	"context"

	"github.com/develogo/kafka-demo/internal/events"
	"github.com/develogo/kafka-demo/internal/kafkax"
)

// RunNotification is the fan-out consumer: it subscribes to all four topics
// and skips nothing, so its lines are the full timeline of every order.
func RunNotification(ctx context.Context, cfg Config) error {
	const name = "notification-service"
	log := cfg.Printer.For(name)

	consumer, err := kafkax.NewConsumer(cfg.Brokers, name, events.AllTopics()...)
	if err != nil {
		return err
	}
	defer consumer.Close()

	return consumer.Run(ctx, func(ctx context.Context, r kafkax.Record) error {
		log.Received(r, customerCopy(r.EventType))
		return nil
	})
}

func customerCopy(t events.Type) string {
	switch t {
	case events.OrderCreated:
		return "email: we received your order"
	case events.PaymentApproved:
		return "email: payment confirmed"
	case events.PaymentRejected:
		return "email: your card was declined"
	case events.InventoryReserved:
		return "push: your items are being packed"
	case events.ShipmentDispatched:
		return "sms: your order is on its way"
	case events.OrderCancelled:
		return "email: your order was cancelled"
	case events.OrderCompleted:
		return "push: order complete"
	default:
		return "no notification for this event"
	}
}
