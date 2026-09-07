package services

import (
	"context"
	"strings"

	"github.com/develogo/kafka-demo/internal/events"
	"github.com/develogo/kafka-demo/internal/kafkax"
)

const carrier = "Correios"

// RunShipping dispatches reserved orders.
func RunShipping(ctx context.Context, cfg Config) error {
	const name = "shipping-service"
	log := cfg.Printer.For(name)

	producer, err := kafkax.NewProducer(cfg.Brokers)
	if err != nil {
		return err
	}
	defer producer.Close()

	consumer, err := kafkax.NewConsumer(cfg.Brokers, name, events.TopicInventory)
	if err != nil {
		return err
	}
	defer consumer.Close()

	return consumer.Run(ctx, func(ctx context.Context, r kafkax.Record) error {
		if r.EventType != events.InventoryReserved {
			log.Skipped(r, "no action for this event type")
			return nil
		}
		log.Received(r, "booking a carrier")

		if err := pause(ctx, shippingLatency); err != nil {
			return nil
		}
		tracking := trackingCode(r.Envelope.OrderID)
		env, err := events.New(events.ShipmentDispatched, r.Envelope.OrderID, events.Shipment{
			Carrier:      carrier,
			TrackingCode: tracking,
		})
		if err != nil {
			return err
		}
		published, err := producer.Publish(ctx, events.TopicShipments, env)
		if err != nil {
			return err
		}
		log.Published(published, tracking)
		return nil
	})
}

func trackingCode(orderID string) string {
	return "TRK-BR" + strings.TrimPrefix(orderID, "ORD-")
}
