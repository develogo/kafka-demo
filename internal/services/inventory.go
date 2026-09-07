package services

import (
	"context"

	"github.com/develogo/kafka-demo/internal/events"
	"github.com/develogo/kafka-demo/internal/kafkax"
)

const warehouseID = "WH-SP-01"

// RunInventory reserves stock once a payment clears. It shares the payments
// topic with order-service: the same PaymentApproved record is delivered to
// both groups, each at its own offset.
func RunInventory(ctx context.Context, cfg Config) error {
	const name = "inventory-service"
	log := cfg.Printer.For(name)

	producer, err := kafkax.NewProducer(cfg.Brokers)
	if err != nil {
		return err
	}
	defer producer.Close()

	consumer, err := kafkax.NewConsumer(cfg.Brokers, name, events.TopicPayments)
	if err != nil {
		return err
	}
	defer consumer.Close()

	return consumer.Run(ctx, func(ctx context.Context, r kafkax.Record) error {
		// A rejected payment reserves nothing; order-service handles it.
		if r.EventType != events.PaymentApproved {
			log.Skipped(r)
			return nil
		}
		log.Received(r, "reserving stock")

		if err := pause(ctx, inventoryLatency); err != nil {
			return nil
		}
		env, err := events.New(events.InventoryReserved, r.Envelope.OrderID, events.Reservation{
			WarehouseID: warehouseID,
		})
		if err != nil {
			return err
		}
		published, err := producer.Publish(ctx, events.TopicInventory, env)
		if err != nil {
			return err
		}
		log.Published(published, "reserved at "+warehouseID)
		return nil
	})
}
