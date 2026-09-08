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

const shipmentsSchema = "shipments"

const shipmentsDDL = `
CREATE TABLE IF NOT EXISTS shipments.shipments (
    order_id      uuid        PRIMARY KEY,
    carrier       text        NOT NULL,
    tracking_code text        NOT NULL,
    dispatched_at timestamptz NOT NULL DEFAULT now()
);`

const carrier = "Correios"

// RunShipping dispatches reserved orders.
func RunShipping(ctx context.Context, cfg Config) error {
	return participant{
		name:      "shipping-service",
		schema:    shipmentsSchema,
		ddl:       shipmentsDDL + outbox.DDL(shipmentsSchema),
		topics:    []string{events.TopicInventory},
		publishes: true,
		handle:    dispatchShipment,
	}.run(ctx, cfg)
}

func dispatchShipment(ctx context.Context, tx pgx.Tx, r kafkax.Record, log *console.Logger) ([]emission, error) {
	if r.EventType != events.InventoryReserved {
		log.Skipped(r, "no action for this event type")
		return nil, nil
	}
	if err := pause(ctx, shippingLatency); err != nil {
		return nil, nil
	}

	tracking := trackingCode(r.Envelope.OrderID)
	tag, err := tx.Exec(ctx, `
        INSERT INTO shipments.shipments (order_id, carrier, tracking_code)
        VALUES ($1, $2, $3)
        ON CONFLICT (order_id) DO NOTHING`,
		r.Envelope.OrderID, carrier, tracking)
	if err != nil {
		return nil, fmt.Errorf("record shipment: %w", err)
	}
	if tag.RowsAffected() == 0 {
		log.Skipped(r, "this order was already dispatched")
		return nil, nil
	}
	log.Received(r, "booking a carrier")

	env, err := events.New(events.ShipmentDispatched, r.Envelope.OrderID, events.Shipment{
		Carrier:      carrier,
		TrackingCode: tracking,
	})
	if err != nil {
		return nil, err
	}
	return []emission{{topic: events.TopicShipments, env: env, detail: tracking}}, nil
}

// trackingCode is derived from the order id so it is stable across a replay:
// the same order always gets the same code.
func trackingCode(orderID string) string {
	return "TRK-BR" + events.ShortID(orderID)
}
