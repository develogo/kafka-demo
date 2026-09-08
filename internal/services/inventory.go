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

const inventorySchema = "inventory"

const inventoryDDL = `
CREATE TABLE IF NOT EXISTS inventory.reservations (
    order_id     uuid        PRIMARY KEY,
    warehouse_id text        NOT NULL,
    reserved_at  timestamptz NOT NULL DEFAULT now()
);`

const warehouseID = "WH-SP-01"

// RunInventory reserves stock once a payment clears. It shares the payments
// topic with order-service: the same PaymentApproved record is delivered to
// both groups, each at its own offset.
func RunInventory(ctx context.Context, cfg Config) error {
	return participant{
		name:      "inventory-service",
		schema:    inventorySchema,
		ddl:       inventoryDDL + outbox.DDL(inventorySchema),
		topics:    []string{events.TopicPayments},
		publishes: true,
		handle:    reserveStock,
	}.run(ctx, cfg)
}

func reserveStock(ctx context.Context, tx pgx.Tx, r kafkax.Record, log *console.Logger) ([]emission, error) {
	// A rejected payment reserves nothing; order-service handles it.
	if r.EventType != events.PaymentApproved {
		log.Skipped(r, "nothing to reserve for this event type")
		return nil, nil
	}
	if err := pause(ctx, inventoryLatency); err != nil {
		return nil, nil
	}

	tag, err := tx.Exec(ctx, `
        INSERT INTO inventory.reservations (order_id, warehouse_id)
        VALUES ($1, $2)
        ON CONFLICT (order_id) DO NOTHING`,
		r.Envelope.OrderID, warehouseID)
	if err != nil {
		return nil, fmt.Errorf("record reservation: %w", err)
	}
	if tag.RowsAffected() == 0 {
		log.Skipped(r, "stock is already reserved for this order")
		return nil, nil
	}
	log.Received(r, "reserving stock")

	env, err := events.New(events.InventoryReserved, r.Envelope.OrderID, events.Reservation{
		WarehouseID: warehouseID,
	})
	if err != nil {
		return nil, err
	}
	return []emission{{topic: events.TopicInventory, env: env, detail: "reserved at " + warehouseID}}, nil
}
