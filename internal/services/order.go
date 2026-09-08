package services

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/develogo/kafka-demo/internal/console"
	"github.com/develogo/kafka-demo/internal/events"
	"github.com/develogo/kafka-demo/internal/kafkax"
	"github.com/develogo/kafka-demo/internal/outbox"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const ordersSchema = "orders"

// order-service is the only service whose table carries a status, because it
// is the only one that owns the order. The statuses in between belong to the
// projection: this service never learns that stock was reserved.
const ordersDDL = `
CREATE TABLE IF NOT EXISTS orders.orders (
    id          uuid        PRIMARY KEY,
    customer_id text        NOT NULL,
    items       jsonb       NOT NULL,
    total_cents bigint      NOT NULL,
    status      text        NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    updated_at  timestamptz NOT NULL DEFAULT now()
);`

// catalog is what a standard order is drawn from, one item per order, in turn.
var catalog = []events.Item{
	{SKU: "SKU-HEADSET", Name: "Noise-Cancelling Headset", Quantity: 1, UnitCents: 89900},
	{SKU: "SKU-COFFEE", Name: "Espresso Machine", Quantity: 1, UnitCents: 129900},
	{SKU: "SKU-CHAIR", Name: "Ergonomic Chair", Quantity: 1, UnitCents: 249900},
	{SKU: "SKU-MOUSE", Name: "Wireless Mouse", Quantity: 2, UnitCents: 34900},
	{SKU: "SKU-MONITOR", Name: "4K Monitor", Quantity: 1, UnitCents: 199900},
}

// bigTicket sits above the payment limit, so the order built from it is always
// rejected. It is what the front's second button places: the failure branch is
// the customer's choice rather than something hidden in a counter.
var bigTicket = events.Item{SKU: "SKU-LAPTOP", Name: "Workstation Laptop", Quantity: 3, UnitCents: 1_299_900}

// placed counts the orders this process has taken, so consecutive clicks buy
// different things.
var placed atomic.Int64

// RunOrder serves the API and closes the lifecycle of the orders it took: it
// consumes payments and shipments and writes the terminal OrderCancelled /
// OrderCompleted back onto the orders topic. That is what makes orders carry
// three event types and forces payment-service to route by type rather than by
// topic.
func RunOrder(ctx context.Context, cfg Config) error {
	return participant{
		name:      "order-service",
		schema:    ordersSchema,
		ddl:       ordersDDL + outbox.DDL(ordersSchema),
		topics:    []string{events.TopicPayments, events.TopicShipments},
		publishes: true,
		handle:    closeOrder,
		alongside: func(ctx context.Context, pool *pgxpool.Pool, log *console.Logger) error {
			return serveAPI(ctx, cfg.APIAddr, pool, log)
		},
	}.run(ctx, cfg)
}

func closeOrder(ctx context.Context, tx pgx.Tx, r kafkax.Record, log *console.Logger) ([]emission, error) {
	var terminal events.Type
	var reason string

	switch r.EventType {
	case events.PaymentRejected:
		payment, err := events.Decode[events.Payment](r.Envelope)
		if err != nil {
			return nil, err
		}
		terminal, reason = events.OrderCancelled, payment.Reason
	case events.ShipmentDispatched:
		shipment, err := events.Decode[events.Shipment](r.Envelope)
		if err != nil {
			return nil, err
		}
		terminal, reason = events.OrderCompleted, "handed to "+shipment.Carrier
	default:
		// This service consumes payments and shipments, but an approval is
		// inventory-service's business, not its own.
		log.Skipped(r, "no action for this event type")
		return nil, nil
	}
	if err := pause(ctx, orderLatency); err != nil {
		return nil, nil
	}

	status, _ := events.StatusFor(terminal)
	// One conditional write covers both guards: an order this database never
	// placed does not match, and neither does one that is already closed. That
	// is what makes a replayed terminal event a no-op, and it survives a
	// restart in a way the in-memory guard it replaces did not.
	tag, err := tx.Exec(ctx, `
        UPDATE orders.orders
           SET status = $2, updated_at = now()
         WHERE id = $1 AND status <> $3 AND status <> $4`,
		r.Envelope.OrderID, string(status), string(events.StatusCompleted), string(events.StatusCancelled))
	if err != nil {
		return nil, fmt.Errorf("close order: %w", err)
	}
	if tag.RowsAffected() == 0 {
		log.Skipped(r, "unknown order, or already closed")
		return nil, nil
	}
	log.Received(r, "closing the order")

	env, err := events.New(terminal, r.Envelope.OrderID, events.Outcome{Reason: reason})
	if err != nil {
		return nil, err
	}
	return []emission{{topic: events.TopicOrders, env: env, detail: reason}}, nil
}

// buildOrder is the mocked basket behind a button. A standard order walks the
// catalog; an expensive one is always the same laptop.
func buildOrder(expensive bool) events.Order {
	n := placed.Add(1) - 1
	items := []events.Item{catalog[n%int64(len(catalog))]}
	if expensive {
		items = []events.Item{bigTicket}
	}
	return events.Order{
		CustomerID: fmt.Sprintf("CUST-%04d", n%3+1),
		Items:      items,
		TotalCents: events.TotalCents(items),
	}
}
