package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/develogo/kafka-demo/internal/console"
	"github.com/develogo/kafka-demo/internal/events"
	"github.com/develogo/kafka-demo/internal/kafkax"
	"github.com/jackc/pgx/v5"
)

const projectionSchema = "projection"

// The projection is derived, never authoritative: nothing in the saga reads
// it, and dropping it loses nothing that replaying the topics would not
// rebuild. It exists so the front end has one place to read (docs/adr/0003).
//
// The last two statements are the only cross-schema access in the demo:
// order-service serves the API, so it may read the projection — and nothing
// else of anyone else's, and nothing at all for writing.
const projectionDDL = `
CREATE TABLE IF NOT EXISTS projection.order_status (
    order_id    uuid        PRIMARY KEY,
    customer_id text        NOT NULL,
    items       jsonb       NOT NULL,
    total_cents bigint      NOT NULL,
    status      text        NOT NULL,
    placed_at   timestamptz NOT NULL,
    updated_at  timestamptz NOT NULL DEFAULT now()
);
CREATE TABLE IF NOT EXISTS projection.order_timeline (
    event_id    text        PRIMARY KEY,
    order_id    uuid        NOT NULL,
    event_type  text        NOT NULL,
    detail      text        NOT NULL DEFAULT '',
    occurred_at timestamptz NOT NULL
);
CREATE INDEX IF NOT EXISTS order_timeline_by_order
    ON projection.order_timeline (order_id, occurred_at);
GRANT USAGE ON SCHEMA projection TO orders_service;
GRANT SELECT ON ALL TABLES IN SCHEMA projection TO orders_service;`

// RunProjection maintains the one view of an order's whole lifecycle. Under
// choreography no participant sees more than the events it subscribes to, so
// this service subscribes to all four topics and owns nothing else.
func RunProjection(ctx context.Context, cfg Config) error {
	return participant{
		name:   "projection-service",
		schema: projectionSchema,
		ddl:    projectionDDL,
		topics: events.AllTopics(),
		handle: project,
	}.run(ctx, cfg)
}

func project(ctx context.Context, tx pgx.Tx, r kafkax.Record, log *console.Logger) ([]emission, error) {
	detail, err := timelineDetail(r.Envelope)
	if err != nil {
		return nil, err
	}

	// The event id is the idempotency guard for the whole handler: a replayed
	// event conflicts here and moves nothing.
	tag, err := tx.Exec(ctx, `
        INSERT INTO projection.order_timeline (event_id, order_id, event_type, detail, occurred_at)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (event_id) DO NOTHING`,
		r.Envelope.EventID, r.Envelope.OrderID, string(r.EventType), detail, r.Envelope.OccurredAt)
	if err != nil {
		return nil, fmt.Errorf("record timeline: %w", err)
	}
	if tag.RowsAffected() == 0 {
		log.Skipped(r, "this event is already in the timeline")
		return nil, nil
	}

	status, ok := events.StatusFor(r.EventType)
	if !ok {
		log.Received(r, detail)
		return nil, nil
	}
	if r.EventType == events.OrderCreated {
		return nil, placeInProjection(ctx, tx, r, log, status)
	}
	return nil, advanceInProjection(ctx, tx, r, log, status)
}

// placeInProjection creates the row an order's status lives in.
func placeInProjection(ctx context.Context, tx pgx.Tx, r kafkax.Record, log *console.Logger, status events.Status) error {
	order, err := events.Decode[events.Order](r.Envelope)
	if err != nil {
		return err
	}
	items, err := json.Marshal(order.Items)
	if err != nil {
		return fmt.Errorf("encode items: %w", err)
	}
	if _, err := tx.Exec(ctx, `
        INSERT INTO projection.order_status
            (order_id, customer_id, items, total_cents, status, placed_at)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (order_id) DO NOTHING`,
		r.Envelope.OrderID, order.CustomerID, items, order.TotalCents,
		string(status), r.Envelope.OccurredAt); err != nil {
		return fmt.Errorf("record order status: %w", err)
	}
	log.Received(r, string(status))
	return nil
}

// advanceInProjection moves an order forward, and only forward.
func advanceInProjection(ctx context.Context, tx pgx.Tx, r kafkax.Record, log *console.Logger, status events.Status) error {
	var current events.Status
	err := tx.QueryRow(ctx,
		`SELECT status FROM projection.order_status WHERE order_id = $1 FOR UPDATE`,
		r.Envelope.OrderID).Scan(&current)
	if errors.Is(err, pgx.ErrNoRows) {
		// Kafka orders records within a partition of one topic, not across
		// topics, so in principle this event can arrive before the
		// OrderCreated that opens the order.
		log.Skipped(r, "no order placed under this id yet")
		return nil
	}
	if err != nil {
		return fmt.Errorf("read order status: %w", err)
	}
	if !status.Advances(current) {
		log.Skipped(r, "order is already at "+string(current))
		return nil
	}
	if _, err := tx.Exec(ctx,
		`UPDATE projection.order_status SET status = $2, updated_at = now() WHERE order_id = $1`,
		r.Envelope.OrderID, string(status)); err != nil {
		return fmt.Errorf("advance order status: %w", err)
	}
	log.Received(r, string(current)+" -> "+string(status))
	return nil
}

// timelineDetail is the human-readable half of a timeline entry: the one thing
// worth knowing about an event beyond its type.
func timelineDetail(env events.Envelope) (string, error) {
	switch env.EventType {
	case events.OrderCreated:
		order, err := events.Decode[events.Order](env)
		if err != nil {
			return "", err
		}
		return events.BRL(order.TotalCents), nil
	case events.PaymentApproved, events.PaymentRejected:
		payment, err := events.Decode[events.Payment](env)
		if err != nil {
			return "", err
		}
		if payment.Reason != "" {
			return payment.Reason, nil
		}
		return events.BRL(payment.AmountCents) + " charged", nil
	case events.InventoryReserved:
		reservation, err := events.Decode[events.Reservation](env)
		if err != nil {
			return "", err
		}
		return "reserved at " + reservation.WarehouseID, nil
	case events.ShipmentDispatched:
		shipment, err := events.Decode[events.Shipment](env)
		if err != nil {
			return "", err
		}
		return shipment.Carrier + " " + shipment.TrackingCode, nil
	case events.OrderCompleted, events.OrderCancelled:
		outcome, err := events.Decode[events.Outcome](env)
		if err != nil {
			return "", err
		}
		return outcome.Reason, nil
	default:
		return "", nil
	}
}
