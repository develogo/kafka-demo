package services

import (
	"context"
	"fmt"

	"github.com/develogo/kafka-demo/internal/console"
	"github.com/develogo/kafka-demo/internal/events"
	"github.com/develogo/kafka-demo/internal/kafkax"
	"github.com/jackc/pgx/v5"
)

const notificationsSchema = "notifications"

// The event id is the primary key rather than the order id: an order gets one
// notification per event, and the key is what stops a replayed event from
// sending the same message twice.
const notificationsDDL = `
CREATE TABLE IF NOT EXISTS notifications.notifications (
    event_id text        PRIMARY KEY,
    order_id uuid        NOT NULL,
    channel  text        NOT NULL,
    body     text        NOT NULL,
    sent_at  timestamptz NOT NULL DEFAULT now()
);`

// RunNotification is the fan-out consumer: it subscribes to all four topics
// and reacts to every type on them. It publishes nothing, so it has no outbox.
func RunNotification(ctx context.Context, cfg Config) error {
	return participant{
		name:   "notification-service",
		schema: notificationsSchema,
		ddl:    notificationsDDL,
		topics: events.AllTopics(),
		handle: notifyCustomer,
	}.run(ctx, cfg)
}

func notifyCustomer(ctx context.Context, tx pgx.Tx, r kafkax.Record, log *console.Logger) ([]emission, error) {
	channel, body := customerCopy(r.EventType)
	if channel == "" {
		log.Skipped(r, "no notification for this event type")
		return nil, nil
	}

	tag, err := tx.Exec(ctx, `
        INSERT INTO notifications.notifications (event_id, order_id, channel, body)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (event_id) DO NOTHING`,
		r.Envelope.EventID, r.Envelope.OrderID, channel, body)
	if err != nil {
		return nil, fmt.Errorf("record notification: %w", err)
	}
	if tag.RowsAffected() == 0 {
		log.Skipped(r, "this notification was already sent")
		return nil, nil
	}
	log.Received(r, channel+": "+body)
	return nil, nil
}

// customerCopy is what the customer would be told, and over which channel. An
// empty channel means this event reaches no customer.
func customerCopy(t events.Type) (channel, body string) {
	switch t {
	case events.OrderCreated:
		return "email", "we received your order"
	case events.PaymentApproved:
		return "email", "payment confirmed"
	case events.PaymentRejected:
		return "email", "your card was declined"
	case events.InventoryReserved:
		return "push", "your items are being packed"
	case events.ShipmentDispatched:
		return "sms", "your order is on its way"
	case events.OrderCancelled:
		return "email", "your order was cancelled"
	case events.OrderCompleted:
		return "push", "order complete"
	default:
		return "", ""
	}
}
