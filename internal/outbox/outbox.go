// Package outbox implements the transactional outbox (see docs/adr/0004).
//
// No service publishes to Kafka. A service writes its state change and the
// events it caused to its own outbox in one transaction, and the relay of that
// service publishes them afterwards. State and events can no longer disagree
// about what happened.
package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/develogo/kafka-demo/internal/console"
	"github.com/develogo/kafka-demo/internal/events"
	"github.com/develogo/kafka-demo/internal/kafkax"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// How often a relay looks for unpublished rows, and how many it takes at once.
const (
	pollInterval = 100 * time.Millisecond
	batchSize    = 32
)

// DDL is the outbox table of one schema. Rows are kept after publishing rather
// than deleted, so the table stays a readable record of what a service decided
// and when the event left for Kafka.
//
// schema is always one of this repo's own constants, never input.
func DDL(schema string) string {
	return fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %[1]s.outbox (
    id           bigserial   PRIMARY KEY,
    topic        text        NOT NULL,
    event_id     text        NOT NULL UNIQUE,
    event_type   text        NOT NULL,
    order_id     uuid        NOT NULL,
    envelope     jsonb       NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);
CREATE INDEX IF NOT EXISTS outbox_unpublished ON %[1]s.outbox (id) WHERE published_at IS NULL;
`, schema)
}

// Insert adds an event to the outbox of schema, inside the caller's
// transaction. The unique event id makes a retried transaction a no-op.
func Insert(ctx context.Context, tx pgx.Tx, schema, topic string, env events.Envelope) error {
	raw, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("encode %s for outbox: %w", env.EventType, err)
	}
	_, err = tx.Exec(ctx, fmt.Sprintf(`
        INSERT INTO %s.outbox (topic, event_id, event_type, order_id, envelope)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (event_id) DO NOTHING`, schema),
		topic, env.EventID, string(env.EventType), env.OrderID, raw)
	if err != nil {
		return fmt.Errorf("insert %s into %s.outbox: %w", env.EventType, schema, err)
	}
	return nil
}

// Relay publishes one service's outbox to Kafka, oldest row first.
type Relay struct {
	pool     *pgxpool.Pool
	schema   string
	producer *kafkax.Producer
	log      *console.Logger
}

func NewRelay(pool *pgxpool.Pool, schema string, producer *kafkax.Producer, log *console.Logger) *Relay {
	return &Relay{pool: pool, schema: schema, producer: producer, log: log}
}

// Run drains the outbox until ctx is cancelled.
func (r *Relay) Run(ctx context.Context) error {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		// Keep draining while there is anything to drain, so a burst does not
		// trickle out one batch per tick.
		for {
			n, err := r.drain(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return err
			}
			if n < batchSize {
				break
			}
		}
		select {
		case <-ticker.C:
		case <-ctx.Done():
			return nil
		}
	}
}

// drain publishes one batch and reports how many rows it handled.
//
// The rows stay locked while they are published, which is what keeps two
// relays of the same service from publishing the same event and what keeps
// them in order. SKIP LOCKED means the second relay takes the next batch
// instead of waiting.
func (r *Relay) drain(ctx context.Context) (int, error) {
	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return 0, fmt.Errorf("relay %s: begin: %w", r.schema, err)
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, fmt.Sprintf(`
        SELECT id, topic, envelope
          FROM %s.outbox
         WHERE published_at IS NULL
         ORDER BY id
         LIMIT %d
           FOR UPDATE SKIP LOCKED`, r.schema, batchSize))
	if err != nil {
		return 0, fmt.Errorf("relay %s: select: %w", r.schema, err)
	}

	type pending struct {
		id  int64
		top string
		env events.Envelope
	}
	var batch []pending
	for rows.Next() {
		var p pending
		var raw []byte
		if err := rows.Scan(&p.id, &p.top, &raw); err != nil {
			rows.Close()
			return 0, fmt.Errorf("relay %s: scan: %w", r.schema, err)
		}
		if err := json.Unmarshal(raw, &p.env); err != nil {
			rows.Close()
			return 0, fmt.Errorf("relay %s: decode outbox row %d: %w", r.schema, p.id, err)
		}
		batch = append(batch, p)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("relay %s: read rows: %w", r.schema, err)
	}
	if len(batch) == 0 {
		return 0, nil
	}

	for _, p := range batch {
		// A relay that dies between this ack and the commit below republishes
		// the row on restart: at-least-once, which is why every consumer
		// guards for duplicates.
		published, err := r.producer.Publish(ctx, p.top, p.env)
		if err != nil {
			return 0, err
		}
		if _, err := tx.Exec(ctx,
			fmt.Sprintf(`UPDATE %s.outbox SET published_at = now() WHERE id = $1`, r.schema),
			p.id); err != nil {
			return 0, fmt.Errorf("relay %s: mark row %d published: %w", r.schema, p.id, err)
		}
		r.log.Published(published, "")
	}
	if err := tx.Commit(ctx); err != nil {
		return 0, fmt.Errorf("relay %s: commit: %w", r.schema, err)
	}
	return len(batch), nil
}
