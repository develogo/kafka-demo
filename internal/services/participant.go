package services

import (
	"context"
	"sync"

	"github.com/develogo/kafka-demo/internal/console"
	"github.com/develogo/kafka-demo/internal/db"
	"github.com/develogo/kafka-demo/internal/events"
	"github.com/develogo/kafka-demo/internal/kafkax"
	"github.com/develogo/kafka-demo/internal/outbox"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// emission is an event a handler decided to publish. It goes to the outbox in
// the same transaction as the state change that caused it; the relay takes it
// from there.
type emission struct {
	topic  string
	env    events.Envelope
	detail string
}

// participant is the shape every service of the saga shares: a Postgres schema
// it alone writes, a consumer group, and — for the ones that publish — an
// outbox relay.
type participant struct {
	name   string
	schema string // also the role it connects as, with an underscore
	ddl    string
	topics []string

	// publishes is false for the two services that only read the saga:
	// notification and projection have no outbox and no relay.
	publishes bool

	// handle acts on one record inside a transaction and returns the events
	// that follow from it. Everything it writes commits with those events or
	// with neither.
	handle func(ctx context.Context, tx pgx.Tx, r kafkax.Record, log *console.Logger) ([]emission, error)

	// alongside is extra work the service runs next to its consumer, used by
	// order-service for the HTTP API.
	alongside func(ctx context.Context, pool *pgxpool.Pool, log *console.Logger) error
}

// run wires the participant up and blocks until ctx is cancelled or something
// fails.
func (p participant) run(ctx context.Context, cfg Config) error {
	log := cfg.Printer.For(p.name)

	pool, err := db.Open(ctx, cfg.Postgres, p.role())
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := db.Apply(ctx, pool, p.ddl); err != nil {
		return err
	}

	producer, err := kafkax.NewProducer(cfg.Brokers)
	if err != nil {
		return err
	}
	defer producer.Close()

	consumer, err := kafkax.NewConsumer(cfg.Brokers, p.name, p.topics...)
	if err != nil {
		return err
	}
	defer consumer.Close()

	// Cancelling here stops the relay and the API once the consumer is done,
	// and the WaitGroup keeps them from outliving the pool they use.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errs := make(chan error, 2)
	start := func(fn func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := fn(ctx); err != nil && ctx.Err() == nil {
				errs <- err
				cancel()
			}
		}()
	}
	if p.publishes {
		start(outbox.NewRelay(pool, p.schema, producer, log.Relay()).Run)
	}
	if p.alongside != nil {
		start(func(ctx context.Context) error { return p.alongside(ctx, pool, log) })
	}

	err = consumer.Run(ctx, func(ctx context.Context, r kafkax.Record) error {
		return p.act(ctx, pool, log, r)
	})
	cancel()
	wg.Wait()
	close(errs)

	if err != nil {
		return err
	}
	return <-errs
}

// act runs one record through the handler. The state change and the events it
// produced reach Postgres together or not at all; only then are they logged,
// because until the commit neither of them happened.
func (p participant) act(ctx context.Context, pool *pgxpool.Pool, log *console.Logger, r kafkax.Record) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	out, err := p.handle(ctx, tx, r, log)
	if err != nil {
		return err
	}
	for _, e := range out {
		if err := outbox.Insert(ctx, tx, p.schema, e.topic, e.env); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	for _, e := range out {
		log.Outboxed(e.topic, e.env, e.detail)
	}
	return nil
}

// role is the Postgres role a service connects as. It owns exactly one schema
// and cannot write into anyone else's.
func (p participant) role() string { return p.schema + "_service" }
