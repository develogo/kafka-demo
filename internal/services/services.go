// Package services holds the participants of the order saga. Each one is an
// independent consumer group with its own Postgres schema: it reacts to the
// events it cares about, records what it did, and writes what happens next to
// its outbox.
package services

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/develogo/kafka-demo/internal/console"
	"github.com/develogo/kafka-demo/internal/db"
	"github.com/develogo/kafka-demo/internal/events"
	"github.com/develogo/kafka-demo/internal/kafkax"
)

// Config is shared by every service so the combined demo and the standalone
// binaries configure them the same way.
type Config struct {
	Brokers          []string
	Postgres         string // DSN; the user in it is replaced per service
	Printer          *console.Printer
	RejectAboveCents int64  // payment rejects anything above this total
	APIAddr          string // where order-service serves the front end
}

// DefaultRejectAboveCents is the payment limit the demo ships with. The
// expensive order the front can place sits above it; the standard one does not.
const DefaultRejectAboveCents int64 = 500_000

// DefaultAPIAddr is where order-service listens, and what web/next.config.ts
// proxies to.
const DefaultAPIAddr = "localhost:8090"

// Latency each service pretends to spend on its work, so the chain unfolds at
// a readable pace instead of all at once.
const (
	paymentLatency   = 250 * time.Millisecond
	inventoryLatency = 200 * time.Millisecond
	shippingLatency  = 300 * time.Millisecond
	orderLatency     = 100 * time.Millisecond
)

// pause waits unless the context is cancelled first.
func pause(ctx context.Context, d time.Duration) error {
	select {
	case <-time.After(d):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Flags registers the shared configuration on fs.
func Flags(fs *flag.FlagSet) func() Config {
	brokers := fs.String("brokers", kafkax.DefaultBroker, "Kafka bootstrap server")
	postgres := fs.String("postgres", db.DefaultDSN, "Postgres DSN (the user is replaced per service)")
	rejectAbove := fs.Int64("reject-above-cents", DefaultRejectAboveCents, "payment rejects orders above this total, in cents")
	apiAddr := fs.String("api", DefaultAPIAddr, "address order-service serves the API on")

	return func() Config {
		return Config{
			Brokers:          []string{*brokers},
			Postgres:         *postgres,
			Printer:          console.NewPrinter(),
			RejectAboveCents: *rejectAbove,
			APIAddr:          *apiAddr,
		}
	}
}

// Main runs one service as its own process. cmd/demo wires the same functions
// into a single process instead; the Kafka topology is identical either way.
func Main(name string, run func(context.Context, Config) error) {
	fs := flag.NewFlagSet(name, flag.ExitOnError)
	partitions := fs.Int("partitions", 3, "partitions to create topics with")
	config := Flags(fs)
	_ = fs.Parse(os.Args[1:])
	cfg := config()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := kafkax.EnsureTopics(ctx, cfg.Brokers, int32(*partitions), events.AllTopics()...); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	if err := run(ctx, cfg); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}
