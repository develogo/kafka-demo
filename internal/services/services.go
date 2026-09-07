// Package services holds the five participants of the order saga. Each one is
// an independent consumer group that reacts to the events it cares about and
// publishes what happens next.
package services

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/develogo/kafka-demo/internal/console"
	"github.com/develogo/kafka-demo/internal/events"
	"github.com/develogo/kafka-demo/internal/kafkax"
)

// Config is shared by every service so the combined demo and the standalone
// binaries configure them the same way.
type Config struct {
	Brokers          []string
	Printer          *console.Printer
	Orders           int           // orders to place; 0 keeps going until interrupted
	Interval         time.Duration // delay between orders
	RejectAboveCents int64         // payment rejects anything above this total
}

// DefaultRejectAboveCents is the payment limit the demo ships with. The
// catalog is built so that exactly one order in five sits above it.
const DefaultRejectAboveCents int64 = 500_000

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
	orders := fs.Int("orders", 5, "orders to place (0 = until interrupted)")
	interval := fs.Duration("interval", 1500*time.Millisecond, "delay between orders")
	rejectAbove := fs.Int64("reject-above-cents", DefaultRejectAboveCents, "payment rejects orders above this total, in cents")

	return func() Config {
		return Config{
			Brokers:          []string{*brokers},
			Printer:          console.NewPrinter(),
			Orders:           *orders,
			Interval:         *interval,
			RejectAboveCents: *rejectAbove,
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
