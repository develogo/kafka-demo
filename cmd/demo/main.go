// Command demo runs the whole order saga in one process: the six services each
// keep their own Kafka client, consumer group and Postgres role, so the
// topology is the same one the cmd/<service> binaries produce, but the story
// fits in a single terminal.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"

	"github.com/develogo/kafka-demo/internal/events"
	"github.com/develogo/kafka-demo/internal/kafkax"
	"github.com/develogo/kafka-demo/internal/services"
)

func main() {
	fs := flag.NewFlagSet("demo", flag.ExitOnError)
	partitions := fs.Int("partitions", 3, "partitions to create topics with")
	config := services.Flags(fs)
	_ = fs.Parse(os.Args[1:])

	if err := run(config(), int32(*partitions)); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// saga is every participant, keyed by the name it logs and consumes under.
var saga = map[string]func(context.Context, services.Config) error{
	"order-service":        services.RunOrder,
	"payment-service":      services.RunPayment,
	"inventory-service":    services.RunInventory,
	"shipping-service":     services.RunShipping,
	"notification-service": services.RunNotification,
	"projection-service":   services.RunProjection,
}

// run starts every service and waits. Nothing here ends on its own any more:
// orders arrive from the front end, so the demo runs until it is interrupted.
func run(cfg services.Config, partitions int32) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if err := kafkax.EnsureTopics(ctx, cfg.Brokers, partitions, events.AllTopics()...); err != nil {
		return err
	}
	cfg.Printer.Note("topics ready on %s: %s (%d partitions each)",
		strings.Join(cfg.Brokers, ","), strings.Join(events.AllTopics(), ", "), partitions)

	// One service failing takes the rest down with it: a saga missing a
	// participant would stall silently, which is worse than stopping.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var wg sync.WaitGroup
	errs := make(chan error, len(saga))
	for name, service := range saga {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := service(ctx, cfg); err != nil && ctx.Err() == nil {
				errs <- fmt.Errorf("%s: %w", name, err)
				cancel()
			}
		}()
	}
	cfg.Printer.Note("place an order at http://localhost:3000 — topics at http://localhost:8082")

	wg.Wait()
	close(errs)
	if err := <-errs; err != nil {
		return err
	}
	cfg.Printer.Note("stopped")
	return nil
}
