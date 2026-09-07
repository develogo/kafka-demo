// Command demo runs the whole order saga in one process: the five services
// each keep their own Kafka client and consumer group, so the topology is the
// same one the cmd/<service> binaries produce, but the story fits in a single
// terminal.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/develogo/kafka-demo/internal/events"
	"github.com/develogo/kafka-demo/internal/kafkax"
	"github.com/develogo/kafka-demo/internal/services"
)

func main() {
	fs := flag.NewFlagSet("demo", flag.ExitOnError)
	partitions := fs.Int("partitions", 3, "partitions to create topics with")
	drain := fs.Duration("drain", time.Second, "grace period for consumers to catch up before shutting down")
	timeout := fs.Duration("timeout", 2*time.Minute, "give up if a bounded run has not finished by then")
	config := services.Flags(fs)
	_ = fs.Parse(os.Args[1:])

	if err := run(config(), int32(*partitions), *drain, *timeout); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run(cfg services.Config, partitions int32, drain, timeout time.Duration) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	if cfg.Orders > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	if err := kafkax.EnsureTopics(ctx, cfg.Brokers, partitions, events.AllTopics()...); err != nil {
		return err
	}
	cfg.Printer.Note("topics ready on %s: %s (%d partitions each)",
		strings.Join(cfg.Brokers, ","), strings.Join(events.AllTopics(), ", "), partitions)

	// The four reactive services run until the saga is over; order-service
	// drives it and returns once every order it placed has been closed.
	reactive, cancelReactive := context.WithCancel(ctx)
	defer cancelReactive()

	var wg sync.WaitGroup
	errs := make(chan error, 4)
	for name, service := range map[string]func(context.Context, services.Config) error{
		"payment-service":      services.RunPayment,
		"inventory-service":    services.RunInventory,
		"shipping-service":     services.RunShipping,
		"notification-service": services.RunNotification,
	} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := service(reactive, cfg); err != nil && reactive.Err() == nil {
				errs <- fmt.Errorf("%s: %w", name, err)
			}
		}()
	}

	err := services.RunOrder(ctx, cfg)

	// Let the consumers print what is still in flight before tearing down.
	select {
	case <-time.After(drain):
	case <-ctx.Done():
	}
	cancelReactive()
	wg.Wait()
	close(errs)

	if err != nil {
		return fmt.Errorf("order-service: %w", err)
	}
	if serviceErr := <-errs; serviceErr != nil {
		return serviceErr
	}
	cfg.Printer.Note("done — inspect the topics at http://localhost:8082")
	return nil
}
