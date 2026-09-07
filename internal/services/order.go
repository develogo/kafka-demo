package services

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/develogo/kafka-demo/internal/console"
	"github.com/develogo/kafka-demo/internal/events"
	"github.com/develogo/kafka-demo/internal/kafkax"
)

// errDone unwinds the consume loop once every order this run placed has
// reached a terminal state.
var errDone = errors.New("all orders settled")

// catalog is fixed so two runs of the demo produce the same story.
var catalog = []events.Item{
	{SKU: "SKU-HEADSET", Name: "Noise-Cancelling Headset", Quantity: 1, UnitCents: 89900},
	{SKU: "SKU-COFFEE", Name: "Espresso Machine", Quantity: 1, UnitCents: 129900},
	{SKU: "SKU-CHAIR", Name: "Ergonomic Chair", Quantity: 1, UnitCents: 249900},
	{SKU: "SKU-MOUSE", Name: "Wireless Mouse", Quantity: 2, UnitCents: 34900},
	{SKU: "SKU-MONITOR", Name: "4K Monitor", Quantity: 1, UnitCents: 199900},
}

// bigTicket is the order that trips the payment limit.
var bigTicket = events.Item{SKU: "SKU-LAPTOP", Name: "Workstation Laptop", Quantity: 3, UnitCents: 1_299_900}

// bigTicketEvery makes the third order of every five deliberately expensive,
// so both the approved and the rejected branch show up on every run, always in
// the same place.
const bigTicketEvery = 5
const bigTicketIndex = 2

// RunOrder places orders and closes their lifecycle: it consumes payments and
// shipments, and publishes the terminal OrderCancelled / OrderCompleted back
// onto the orders topic. That is what makes orders carry three event types and
// forces payment-service to route by type rather than by topic.
func RunOrder(ctx context.Context, cfg Config) error {
	const name = "order-service"
	log := cfg.Printer.For(name)

	producer, err := kafkax.NewProducer(cfg.Brokers)
	if err != nil {
		return err
	}
	defer producer.Close()

	consumer, err := kafkax.NewConsumer(cfg.Brokers, name, events.TopicPayments, events.TopicShipments)
	if err != nil {
		return err
	}
	defer consumer.Close()

	run := newRunID()
	placed := &openOrders{ids: map[string]struct{}{}, total: cfg.Orders}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var producing sync.WaitGroup
	producing.Add(1)
	go func() {
		defer producing.Done()
		if err := placeOrders(ctx, cfg, run, producer, log, placed); err != nil && ctx.Err() == nil {
			log.Note("stopped placing orders: %v", err)
		}
	}()

	err = consumer.Run(ctx, func(ctx context.Context, r kafkax.Record) error {
		var terminal events.Type
		var reason string

		switch r.EventType {
		case events.PaymentRejected:
			payment, err := events.Decode[events.Payment](r.Envelope)
			if err != nil {
				return err
			}
			terminal, reason = events.OrderCancelled, payment.Reason
		case events.ShipmentDispatched:
			shipment, err := events.Decode[events.Shipment](r.Envelope)
			if err != nil {
				return err
			}
			terminal, reason = events.OrderCompleted, "handed to "+shipment.Carrier
		default:
			// This service consumes payments and shipments, but an approval
			// is inventory-service's business, not its own.
			log.Skipped(r, "no action for this event type")
			return nil
		}

		// Delivery is at-least-once. An order id names the run that placed it,
		// so a terminal event replayed from an earlier run is not ours to
		// close and must not count towards this run finishing.
		if !placed.owns(r.Envelope.OrderID) {
			log.Skipped(r, "placed by an earlier run")
			return nil
		}
		log.Received(r, "closing the order")

		if err := pause(ctx, orderLatency); err != nil {
			return nil
		}
		env, err := events.New(terminal, r.Envelope.OrderID, events.Outcome{Reason: reason})
		if err != nil {
			return err
		}
		published, err := producer.Publish(ctx, events.TopicOrders, env)
		if err != nil {
			return err
		}
		log.Published(published, reason)

		if placed.settle(r.Envelope.OrderID) {
			return errDone
		}
		return nil
	})
	// Stop placing orders and let that goroutine unwind before the deferred
	// Close pulls the producer out from under it.
	cancel()
	producing.Wait()

	if errors.Is(err, errDone) {
		return nil
	}
	return err
}

func placeOrders(ctx context.Context, cfg Config, run string, producer *kafkax.Producer, log *console.Logger, placed *openOrders) error {
	for i := 0; cfg.Orders == 0 || i < cfg.Orders; i++ {
		if err := pause(ctx, cfg.Interval); err != nil {
			return nil
		}

		id := orderID(run, i)
		order := buildOrder(i)
		placed.track(id)

		env, err := events.New(events.OrderCreated, id, order)
		if err != nil {
			return err
		}
		published, err := producer.Publish(ctx, events.TopicOrders, env)
		if err != nil {
			return err
		}
		log.Published(published, events.BRL(order.TotalCents))
	}
	return nil
}

// newRunID labels the orders of one run. Order numbers restart at 1 every
// run, so without it a record replayed from an earlier run would carry an id
// the current run is also using, and order-service would close the wrong
// order.
func newRunID() string {
	return fmt.Sprintf("%04x", time.Now().UnixNano()&0xffff)
}

// orderID names the i-th order of a run.
func orderID(run string, i int) string {
	return fmt.Sprintf("ORD-%s-%04d", run, i+1)
}

// buildOrder is deterministic: order i always has the same contents.
func buildOrder(i int) events.Order {
	items := []events.Item{catalog[i%len(catalog)]}
	if i%bigTicketEvery == bigTicketIndex {
		items = []events.Item{bigTicket}
	}
	return events.Order{
		CustomerID: fmt.Sprintf("CUST-%04d", i%3+1),
		Items:      items,
		TotalCents: events.TotalCents(items),
	}
}

// openOrders tracks the orders this run placed and how many are still to be
// closed. It is also the idempotency guard: delivery is at-least-once, so a
// terminal event must not be published twice for the same order.
type openOrders struct {
	mu      sync.Mutex
	ids     map[string]struct{}
	settled int
	total   int // 0 means the run has no end
}

func (l *openOrders) track(id string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.ids[id] = struct{}{}
}

func (l *openOrders) owns(id string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	_, ok := l.ids[id]
	return ok
}

// settle closes an order and reports whether every order of a bounded run is
// now done.
func (l *openOrders) settle(id string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.ids[id]; !ok {
		return false
	}
	delete(l.ids, id)
	l.settled++
	return l.total > 0 && l.settled >= l.total
}
