// Package kafkax holds the Kafka wiring the five services share: topic
// creation, a producer that speaks envelopes, and a consumer-group loop.
package kafkax

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/develogo/kafka-demo/internal/events"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// DefaultBroker is what docker-compose.yml advertises to the host.
const DefaultBroker = "localhost:9092"

// Record is a decoded envelope plus the log coordinates that prove it really
// travelled through Kafka.
type Record struct {
	Topic     string
	Partition int32
	Offset    int64
	// EventType comes from the record header, so routing never needs the
	// value. Envelope.EventType mirrors it.
	EventType events.Type
	Envelope  events.Envelope
}

// EnsureTopics creates the demo topics if they are missing. The broker has
// auto-creation enabled, but it would create them with a single partition,
// which would hide the partitioning this demo is meant to show.
func EnsureTopics(ctx context.Context, brokers []string, partitions int32, topics ...string) error {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return fmt.Errorf("connect to %v: %w", brokers, err)
	}
	defer cl.Close()

	resp, err := kadm.NewClient(cl).CreateTopics(ctx, partitions, 1, nil, topics...)
	if err != nil {
		return fmt.Errorf("create topics: %w", err)
	}
	for _, r := range resp.Sorted() {
		if r.Err != nil && !errors.Is(r.Err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("create topic %q: %w", r.Topic, r.Err)
		}
	}
	return nil
}

// Producer publishes envelopes.
type Producer struct{ cl *kgo.Client }

func NewProducer(brokers []string) (*Producer, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		return nil, fmt.Errorf("producer: connect to %v: %w", brokers, err)
	}
	return &Producer{cl: cl}, nil
}

// Publish writes the envelope and waits for the broker to acknowledge it, so
// the caller gets back the partition and offset it landed on.
func (p *Producer) Publish(ctx context.Context, topic string, env events.Envelope) (Record, error) {
	value, err := json.Marshal(env)
	if err != nil {
		return Record{}, fmt.Errorf("encode %s: %w", env.EventType, err)
	}
	// The order id is the partition key: every event about one order lands on
	// the same partition of every topic, which is the only ordering guarantee
	// Kafka offers.
	res, err := p.cl.ProduceSync(ctx, &kgo.Record{
		Topic:   topic,
		Key:     []byte(env.OrderID),
		Value:   value,
		Headers: []kgo.RecordHeader{{Key: events.HeaderEventType, Value: []byte(env.EventType)}},
	}).First()
	if err != nil {
		return Record{}, fmt.Errorf("publish %s to %s: %w", env.EventType, topic, err)
	}
	return Record{
		Topic:     res.Topic,
		Partition: res.Partition,
		Offset:    res.Offset,
		EventType: env.EventType,
		Envelope:  env,
	}, nil
}

func (p *Producer) Close() { p.cl.Close() }

// Consumer is one service's consumer group.
type Consumer struct {
	cl    *kgo.Client
	group string
}

// NewConsumer joins group and subscribes to topics. A group with no committed
// offsets starts at the beginning of the log; offsets are committed as it
// goes, so restarting resumes instead of replaying.
func NewConsumer(brokers []string, group string, topics ...string) (*Consumer, error) {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return nil, fmt.Errorf("consumer %s: connect to %v: %w", group, brokers, err)
	}
	return &Consumer{cl: cl, group: group}, nil
}

// Run polls until ctx is cancelled, the client is closed, or handle fails.
func (c *Consumer) Run(ctx context.Context, handle func(context.Context, Record) error) error {
	for {
		fetches := c.cl.PollFetches(ctx)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			return fmt.Errorf("%s: fetch %s: %w", c.group, errs[0].Topic, errs[0].Err)
		}

		var handleErr error
		fetches.EachRecord(func(r *kgo.Record) {
			if handleErr != nil {
				return
			}
			var env events.Envelope
			if err := json.Unmarshal(r.Value, &env); err != nil {
				handleErr = fmt.Errorf("%s: decode %s@%d: %w", c.group, r.Topic, r.Offset, err)
				return
			}
			eventType := headerEventType(r)
			if eventType == "" {
				eventType = env.EventType
			}
			handleErr = handle(ctx, Record{
				Topic:     r.Topic,
				Partition: r.Partition,
				Offset:    r.Offset,
				EventType: eventType,
				Envelope:  env,
			})
		})
		if handleErr != nil {
			return handleErr
		}
	}
}

// Close leaves the group, committing the offsets processed so far.
func (c *Consumer) Close() { c.cl.Close() }

func headerEventType(r *kgo.Record) events.Type {
	for _, h := range r.Headers {
		if h.Key == events.HeaderEventType {
			return events.Type(h.Value)
		}
	}
	return ""
}
