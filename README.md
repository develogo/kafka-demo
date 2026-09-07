# kafka-demo

A distributed e-commerce order flow on Kafka, written in Go. Five services
choreograph an order from placed to completed — or cancelled — by reacting to
each other's events. Nothing calls anything directly.

Infrastructure is a local Kafka (KRaft, no ZooKeeper) plus Kafka UI, via Docker
Compose.

## The saga

```
orders/OrderCreated  ->  payments/PaymentApproved  ->  inventory/InventoryReserved
                     ->  shipments/ShipmentDispatched  ->  orders/OrderCompleted

orders/OrderCreated  ->  payments/PaymentRejected  ->  orders/OrderCancelled
```

Four topics, one per aggregate, three partitions each. The event type rides in
an `event_type` record header, so each consumer routes on the type rather than
on the topic ([ADR-0001](docs/adr/0001-aggregate-topics-with-event-type-header.md)).

| Topic | Produced by | Event types | Consumer groups |
| --- | --- | --- | --- |
| `orders` | order-service | `OrderCreated`, `OrderCancelled`, `OrderCompleted` | payment-service, notification-service |
| `payments` | payment-service | `PaymentApproved`, `PaymentRejected` | inventory-service, order-service, notification-service |
| `inventory` | inventory-service | `InventoryReserved` | shipping-service, notification-service |
| `shipments` | shipping-service | `ShipmentDispatched` | order-service, notification-service |

`order-service` both starts and closes the saga, which is why `orders` carries
three event types ([ADR-0002](docs/adr/0002-choreographed-saga.md)).
`notification-service` subscribes to all four topics and skips nothing, so its
lines are the full timeline of every order.

The order id is the partition key, so every event about one order lands on the
same partition of every topic and its timeline keeps its order.

## Run it

```bash
docker compose up -d      # Kafka on localhost:9092, Kafka UI on http://localhost:8082
go run ./cmd/demo
```

The demo creates the topics (three partitions — the broker's auto-creation would
give them one), then runs the five services as goroutines, each with its own
Kafka client and consumer group. Output is one line per event:

```
🛒 order-service        | ORD-c7cc-0003  -> orders/OrderCreated        p0 @10  BRL 38997.00
💳 payment-service      | ORD-c7cc-0003  <- orders/OrderCreated        p0 @10  BRL 38997.00
💳 payment-service      | ORD-c7cc-0003  -> payments/PaymentRejected   p0 @5   above the BRL 5000.00 limit
📦 inventory-service    | ORD-c7cc-0003   x payments/PaymentRejected   p0 @5   nothing to reserve for this type
🛒 order-service        | ORD-c7cc-0003  <- payments/PaymentRejected   p0 @5   closing the order
🛒 order-service        | ORD-c7cc-0003  -> orders/OrderCancelled      p0 @11  above the BRL 5000.00 limit
```

`->` published, `<-` consumed and acted on, ` x` consumed and ignored. The greyed
out `x` lines are the point of aggregate topics: every consumer of a topic
receives every type on it, and filtering is the application's job. `pN @M` is
the partition and offset the record occupies. The middle segment of an order id
names the run that placed it.

The third order of every five is deliberately expensive, so both the approved
and the rejected branch show up on every run, always in the same place.

### Flags

| Flag | Default | |
| --- | --- | --- |
| `-brokers` | `localhost:9092` | bootstrap server |
| `-orders` | `5` | orders to place; `0` runs until Ctrl-C |
| `-interval` | `1500ms` | delay between orders |
| `-reject-above-cents` | `500000` | payment rejects orders above this total |
| `-partitions` | `3` | partitions to create topics with |
| `-drain` | `1s` | grace period for consumers to catch up before shutdown |
| `-timeout` | `2m` | give up if a bounded run has not finished |

### One process per service

`cmd/demo` is a convenience. The same services also build as separate binaries,
each taking the same flags:

```bash
go run ./cmd/payment-service      # and inventory-, shipping-, notification-
go run ./cmd/order-service        # places the orders
```

The Kafka topology is identical either way.

## Consumer offsets

A group with no committed offsets starts at the beginning of the log, and
offsets are committed as it goes — so a second run resumes rather than replays.

Delivery is still at-least-once, and a run that exits before its last offsets
are committed leaves records behind for the next one to re-read. That is why an
order id names the run that placed it (`ORD-c7cc-0003`): order numbers restart
at 1 every run, so without the run label a replayed `ShipmentDispatched` would
look like it belonged to an order the current run had just placed, and
`order-service` would close the wrong one. Replays now show up as skipped,
labelled `placed by an earlier run`.

To start from a clean slate:

```bash
docker compose down -v && docker compose up -d
```

## Layout

```
cmd/demo/              all five services in one process
cmd/<name>-service/    one process per service
internal/events/       envelope, event types, topic names
internal/kafkax/       topic creation, producer, consumer-group loop
internal/console/      the coloured output
internal/services/     the five services
```

`CONTEXT.md` holds the vocabulary; `docs/adr/` holds the decisions behind the
shape of it.

## Poking at it by hand

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders --from-beginning
```

On Git Bash (Windows), prefix with `MSYS_NO_PATHCONV=1` so `/opt/...` is not
rewritten. Kafka UI at http://localhost:8082 shows the same records with their
headers; it uses port 8082 because 8080 and 8081 were taken on the development
machine.

## Tear down

```bash
docker compose down        # keeps the data in the kafka-data volume
docker compose down -v     # drops the data too
```
