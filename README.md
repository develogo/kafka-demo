# kafka-demo

A distributed e-commerce order flow on Kafka, written in Go, with a Next.js
front end and Postgres behind every service. You click a button, the order is
written to a database, and six services choreograph it from placed to completed
— or cancelled — by reacting to each other's events. Nothing calls anything
directly.

Infrastructure is a local Kafka (KRaft, no ZooKeeper), Postgres and Kafka UI,
via Docker Compose.

## The saga

```
orders/OrderCreated  ->  payments/PaymentApproved  ->  inventory/InventoryReserved
                     ->  shipments/ShipmentDispatched  ->  orders/OrderCompleted

orders/OrderCreated  ->  payments/PaymentRejected  ->  orders/OrderCancelled
```

Four topics, one per aggregate, three partitions each. The event type rides in
an `event_type` record header, so each consumer routes on the type rather than
on the topic ([ADR-0001](docs/adr/0001-aggregate-topics-with-event-type-header.md)).

| Topic | Written by | Event types | Consumer groups |
| --- | --- | --- | --- |
| `orders` | order-service | `OrderCreated`, `OrderCancelled`, `OrderCompleted` | payment-service, notification-service, projection-service |
| `payments` | payment-service | `PaymentApproved`, `PaymentRejected` | inventory-service, order-service, notification-service, projection-service |
| `inventory` | inventory-service | `InventoryReserved` | shipping-service, notification-service, projection-service |
| `shipments` | shipping-service | `ShipmentDispatched` | order-service, notification-service, projection-service |

`order-service` both starts and closes the saga, which is why `orders` carries
three event types ([ADR-0002](docs/adr/0002-choreographed-saga.md)).
`notification-service` subscribes to all four topics and skips nothing.

The order id is the partition key, so every event about one order lands on the
same partition of every topic and its timeline keeps its order.

## Postgres

Every service owns one schema and connects as its own role, so the ownership
boundary is enforced rather than merely documented — `payment-service` gets
`permission denied for schema orders` if it tries to look at an order.
Everything it knows about an order arrived in an event.

| Schema | Role | Tables |
| --- | --- | --- |
| `orders` | `orders_service` | `orders`, `outbox` |
| `payments` | `payments_service` | `payments`, `outbox` |
| `inventory` | `inventory_service` | `reservations`, `outbox` |
| `shipments` | `shipments_service` | `shipments`, `outbox` |
| `notifications` | `notifications_service` | `notifications` |
| `projection` | `projection_service` | `order_status`, `order_timeline` |

Roles and schemas come from [`docker/postgres/init.sql`](docker/postgres/init.sql),
which the Postgres container runs once. The tables come from the service that
owns them, on start-up.

### The outbox

No service publishes to Kafka. A service writes its state change *and* the
events it caused to its own outbox in one transaction, and a relay goroutine
publishes them afterwards and marks the row published
([ADR-0004](docs/adr/0004-transactional-outbox-instead-of-publishing-directly.md)).
State and events can no longer disagree about what happened.

The terminal shows both halves: `=>` is the write to the outbox, `->` is the
relay putting it on a topic. The gap between the two lines is what the pattern
costs.

### The projection

Under choreography no service sees more than the events it subscribes to, so
none of them can answer "where is this order?". `projection-service` subscribes
to all four topics and keeps `order_status` and `order_timeline`, which is the
only thing the front end reads
([ADR-0003](docs/adr/0003-projection-service-owns-the-order-lifecycle-view.md)).
It is derived: dropping it loses nothing that replaying the topics would not
rebuild.

## Run it

```bash
docker compose up -d      # Kafka :9092, Postgres :5434, Kafka UI :8082
go run ./cmd/demo         # the six services, and the API on :8090
cd web && npm install && npm run dev   # the front end on :3000
```

Then open <http://localhost:3000> and press a button. `cmd/demo` creates the
topics (three partitions — the broker's auto-creation would give them one) and
runs the six services as goroutines, each with its own Kafka client, consumer
group and Postgres role.

Output is one line per event:

```
🛒 order-service          | b91244694e2c => orders/OrderCreated       outbox   BRL 38997.00
📮 order-service relay    | b91244694e2c -> orders/OrderCreated       p2 @4
🔔 notification-service   | b91244694e2c <- orders/OrderCreated       p2 @4    email: we received your order
💳 payment-service        | b91244694e2c <- orders/OrderCreated       p2 @4    BRL 38997.00
💳 payment-service        | b91244694e2c => payments/PaymentRejected  outbox   BRL 38997.00 is above the BRL 5000.00 limit
📦 inventory-service      | b91244694e2c  x payments/PaymentRejected  p2 @2    nothing to reserve for this event type
📮 payment-service relay  | b91244694e2c -> payments/PaymentRejected  p2 @2
🗂️ projection-service     | b91244694e2c <- payments/PaymentRejected  p2 @2    PLACED -> REJECTED
🛒 order-service          | b91244694e2c <- payments/PaymentRejected  p2 @2    closing the order
🛒 order-service          | b91244694e2c => orders/OrderCancelled     outbox   BRL 38997.00 is above the BRL 5000.00 limit
```

`=>` written to the outbox, `->` published, `<-` consumed and acted on, ` x`
consumed and ignored. The greyed out `x` lines are the point of aggregate
topics: every consumer of a topic receives every type on it, and filtering is
the application's job. `pN @M` is the partition and offset the record occupies.
The id is the tail of the order's UUID, which is what the front end shows too.

The front's second button places an order above the payment limit, so the
rejection branch is one click away rather than hidden in a counter.

### The API

`order-service` serves it, and `web/next.config.ts` proxies `/api/*` to it, so
the browser stays on one origin and no Go code deals with CORS.

| | | |
| --- | --- | --- |
| `POST /orders` | `{"kind":"standard"\|"expensive"}` | `202` with the new order id |
| `GET /orders` | | the 20 most recent, from the projection |
| `GET /orders/{id}` | | one order and its timeline |

`POST` answers before anything has been published: the order exists in
`orders.orders`, and the event follows once the relay gets to it. Until the
projection catches up, `GET /orders/{id}` answers `404` — the front end says so
rather than pretending otherwise, because that gap is the thing being
demonstrated.

### Flags

| Flag | Default | |
| --- | --- | --- |
| `-brokers` | `localhost:9092` | bootstrap server |
| `-postgres` | `postgres://service:demo@localhost:5434/kafkademo?sslmode=disable` | the user is replaced by each service's role |
| `-api` | `localhost:8090` | where order-service serves the API |
| `-reject-above-cents` | `500000` | payment rejects orders above this total |
| `-partitions` | `3` | partitions to create topics with |

### One process per service

`cmd/demo` is a convenience. The same services also build as separate binaries,
each taking the same flags:

```bash
go run ./cmd/order-service        # serves the API and closes the saga
go run ./cmd/payment-service      # and inventory-, shipping-, notification-, projection-
```

The Kafka topology and the Postgres schemas are identical either way.

## Delivery is at-least-once

A group with no committed offsets starts at the beginning of the log. Offsets
are committed by hand after the handler's transaction commits, never before: an
offset that moved past a record whose transaction had not committed would lose
the event rather than merely repeat it.

Repeating is still possible — a relay that dies between the broker's ack and
its `UPDATE` republishes on restart — so every service guards for it in
Postgres:

| Service | Guard |
| --- | --- |
| payment, inventory, shipping | `INSERT ... ON CONFLICT (order_id) DO NOTHING` |
| notification, projection | `ON CONFLICT (event_id) DO NOTHING` |
| order | `UPDATE ... WHERE id = $1 AND status <> 'COMPLETED' AND status <> 'CANCELLED'` |

order-service's conditional update covers a second case for free: a terminal
event for an order this database never placed matches nothing, so it closes
nothing. That is why order ids are UUIDv7 and nothing has to label the run that
placed them.

To start from a clean slate:

```bash
docker compose down -v && docker compose up -d
```

## Layout

```
cmd/demo/              all six services in one process
cmd/<name>-service/    one process per service
internal/events/       envelope, event types, topic names, order status
internal/kafkax/       topic creation, producer, consumer-group loop
internal/db/           the Postgres pool, one role per service
internal/outbox/       the outbox table and its relay
internal/console/      the coloured output
internal/services/     the six services and the HTTP API
docker/postgres/       roles and schemas, run once by the container
web/                   the Next.js front end
```

`CONTEXT.md` holds the vocabulary; `docs/adr/` holds the decisions behind the
shape of it.

## Poking at it by hand

```bash
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders --from-beginning

docker exec -it kafka-demo-postgres psql -U postgres -d kafkademo \
  -c 'select event_type, created_at, published_at from orders.outbox order by id'
```

On Git Bash (Windows), prefix the first with `MSYS_NO_PATHCONV=1` so `/opt/...`
is not rewritten. Kafka UI at <http://localhost:8082> shows the same records
with their headers; it uses port 8082, and Postgres 5434, because the obvious
ports were taken on the development machine.

## Tear down

```bash
docker compose down        # keeps the Kafka and Postgres volumes
docker compose down -v     # drops the data too
```
