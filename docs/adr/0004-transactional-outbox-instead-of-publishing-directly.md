# Events go through a transactional outbox, not straight to Kafka

Every service now writes its own state to Postgres *and* publishes an event.
Doing both directly is a dual write: a crash in between leaves a service whose
table says the payment was approved and a saga that never heard about it. So no
service calls the producer. Each writes its state change and its event to its
own Outbox in one transaction, and a Relay publishes from the Outbox and marks
the row published.

## Considered options

**Accept the dual write, database first, and document it.** Cheapest by far,
and the demo never crashes on purpose. Rejected: divergence between state and
events is the characteristic failure of this architecture, and a demo of the
architecture that quietly cannot fail that way teaches the wrong lesson.

**Debezium reading the Postgres WAL.** What a lot of production systems
actually run, and it removes the polling loop. Rejected: it brings Kafka
Connect, a connector, and connector configuration — more moving infrastructure
than the rest of the demo combined — and it hides the mechanism inside a
container, when showing the mechanism is the point.

## Consequences

- The Outbox closes the window where an event is lost, not the one where it is
  duplicated. A Relay that dies between the broker's ack and the `UPDATE`
  republishes on restart, so at-least-once still holds and every consumer needs
  its idempotency guard. That guard is now a conditional write in Postgres,
  which survives a restart; the in-memory one it replaces did not.
- Publishing is no longer instantaneous, and the terminal shows it: a service
  logs `-> outbox/PaymentApproved` and its Relay logs `-> payments/...` with
  the partition and offset. The gap between the two lines is the cost of the
  pattern, made visible on purpose.
- Outbox rows are marked published rather than deleted, so the table is a
  readable record of what each service decided and when it left for Kafka.
- Each service carries a Relay goroutine alongside its consumer, and needs a
  Postgres connection before it needs a Kafka one.
