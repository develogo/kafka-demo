# Order Saga

A demo of a distributed e-commerce order flow on Kafka. Services react to each
other's events to take an order from placed to completed (or cancelled), with
no service calling another directly. Each keeps its own state in Postgres, so
the flow is visible as data as well as as a log.

## Language

**Order**:
A customer's request to buy items, identified by an Order ID and closed by
exactly one terminal event.
_Avoid_: Purchase, transaction, cart, checkout

**Order ID**:
The identifier of an Order and the partition key of every event about it, so
one order's events stay ordered across all topics.
_Avoid_: Correlation ID, request ID

**Order Status**:
Where an Order currently sits in its lifecycle. Only the Order's owner and the
Projection record it; every other service knows its own aggregate, not the
Order's status.
_Avoid_: State, stage, phase, step

**Event**:
A statement that something already happened, named in the past tense
(`PaymentApproved`, never `ApprovePayment`). The only way services learn about
each other.
_Avoid_: Message, command, notification

**Envelope**:
The common outer shape of every event: event id, event type, order id,
timestamp, and a type-specific `data` payload.
_Avoid_: Header, wrapper, metadata

**Event Type**:
The name of an event. Because topics are per aggregate, the event type — not
the topic — decides which service acts on a record.
_Avoid_: Kind, action, subject

**Aggregate Topic**:
A topic named after the aggregate it describes (`orders`, `payments`,
`inventory`, `shipments`), carrying every event type about that aggregate.
_Avoid_: Channel, queue, stream, event topic

**Service**:
One participant of the saga: one consumer group, one set of subscribed topics,
one Postgres schema it alone may write, and the events it publishes in
response.
_Avoid_: Worker, handler, microservice, node

**Saga**:
The chain of events that carries an order to a terminal state, choreographed by
the services themselves rather than by a central coordinator.
_Avoid_: Workflow, pipeline, orchestration, process manager

**Terminal Event**:
`OrderCompleted` or `OrderCancelled`: the last event an order will ever
produce.
_Avoid_: Final state, closing event, result

## Writing events down

**Outbox**:
Events a Service has decided to publish, written in the same transaction as the
state change that caused them. A Service publishes nothing directly; it writes
to its Outbox and is done.
_Avoid_: Queue, buffer, spool, pending events, event log

**Relay**:
The loop that reads a Service's Outbox in order, publishes each event to Kafka,
and marks it published. One per Service, and the only thing that talks to a
producer.
_Avoid_: Dispatcher, publisher, pump, forwarder, worker

**Projection**:
The one place an Order's whole lifecycle is visible, rebuilt purely from the
events on the four topics. It owns no part of the saga and nothing reacts to
it: it exists to be read.
_Avoid_: Read model, view, cache, materialized view, aggregator

## Order lifecycle

The values of Order Status, in the order they occur.

**Placed**:
`OrderCreated` is on the `orders` topic and nothing has acted on it yet.

**Approved / Rejected**:
Payment decided. Above the payment limit an order is always rejected, which is
what makes the failure branch reachable on demand.

**Reserved**:
Stock is held for an approved order.

**Dispatched**:
A carrier has the order and a tracking code exists.

**Completed / Cancelled**:
The order reached a terminal event and will produce nothing further.
