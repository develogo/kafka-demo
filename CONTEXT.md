# Order Saga

A demo of a distributed e-commerce order flow on Kafka. Five Go services react
to each other's events to take an order from placed to completed (or
cancelled), with no service calling another directly.

## Language

**Order**:
A customer's request to buy items, identified by an Order ID and closed by
exactly one terminal event.
_Avoid_: Purchase, transaction, cart, checkout

**Order ID**:
The identifier of an Order and the partition key of every event about it, so
one order's events stay ordered across all topics.
_Avoid_: Correlation ID, request ID

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
and the events it publishes in response.
_Avoid_: Worker, handler, microservice, node

**Saga**:
The chain of events that carries an order to a terminal state, choreographed by
the services themselves rather than by a central coordinator.
_Avoid_: Workflow, pipeline, orchestration, process manager

**Terminal Event**:
`OrderCompleted` or `OrderCancelled`: the last event an order will ever
produce.
_Avoid_: Final state, closing event, result

## Order lifecycle

**Placed**:
`OrderCreated` is on the `orders` topic and nothing has acted on it yet.

**Approved / Rejected**:
Payment decided. Above the payment limit an order is always rejected, which is
what makes the failure branch appear on every run.

**Reserved**:
Stock is held for an approved order.

**Dispatched**:
A carrier has the order and a tracking code exists.

**Completed / Cancelled**:
The order reached a terminal event and will produce nothing further.
