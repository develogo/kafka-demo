# Aggregate topics with the event type in a record header

Events are grouped into one topic per aggregate — `orders`, `payments`,
`inventory`, `shipments` — rather than one topic per event type, and the event
type travels in an `event_type` record header (mirrored inside the envelope, so
a record stays self-describing once exported from Kafka).

The obvious alternative, a topic per event type, would let a consumer subscribe
to exactly what it wants and never see anything else. We chose aggregate topics
because they are what production systems actually do: they keep the topic count
flat as event types multiply, and they preserve ordering across every event
about one aggregate. The price is that filtering becomes the application's
job — every consumer of a topic receives every type on it and routes on
`event_type`.

## Consequences

- Adding an event type is a code change, not a topic. `orders` grew from one
  type to three (`OrderCreated`, `OrderCancelled`, `OrderCompleted`) without
  touching the broker.
- Every consumer needs a default branch for types it does not handle. The demo
  prints those skipped records in grey on purpose: the filtering is the visible
  consequence of this decision.
- Routing reads the header, so a consumer decides whether a record is for it
  without decoding the value.
