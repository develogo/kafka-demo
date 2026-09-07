# The order saga is choreographed, not orchestrated

Each service consumes the event that concerns it and publishes the next one:
`OrderCreated` → `PaymentApproved` → `InventoryReserved` →
`ShipmentDispatched` → `OrderCompleted`. No service knows who comes after it,
and there is no coordinator holding the flow.

The alternative was an orchestrator owning the sequence and telling each service
what to do next. Orchestration makes the flow readable in one place and failure
handling easier to reason about, but it centralises knowledge of the whole
process and would reduce the other services to RPC endpoints — which would
demonstrate the opposite of what this repo is for. Choreography puts the
coupling in the events instead.

## Consequences

- `order-service` both starts and closes the saga: it publishes `OrderCreated`,
  and it consumes `payments` and `shipments` to publish the terminal event. That
  cycle is why the `orders` topic carries three event types.
- The full flow exists nowhere in the code. `notification-service` subscribes to
  all four topics and prints the timeline, which is the closest thing to a view
  of the whole saga.
- Compensation would have to be another event. The rejection branch already
  works this way: `PaymentRejected` leads to `OrderCancelled` rather than to a
  rollback.
