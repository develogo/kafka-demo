# A projection service owns the view of the whole order lifecycle

The web front needs to show one order moving through placed → approved →
reserved → dispatched → completed, but ADR-0002 put that knowledge nowhere: no
service in a choreographed saga sees more than the events it subscribes to. So
`projection-service` subscribes to all four topics and maintains a Projection —
the one row per order that carries the Order Status and the timestamp of every
step. It is the only thing the front reads.

## Considered options

**`order-service` consumes all four topics and keeps the status itself.** It
already owns the Order, so the column would sit with its aggregate. Rejected:
it would make the one service that starts and closes the saga also the one that
watches every step of it, which is an orchestrator wearing a different hat, and
would contradict ADR-0002 in everything but name.

**The front reads all four service tables and stitches the timeline.** Honest
about the choreography — the view really is assembled, not owned. Rejected: it
makes every service's internal schema part of the browser's contract, so
`payments` could not change a column without breaking the UI.

**Promote `notification-service`, which already subscribes to all four
topics.** Rejected: notification is a side effect, projection is state. A
service that both sends the email and answers "what is the status" has two
reasons to change and no clear owner for either.

## Consequences

- The Projection is derived, never authoritative. Nothing in the saga reads it,
  and deleting it loses nothing that replaying the topics would not rebuild.
- ADR-0002 said `notification-service` was "the closest thing to a view of the
  whole saga". That is now the Projection's job; `notification-service` goes
  back to being only notifications.
- The front sees eventual consistency directly: `POST /orders` returns before
  the Projection knows the order exists, so the first poll can legitimately
  come back empty.
