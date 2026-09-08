"use client";

import { useCallback, useEffect, useState } from "react";
import {
  brl,
  clock,
  fetchOrder,
  fetchOrders,
  isFailure,
  isTerminal,
  placeOrder,
  reachedStep,
  STEPS,
  type Order,
  type Status,
} from "./orders";

// How often the browser asks. Polling rather than a push: the delay is the
// demonstration, not a shortcoming.
const POLL_MS = 500;

// The tail of the id, which is what the terminal output prints too.
const shortId = (id: string) => id.slice(-12);

export default function Page() {
  const [orders, setOrders] = useState<Order[]>([]);
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [selected, setSelected] = useState<Order | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [placing, setPlacing] = useState(false);

  useEffect(() => poll(fetchOrders, setOrders, setError), []);
  useEffect(() => {
    if (!selectedId) {
      setSelected(null);
      return;
    }
    return poll(() => fetchOrder(selectedId), setSelected, setError);
  }, [selectedId]);

  const place = useCallback(async (kind: "standard" | "expensive") => {
    setPlacing(true);
    try {
      const { order_id } = await placeOrder(kind);
      // The projection has not seen it yet, and showing that gap is the point.
      setSelected(null);
      setSelectedId(order_id);
      setError(null);
    } catch (e) {
      setError(String(e));
    } finally {
      setPlacing(false);
    }
  }, []);

  return (
    <main>
      <h1>Order Saga</h1>
      <p className="lede">
        Placing an order writes it to Postgres and to an outbox in one
        transaction. Everything below arrives afterwards, one Kafka event at a
        time, as each service reacts and records what it did.
      </p>

      <div className="buttons">
        <button
          className="primary"
          disabled={placing}
          onClick={() => place("standard")}
        >
          Place an order
        </button>
        <button
          className="danger"
          disabled={placing}
          onClick={() => place("expensive")}
        >
          Place an expensive order (will be rejected)
        </button>
      </div>

      {error && <p className="error">{error}</p>}

      <div className="columns">
        <section className="panel">
          <h2>Orders</h2>
          {orders.length === 0 ? (
            <p className="empty">No orders yet.</p>
          ) : (
            <ul className="orders">
              {orders.map((order) => (
                <li key={order.order_id}>
                  <button
                    aria-current={order.order_id === selectedId}
                    onClick={() => setSelectedId(order.order_id)}
                  >
                    <span className="id">{shortId(order.order_id)}</span>
                    <span>{brl(order.total_cents)}</span>
                    <StatusPill status={order.status} />
                  </button>
                </li>
              ))}
            </ul>
          )}
        </section>

        <section className="panel">
          <h2>{selected ? shortId(selected.order_id) : "Order"}</h2>
          {!selectedId && <p className="empty">Pick an order, or place one.</p>}
          {selectedId && !selected && (
            <p className="waiting">
              The order exists. The projection has not seen it yet — it is
              waiting on the relay to publish OrderCreated.
            </p>
          )}
          {selected && <Detail order={selected} />}
        </section>
      </div>
    </main>
  );
}

function Detail({ order }: { order: Order }) {
  return (
    <>
      <ul className="items">
        {order.items.map((item) => (
          <li key={item.sku}>
            {item.quantity} × {item.name} — {brl(item.unit_cents)}
          </li>
        ))}
        <li className="total">
          {order.customer_id} — {brl(order.total_cents)}
        </li>
      </ul>

      <ul className="steps">
        {STEPS.map((step, i) => (
          <li
            key={step.label}
            className={stepClass(order.status, i)}
            aria-current={step.statuses.includes(order.status)}
          >
            {step.label}
          </li>
        ))}
      </ul>

      <ul className="timeline">
        {(order.timeline ?? []).map((entry) => (
          <li key={entry.occurred_at + entry.event_type}>
            <span className="when">{clock(entry.occurred_at)}</span>
            <span>{entry.event_type}</span>
            <span className="detail">{entry.detail}</span>
          </li>
        ))}
      </ul>

      {!isTerminal(order.status) && (
        <p className="waiting">Waiting for the next event…</p>
      )}
    </>
  );
}

function StatusPill({ status }: { status: Status }) {
  const tone = isFailure(status)
    ? "bad"
    : status === "COMPLETED"
      ? "ok"
      : "pending";
  return <span className={`pill ${tone}`}>{status}</span>;
}

function stepClass(status: Status, step: number): string {
  if (!reachedStep(status, step)) return "";
  return isFailure(status) && step >= STEPS.length - 1 ? "failed" : "done";
}

// poll runs read every POLL_MS until the effect is torn down. It returns the
// cleanup an effect expects, and drops results that arrive after teardown.
function poll<T>(
  read: () => Promise<T>,
  onValue: (value: T) => void,
  onError: (message: string) => void,
): () => void {
  let live = true;
  const once = async () => {
    try {
      const value = await read();
      if (live) {
        onValue(value);
        onError("");
      }
    } catch (e) {
      if (live) onError(String(e));
    }
  };
  void once();
  const timer = setInterval(once, POLL_MS);
  return () => {
    live = false;
    clearInterval(timer);
  };
}
