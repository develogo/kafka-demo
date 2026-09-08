// The shape order-service serves, and the vocabulary of CONTEXT.md as the
// browser sees it.

export type Status =
  | "PLACED"
  | "APPROVED"
  | "REJECTED"
  | "RESERVED"
  | "DISPATCHED"
  | "COMPLETED"
  | "CANCELLED";

export type Item = {
  sku: string;
  name: string;
  quantity: number;
  unit_cents: number;
};

export type TimelineEntry = {
  event_type: string;
  detail: string;
  occurred_at: string;
};

export type Order = {
  order_id: string;
  customer_id: string;
  items: Item[];
  total_cents: number;
  status: Status;
  placed_at: string;
  updated_at: string;
  timeline?: TimelineEntry[];
};

// The lifecycle as a progress bar. Approved and rejected share a step, as do
// the two terminal statuses, because they are the two outcomes of one.
export const STEPS: { label: string; statuses: Status[] }[] = [
  { label: "Placed", statuses: ["PLACED"] },
  { label: "Paid", statuses: ["APPROVED", "REJECTED"] },
  { label: "Reserved", statuses: ["RESERVED"] },
  { label: "Dispatched", statuses: ["DISPATCHED"] },
  { label: "Settled", statuses: ["COMPLETED", "CANCELLED"] },
];

const FAILED: Status[] = ["REJECTED", "CANCELLED"];

export const isFailure = (status: Status) => FAILED.includes(status);

export const isTerminal = (status: Status) =>
  status === "COMPLETED" || status === "CANCELLED";

// reachedStep is how far along the bar an order got. A rejected order stops at
// "Paid" and then jumps to "Settled": it never reserves or dispatches.
export function reachedStep(status: Status, step: number): boolean {
  const at = STEPS.findIndex((s) => s.statuses.includes(status));
  if (at < 0) return false;
  if (isFailure(status) && step > at && step < STEPS.length - 1) return false;
  return step <= at;
}

export const brl = (cents: number) =>
  (cents / 100).toLocaleString("pt-BR", { style: "currency", currency: "BRL" });

export const clock = (iso: string) =>
  new Date(iso).toLocaleTimeString("pt-BR", { hour12: false }) +
  "." +
  String(new Date(iso).getMilliseconds()).padStart(3, "0");

export async function placeOrder(kind: "standard" | "expensive") {
  const res = await fetch("/api/orders", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ kind }),
  });
  if (!res.ok) throw new Error(`could not place the order (${res.status})`);
  return (await res.json()) as { order_id: string; status: Status };
}

export async function fetchOrders(): Promise<Order[]> {
  const res = await fetch("/api/orders", { cache: "no-store" });
  if (!res.ok) throw new Error(`could not load orders (${res.status})`);
  return res.json();
}

// Returns null while the projection has not caught up with the order yet,
// which is a normal state and not an error.
export async function fetchOrder(id: string): Promise<Order | null> {
  const res = await fetch(`/api/orders/${id}`, { cache: "no-store" });
  if (res.status === 404) return null;
  if (!res.ok) throw new Error(`could not load order ${id} (${res.status})`);
  return res.json();
}
