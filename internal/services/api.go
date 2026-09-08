package services

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/develogo/kafka-demo/internal/console"
	"github.com/develogo/kafka-demo/internal/events"
	"github.com/develogo/kafka-demo/internal/outbox"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// recentOrders is how many the list endpoint returns.
const recentOrders = 20

// shutdownGrace is how long in-flight requests get once the process is
// interrupted.
const shutdownGrace = 5 * time.Second

// orderView is what the front end sees. It is assembled from the projection,
// never from the other services' tables.
type orderView struct {
	OrderID    string          `json:"order_id"`
	CustomerID string          `json:"customer_id"`
	Items      []events.Item   `json:"items"`
	TotalCents int64           `json:"total_cents"`
	Status     events.Status   `json:"status"`
	PlacedAt   time.Time       `json:"placed_at"`
	UpdatedAt  time.Time       `json:"updated_at"`
	Timeline   []timelineEntry `json:"timeline,omitempty"`
}

type timelineEntry struct {
	EventType  events.Type `json:"event_type"`
	Detail     string      `json:"detail"`
	OccurredAt time.Time   `json:"occurred_at"`
}

// serveAPI is the front end's only entry point. Writes go to orders.orders and
// the outbox in one transaction; reads come from the projection, which
// order-service may read and may not write.
func serveAPI(ctx context.Context, addr string, pool *pgxpool.Pool, log *console.Logger) error {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /orders", func(w http.ResponseWriter, req *http.Request) {
		placeOrder(w, req, pool, log)
	})
	mux.HandleFunc("GET /orders", func(w http.ResponseWriter, req *http.Request) {
		listOrders(w, req, pool)
	})
	mux.HandleFunc("GET /orders/{id}", func(w http.ResponseWriter, req *http.Request) {
		showOrder(w, req, pool)
	})

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdown, cancel := context.WithTimeout(context.WithoutCancel(ctx), shutdownGrace)
		defer cancel()
		_ = srv.Shutdown(shutdown)
	}()

	log.Note("API on http://%s", addr)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve api on %s: %w", addr, err)
	}
	return nil
}

// placeOrder takes an order and answers before anything has been published:
// the order exists, and the event will follow once the relay gets to it. That
// gap is the point, and the front sees it as a status that is not there yet.
func placeOrder(w http.ResponseWriter, req *http.Request, pool *pgxpool.Pool, log *console.Logger) {
	var body struct {
		Kind string `json:"kind"`
	}
	// An empty body is a standard order, so the front can post nothing at all.
	if err := json.NewDecoder(req.Body).Decode(&body); err != nil && !errors.Is(err, io.EOF) {
		fail(w, http.StatusBadRequest, "malformed body")
		return
	}
	expensive := body.Kind == "expensive"
	if body.Kind != "" && body.Kind != "standard" && !expensive {
		fail(w, http.StatusBadRequest, `kind must be "standard" or "expensive"`)
		return
	}

	ctx := req.Context()
	order := buildOrder(expensive)
	id := events.NewOrderID()

	env, err := events.New(events.OrderCreated, id, order)
	if err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	items, err := json.Marshal(order.Items)
	if err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `
        INSERT INTO orders.orders (id, customer_id, items, total_cents, status)
        VALUES ($1, $2, $3, $4, $5)`,
		id, order.CustomerID, items, order.TotalCents, string(events.StatusPlaced)); err != nil {
		fail(w, http.StatusInternalServerError, fmt.Sprintf("place order: %v", err))
		return
	}
	if err := outbox.Insert(ctx, tx, ordersSchema, events.TopicOrders, env); err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := tx.Commit(ctx); err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	log.Outboxed(events.TopicOrders, env, events.BRL(order.TotalCents))

	respond(w, http.StatusAccepted, map[string]string{
		"order_id": id,
		"status":   string(events.StatusPlaced),
	})
}

func listOrders(w http.ResponseWriter, req *http.Request, pool *pgxpool.Pool) {
	rows, err := pool.Query(req.Context(), `
        SELECT order_id, customer_id, items, total_cents, status, placed_at, updated_at
          FROM projection.order_status
         ORDER BY placed_at DESC
         LIMIT $1`, recentOrders)
	if err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer rows.Close()

	views := []orderView{}
	for rows.Next() {
		view, err := scanOrder(rows)
		if err != nil {
			fail(w, http.StatusInternalServerError, err.Error())
			return
		}
		views = append(views, view)
	}
	if err := rows.Err(); err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	respond(w, http.StatusOK, views)
}

func showOrder(w http.ResponseWriter, req *http.Request, pool *pgxpool.Pool) {
	id, err := uuid.Parse(req.PathValue("id"))
	if err != nil {
		fail(w, http.StatusBadRequest, "not an order id")
		return
	}
	ctx := req.Context()

	row, err := pool.Query(ctx, `
        SELECT order_id, customer_id, items, total_cents, status, placed_at, updated_at
          FROM projection.order_status
         WHERE order_id = $1`, id)
	if err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer row.Close()
	if !row.Next() {
		if err := row.Err(); err != nil {
			fail(w, http.StatusInternalServerError, err.Error())
			return
		}
		// The order may well exist in orders.orders already: the projection is
		// eventually consistent, and this is what that looks like.
		fail(w, http.StatusNotFound, "the projection has not seen this order yet")
		return
	}
	view, err := scanOrder(row)
	if err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	row.Close()

	entries, err := pool.Query(ctx, `
        SELECT event_type, detail, occurred_at
          FROM projection.order_timeline
         WHERE order_id = $1
         ORDER BY occurred_at, event_id`, id)
	if err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer entries.Close()
	for entries.Next() {
		var e timelineEntry
		if err := entries.Scan(&e.EventType, &e.Detail, &e.OccurredAt); err != nil {
			fail(w, http.StatusInternalServerError, err.Error())
			return
		}
		view.Timeline = append(view.Timeline, e)
	}
	if err := entries.Err(); err != nil {
		fail(w, http.StatusInternalServerError, err.Error())
		return
	}
	respond(w, http.StatusOK, view)
}

func scanOrder(rows pgx.Rows) (orderView, error) {
	var view orderView
	var items []byte
	if err := rows.Scan(&view.OrderID, &view.CustomerID, &items, &view.TotalCents,
		&view.Status, &view.PlacedAt, &view.UpdatedAt); err != nil {
		return view, err
	}
	if err := json.Unmarshal(items, &view.Items); err != nil {
		return view, fmt.Errorf("decode items of %s: %w", view.OrderID, err)
	}
	return view, nil
}

func respond(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func fail(w http.ResponseWriter, status int, message string) {
	respond(w, status, map[string]string{"error": message})
}
