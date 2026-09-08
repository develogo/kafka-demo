// Package console renders the demo: one line per event, coloured by service,
// carrying the partition and offset the record occupies.
package console

import (
	"fmt"
	"sync"
	"time"

	"github.com/develogo/kafka-demo/internal/events"
	"github.com/develogo/kafka-demo/internal/kafkax"
	"github.com/fatih/color"
)

const (
	arrowPublish = "->"
	arrowConsume = "<-"
	arrowOutbox  = "=>"
	arrowSkip    = " x"
)

// nameWidth fits the longest label, which is a relay's ("inventory-service
// relay").
const nameWidth = 23

var styles = map[string]struct {
	icon string
	attr color.Attribute
}{
	"order-service":        {"🛒", color.FgHiCyan},
	"payment-service":      {"💳", color.FgHiGreen},
	"inventory-service":    {"📦", color.FgHiYellow},
	"shipping-service":     {"🚚", color.FgHiMagenta},
	"notification-service": {"🔔", color.FgHiBlue},
	"projection-service":   {"🗂️", color.FgHiRed},
}

var (
	dim   = color.New(color.FgHiBlack)
	plain = color.New(color.FgWhite)
)

// Printer serialises the output of the services, which in the combined demo
// all write to the same terminal from their own goroutines.
type Printer struct{ mu sync.Mutex }

func NewPrinter() *Printer { return &Printer{} }

// For returns the logger of one service.
func (p *Printer) For(name string) *Logger {
	style, ok := styles[name]
	if !ok {
		style = struct {
			icon string
			attr color.Attribute
		}{"❔", color.FgWhite}
	}
	return &Logger{p: p, name: name, icon: style.icon, c: color.New(style.attr)}
}

// Note prints a demo-level line that belongs to no service.
func (p *Printer) Note(format string, a ...any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	dim.Fprintf(color.Output, "%s ", time.Now().Format("15:04:05.000"))
	plain.Fprintf(color.Output, format+"\n", a...)
}

// Logger writes the lines of a single service.
type Logger struct {
	p    *Printer
	name string
	icon string
	c    *color.Color
}

// Relay returns the logger of this service's outbox relay. It keeps the
// service's colour: the two are one process, and the gap between the service's
// outbox line and the relay's publish line is the latency the pattern costs.
func (l *Logger) Relay() *Logger {
	return &Logger{p: l.p, name: l.name + " relay", icon: "📮", c: l.c}
}

// Outboxed records an event a service wrote to its outbox. It has no partition
// or offset yet: nothing has reached Kafka.
func (l *Logger) Outboxed(topic string, env events.Envelope, detail string) {
	l.p.mu.Lock()
	defer l.p.mu.Unlock()
	l.prefix()
	l.c.Fprintf(color.Output, "| %-12s %s %-34s outbox   ",
		events.ShortID(env.OrderID), arrowOutbox, topic+"/"+string(env.EventType))
	l.detail(detail)
}

// Published records an event that reached a topic. Only a relay calls this:
// services publish nothing directly.
func (l *Logger) Published(r kafkax.Record, detail string) {
	l.write(l.c, arrowPublish, r, detail)
}

// Received records an event this service consumed and acted on.
func (l *Logger) Received(r kafkax.Record, detail string) {
	l.write(l.c, arrowConsume, r, detail)
}

// Skipped records an event this service consumed and ignored. Every consumer
// of an aggregate topic sees every type on it; filtering is the application's
// job, and these greyed-out lines are the proof.
func (l *Logger) Skipped(r kafkax.Record, reason string) {
	l.write(dim, arrowSkip, r, reason)
}

// Note prints a service-level line unattached to any record.
func (l *Logger) Note(format string, a ...any) {
	l.p.mu.Lock()
	defer l.p.mu.Unlock()
	l.prefix()
	plain.Fprintf(color.Output, "| "+format+"\n", a...)
}

func (l *Logger) write(body *color.Color, arrow string, r kafkax.Record, detail string) {
	l.p.mu.Lock()
	defer l.p.mu.Unlock()
	l.prefix()
	body.Fprintf(color.Output, "| %-12s %s %-34s p%d @%-4d",
		events.ShortID(r.Envelope.OrderID), arrow, r.Topic+"/"+string(r.EventType), r.Partition, r.Offset)
	l.detail(detail)
}

// prefix writes the timestamp and the service label. The caller holds the lock.
func (l *Logger) prefix() {
	dim.Fprintf(color.Output, "%s ", time.Now().Format("15:04:05.000"))
	l.c.Fprintf(color.Output, "%s %-*s", l.icon, nameWidth, l.name)
}

// detail closes a line with its greyed-out trailing note. The caller holds the
// lock.
func (l *Logger) detail(detail string) {
	if detail != "" {
		dim.Fprintf(color.Output, "  %s", detail)
	}
	fmt.Fprintln(color.Output)
}
