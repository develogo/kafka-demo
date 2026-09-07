// Package console renders the demo: one line per event, coloured by service,
// carrying the partition and offset the record occupies.
package console

import (
	"fmt"
	"sync"
	"time"

	"github.com/develogo/kafka-demo/internal/kafkax"
	"github.com/fatih/color"
)

const (
	arrowPublish = "->"
	arrowConsume = "<-"
	arrowSkip    = " x"
)

var styles = map[string]struct {
	icon string
	attr color.Attribute
}{
	"order-service":        {"🛒", color.FgHiCyan},
	"payment-service":      {"💳", color.FgHiGreen},
	"inventory-service":    {"📦", color.FgHiYellow},
	"shipping-service":     {"🚚", color.FgHiMagenta},
	"notification-service": {"🔔", color.FgHiBlue},
}

var (
	dim   = color.New(color.FgHiBlack)
	plain = color.New(color.FgWhite)
)

// Printer serialises the output of the five services, which in the combined
// demo all write to the same terminal from their own goroutines.
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

// Published records an event this service just wrote to a topic.
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
func (l *Logger) Skipped(r kafkax.Record) {
	l.write(dim, arrowSkip, r, "not for this service")
}

// Note prints a service-level line unattached to any record.
func (l *Logger) Note(format string, a ...any) {
	l.p.mu.Lock()
	defer l.p.mu.Unlock()
	dim.Fprintf(color.Output, "%s ", time.Now().Format("15:04:05.000"))
	l.c.Fprintf(color.Output, "%s %-21s", l.icon, l.name)
	plain.Fprintf(color.Output, "| "+format+"\n", a...)
}

func (l *Logger) write(body *color.Color, arrow string, r kafkax.Record, detail string) {
	l.p.mu.Lock()
	defer l.p.mu.Unlock()

	dim.Fprintf(color.Output, "%s ", time.Now().Format("15:04:05.000"))
	l.c.Fprintf(color.Output, "%s %-21s", l.icon, l.name)
	body.Fprintf(color.Output, "| %-9s %s %-34s p%d @%-4d",
		r.Envelope.OrderID, arrow, r.Topic+"/"+string(r.EventType), r.Partition, r.Offset)
	if detail != "" {
		dim.Fprintf(color.Output, "  %s", detail)
	}
	fmt.Fprintln(color.Output)
}

// BRL formats cents as currency. Money never travels or prints as a float.
func BRL(cents int64) string {
	sign := ""
	if cents < 0 {
		sign, cents = "-", -cents
	}
	return fmt.Sprintf("%sBRL %d.%02d", sign, cents/100, cents%100)
}
