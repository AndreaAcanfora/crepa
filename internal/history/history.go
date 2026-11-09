// Package history records what the clients did during a run and checks it
// against what the database holds at the end.
package history

import (
	"sort"
	"sync"
	"time"
)

// Result is what a client learned about one write.
type Result int

const (
	// Ack: the server confirmed the write.
	Ack Result = iota
	// Fail: the server refused the write (an error reply such as READONLY).
	// The write did not happen.
	Fail
	// Unknown: the connection broke or timed out. The write may or may not
	// have happened.
	Unknown
)

func (r Result) String() string {
	return [...]string{"ack", "fail", "unknown"}[r]
}

// Op is one write a client attempted.
type Op struct {
	Value  int64
	Client int
	Node   string // the node the client sent the write to
	Start  time.Time
	End    time.Time
	Result Result
	Err    string
}

// Event is something that happened to the cluster during the run.
type Event struct {
	Time time.Time
	Kind string // "fault", "heal", "primary"
	Text string
}

// History collects ops and events from many goroutines.
type History struct {
	Start time.Time
	// OnEvent, if set, is called with every event as it is recorded.
	OnEvent func(Event)

	mu     sync.Mutex
	ops    []Op
	events []Event
}

// New starts a history at the current time.
func New() *History { return &History{Start: time.Now()} }

// Add records one op.
func (h *History) Add(op Op) {
	h.mu.Lock()
	h.ops = append(h.ops, op)
	h.mu.Unlock()
}

// Event records one event at the current time.
func (h *History) Event(kind, text string) {
	e := Event{Time: time.Now(), Kind: kind, Text: text}
	h.mu.Lock()
	h.events = append(h.events, e)
	h.mu.Unlock()
	if h.OnEvent != nil {
		h.OnEvent(e)
	}
}

// Ops returns the ops sorted by start time.
func (h *History) Ops() []Op {
	h.mu.Lock()
	out := append([]Op(nil), h.ops...)
	h.mu.Unlock()
	sort.Slice(out, func(i, j int) bool { return out[i].Start.Before(out[j].Start) })
	return out
}

// Events returns the events in the order they happened.
func (h *History) Events() []Event {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]Event(nil), h.events...)
}

// Window is a stretch of the run during which a fault was active.
type Window struct {
	Start, End time.Time
	Text       string
}

// FaultWindows pairs each fault event with the heal that ended it. A fault
// that was never healed ends at end.
func FaultWindows(events []Event, end time.Time) []Window {
	var out []Window
	var open *Window
	for _, e := range events {
		switch e.Kind {
		case "fault":
			if open == nil {
				open = &Window{Start: e.Time, Text: e.Text}
			}
		case "heal":
			if open != nil {
				open.End = e.Time
				out = append(out, *open)
				open = nil
			}
		}
	}
	if open != nil {
		open.End = end
		out = append(out, *open)
	}
	return out
}
