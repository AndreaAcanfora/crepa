package history

import (
	"sort"
	"time"
)

// Report is the outcome of checking a history against the final state.
type Report struct {
	Attempted int
	Acked     int
	Failed    int
	Unknown   int

	// Lost are acknowledged writes missing from the final state.
	Lost []Op
	// Unexpected are writes the server refused but that are present anyway.
	Unexpected []Op
	// UnknownPresent counts unknown writes that did make it.
	UnknownPresent int

	// LostByNode counts lost writes by the node that acknowledged them.
	LostByNode map[string]int
	// LostInFault counts lost writes that started during a fault window.
	LostInFault int
	// Windows are the fault windows of the run.
	Windows []Window

	// Divergent lists nodes whose final contents differ from the primary's,
	// with how many values each is missing and has extra.
	Divergent []Divergence
}

// Divergence describes a node that did not converge to the primary's state.
type Divergence struct {
	Node           string
	Missing, Extra int
}

// Check compares the ops with final, the set of values the primary holds at
// the end. nodes optionally maps every other node to the values it holds.
func Check(h *History, final map[int64]bool, nodes map[string]map[int64]bool) Report {
	ops := h.Ops()
	end := time.Now()
	if len(ops) > 0 {
		end = ops[len(ops)-1].End
	}
	r := Report{
		LostByNode: map[string]int{},
		Windows:    FaultWindows(h.Events(), end),
	}
	for _, op := range ops {
		r.Attempted++
		present := final[op.Value]
		switch op.Result {
		case Ack:
			r.Acked++
			if !present {
				r.Lost = append(r.Lost, op)
				r.LostByNode[op.Node]++
				if inWindows(op.Start, r.Windows) {
					r.LostInFault++
				}
			}
		case Fail:
			r.Failed++
			if present {
				r.Unexpected = append(r.Unexpected, op)
			}
		case Unknown:
			r.Unknown++
			if present {
				r.UnknownPresent++
			}
		}
	}
	names := make([]string, 0, len(nodes))
	for n := range nodes {
		names = append(names, n)
	}
	sort.Strings(names)
	for _, n := range names {
		d := Divergence{Node: n}
		for v := range final {
			if !nodes[n][v] {
				d.Missing++
			}
		}
		for v := range nodes[n] {
			if !final[v] {
				d.Extra++
			}
		}
		if d.Missing > 0 || d.Extra > 0 {
			r.Divergent = append(r.Divergent, d)
		}
	}
	return r
}

func inWindows(t time.Time, ws []Window) bool {
	for _, w := range ws {
		if !t.Before(w.Start) && t.Before(w.End) {
			return true
		}
	}
	return false
}

// Bucket is one slice of the timeline.
type Bucket struct {
	Acked, Lost, Failed, Unknown int
	Fault                        bool
	Primary                      string // node that acked the most writes in the bucket
}

// Timeline splits the run into buckets of width step, measured from h.Start.
func Timeline(h *History, r Report, step time.Duration) []Bucket {
	ops := h.Ops()
	if len(ops) == 0 {
		return nil
	}
	last := ops[len(ops)-1].Start
	n := int(last.Sub(h.Start)/step) + 1
	out := make([]Bucket, n)
	perNode := make([]map[string]int, n)
	lost := map[int64]bool{}
	for _, op := range r.Lost {
		lost[op.Value] = true
	}
	for _, op := range ops {
		i := int(op.Start.Sub(h.Start) / step)
		if i < 0 || i >= n {
			continue
		}
		b := &out[i]
		switch op.Result {
		case Ack:
			b.Acked++
			if lost[op.Value] {
				b.Lost++
			}
			if perNode[i] == nil {
				perNode[i] = map[string]int{}
			}
			perNode[i][op.Node]++
		case Fail:
			b.Failed++
		case Unknown:
			b.Unknown++
		}
	}
	for i := range out {
		mid := h.Start.Add(time.Duration(i)*step + step/2)
		out[i].Fault = inWindows(mid, r.Windows)
		best := 0
		for node, c := range perNode[i] {
			if c > best || (c == best && node < out[i].Primary) {
				best, out[i].Primary = c, node
			}
		}
	}
	return out
}
