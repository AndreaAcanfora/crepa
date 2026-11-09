package history

import (
	"testing"
	"time"
)

func TestCheck(t *testing.T) {
	h := New()
	t0 := h.Start
	at := func(s float64) time.Time { return t0.Add(time.Duration(s * float64(time.Second))) }
	add := func(v int64, s float64, node string, res Result) {
		h.Add(Op{Value: v, Node: node, Start: at(s), End: at(s + 0.01), Result: res})
	}
	add(1, 0.5, "redis-1", Ack)
	add(2, 1.5, "redis-1", Ack) // lost, during the fault
	add(3, 1.6, "redis-1", Unknown)
	add(4, 2.5, "redis-2", Fail) // refused, but present: unexpected
	add(5, 2.6, "redis-2", Ack)
	add(6, 3.5, "redis-2", Unknown) // unknown, present
	h.mu.Lock()
	h.events = []Event{{Time: at(1), Kind: "fault"}, {Time: at(2), Kind: "heal"}}
	h.mu.Unlock()

	final := map[int64]bool{1: true, 4: true, 5: true, 6: true}
	nodes := map[string]map[int64]bool{
		"redis-2": {1: true, 4: true, 5: true, 6: true},
		"redis-3": {1: true, 4: true, 2: true},
	}
	r := Check(h, final, nodes)

	if r.Attempted != 6 || r.Acked != 3 || r.Failed != 1 || r.Unknown != 2 {
		t.Fatalf("counts = %+v", r)
	}
	if len(r.Lost) != 1 || r.Lost[0].Value != 2 || r.LostByNode["redis-1"] != 1 || r.LostInFault != 1 {
		t.Fatalf("lost = %+v by node %v in fault %d", r.Lost, r.LostByNode, r.LostInFault)
	}
	if len(r.Unexpected) != 1 || r.Unexpected[0].Value != 4 {
		t.Fatalf("unexpected = %+v", r.Unexpected)
	}
	if r.UnknownPresent != 1 {
		t.Fatalf("unknown present = %d", r.UnknownPresent)
	}
	if len(r.Divergent) != 1 || r.Divergent[0] != (Divergence{Node: "redis-3", Missing: 2, Extra: 1}) {
		t.Fatalf("divergent = %+v", r.Divergent)
	}

	tl := Timeline(h, r, time.Second)
	if len(tl) != 4 {
		t.Fatalf("timeline has %d buckets", len(tl))
	}
	if tl[0].Fault || !tl[1].Fault || tl[2].Fault {
		t.Fatalf("fault buckets = %+v", tl)
	}
	if tl[1].Acked != 1 || tl[1].Lost != 1 || tl[1].Unknown != 1 || tl[1].Primary != "redis-1" {
		t.Fatalf("bucket 1 = %+v", tl[1])
	}
	if tl[2].Primary != "redis-2" || tl[2].Failed != 1 {
		t.Fatalf("bucket 2 = %+v", tl[2])
	}
}

func TestFaultWindowsUnhealed(t *testing.T) {
	t0 := time.Now()
	ws := FaultWindows([]Event{
		{Time: t0, Kind: "fault", Text: "a"},
		{Time: t0.Add(time.Second), Kind: "heal"},
		{Time: t0.Add(2 * time.Second), Kind: "fault", Text: "b"},
	}, t0.Add(5*time.Second))
	if len(ws) != 2 || ws[1].Text != "b" || !ws[1].End.Equal(t0.Add(5*time.Second)) {
		t.Fatalf("windows = %+v", ws)
	}
}
