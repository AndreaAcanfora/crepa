package report

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/AndreaAcanfora/crepa/internal/history"
)

func TestRanges(t *testing.T) {
	got, more := Ranges([]int64{1, 2, 3, 5, 7, 8, 10}, 3)
	if !reflect.DeepEqual(got, []string{"#1–#3", "#5", "#7–#8"}) || more != 1 {
		t.Fatalf("ranges = %q, more %d", got, more)
	}
}

func TestCommas(t *testing.T) {
	for n, want := range map[int]string{0: "0", 999: "999", 1000: "1,000", 1234567: "1,234,567", -4200: "-4,200"} {
		if got := Commas(n); got != want {
			t.Errorf("Commas(%d) = %q, want %q", n, got, want)
		}
	}
}

func sampleRun() Run {
	h := history.New()
	for i := 0; i < 30; i++ {
		node := "redis-1"
		if i >= 20 {
			node = "redis-2"
		}
		at := h.Start.Add(time.Duration(i) * 100 * time.Millisecond)
		h.Add(history.Op{Value: int64(i), Node: node, Start: at, End: at, Result: history.Ack})
	}
	h.Event("fault", "isolated redis-1 (the primary) from redis-2")
	h.Event("heal", "network healed")
	final := map[int64]bool{}
	for i := 0; i < 30; i++ {
		if i < 10 || i >= 20 {
			final[int64(i)] = true
		}
	}
	rep := history.Check(h, final, nil)
	return Run{Title: "t", Start: h.Start, Step: time.Second, Events: h.Events(), Report: rep,
		Buckets: history.Timeline(h, rep, time.Second), Nodes: []string{"redis-1", "redis-2"}}
}

func TestText(t *testing.T) {
	var b bytes.Buffer
	Text(&b, sampleRun())
	out := b.String()
	for _, want := range []string{"✗ LOST 10 of 30 acknowledged writes (33.3%)", "10 acknowledged by redis-1", "first lost write #10", "acked by 112"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
}

func TestHTML(t *testing.T) {
	var b bytes.Buffer
	if err := HTML(&b, sampleRun()); err != nil {
		t.Fatal(err)
	}
	out := b.String()
	for _, want := range []string{"Lost 10 of 30 acknowledged writes", "#10–#19", `class="lost"`, "isolated redis-1"} {
		if !strings.Contains(out, want) {
			t.Errorf("missing %q", want)
		}
	}
}
