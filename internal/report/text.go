// Package report renders a checked run for the terminal and as an HTML page.
package report

import (
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/AndreaAcanfora/crepa/internal/history"
)

// Run is everything the renderers need.
type Run struct {
	Title   string // e.g. "redis-sentinel: 3 nodes, 3 sentinels"
	Setup   string // e.g. "200 writes/s from 5 clients"
	Start   time.Time
	Step    time.Duration
	Events  []history.Event
	Report  history.Report
	Buckets []history.Bucket
	Nodes   []string // node names, in legend order
	Explain []string // workload-specific lines explaining the loss
}

var bars = []rune(" ▁▂▃▄▅▆▇█")

func bar(v, max int) rune {
	if v <= 0 || max <= 0 {
		return ' '
	}
	i := 1 + v*(len(bars)-2)/max
	if i >= len(bars) {
		i = len(bars) - 1
	}
	return bars[i]
}

// Commas formats n with thousands separators.
func Commas(n int) string {
	s := strconv.Itoa(n)
	if n < 0 {
		return "-" + Commas(-n)
	}
	for i := len(s) - 3; i > 0; i -= 3 {
		s = s[:i] + "," + s[i:]
	}
	return s
}

func secs(d time.Duration) string { return fmt.Sprintf("%5.1fs", d.Seconds()) }

// Text writes the terminal report.
func Text(w io.Writer, run Run) {
	r := run.Report
	fmt.Fprintf(w, "\ntimeline (1 column = %v)\n", run.Step)
	n := len(run.Buckets)
	axis := []rune(strings.Repeat(" ", n+8))
	for i := 0; i < n; i += 10 {
		label := []rune(fmt.Sprintf("%v", time.Duration(i)*run.Step))
		for j, ch := range label {
			if i+j < n+8 {
				axis[i+j] = ch
			}
		}
	}
	fmt.Fprintf(w, "  %-8s %s\n", "", strings.TrimRight(string(axis), " "))
	max := 0
	for _, b := range run.Buckets {
		if b.Acked > max {
			max = b.Acked
		}
	}
	row := func(name string, f func(b history.Bucket) rune) {
		var sb strings.Builder
		for _, b := range run.Buckets {
			sb.WriteRune(f(b))
		}
		fmt.Fprintf(w, "  %-8s %s\n", name, strings.TrimRight(sb.String(), " "))
	}
	row("fault", func(b history.Bucket) rune {
		if b.Fault {
			return '▓'
		}
		return ' '
	})
	row("acked", func(b history.Bucket) rune { return bar(b.Acked, max) })
	row("lost", func(b history.Bucket) rune { return bar(b.Lost, max) })
	row("errors", func(b history.Bucket) rune {
		if b.Failed+b.Unknown > 0 {
			return '·'
		}
		return ' '
	})
	sym := map[string]rune{}
	var legend []string
	for i, name := range run.Nodes {
		s := rune('1' + i)
		if i >= 9 {
			s = rune('a' + i - 9)
		}
		sym[name] = s
		legend = append(legend, fmt.Sprintf("%c=%s", s, name))
	}
	row("acked by", func(b history.Bucket) rune {
		if s, ok := sym[b.Primary]; ok {
			return s
		}
		return ' '
	})
	fmt.Fprintf(w, "  %-8s %s\n\n", "", strings.Join(legend, "  "))

	if len(r.Lost) == 0 {
		fmt.Fprintf(w, "✓ NO LOST WRITES  all %s acknowledged writes are in the final state\n", Commas(r.Acked))
	} else {
		fmt.Fprintf(w, "✗ LOST %s of %s acknowledged writes (%.1f%%)\n",
			Commas(len(r.Lost)), Commas(r.Acked), 100*float64(len(r.Lost))/float64(r.Acked))
		for _, line := range lostByNode(r) {
			fmt.Fprintf(w, "  %s\n", line)
		}
		for _, line := range run.Explain {
			fmt.Fprintf(w, "  %s\n", line)
		}
		first, last := r.Lost[0], r.Lost[len(r.Lost)-1]
		fmt.Fprintf(w, "  first lost write #%d at %s, last #%d at %s\n",
			first.Value, strings.TrimSpace(secs(first.Start.Sub(run.Start))),
			last.Value, strings.TrimSpace(secs(last.Start.Sub(run.Start))))
	}
	fmt.Fprintf(w, "  writes: %s attempted, %s acknowledged, %s refused, %s unknown (%s of them made it)\n",
		Commas(r.Attempted), Commas(r.Acked), Commas(r.Failed), Commas(r.Unknown), Commas(r.UnknownPresent))
	if len(r.Unexpected) > 0 {
		fmt.Fprintf(w, "  ✗ %s writes the server refused are in the final state anyway (first #%d)\n",
			Commas(len(r.Unexpected)), r.Unexpected[0].Value)
	}
	for _, d := range r.Divergent {
		fmt.Fprintf(w, "  ✗ %s never caught up: missing %s values, %s extra\n", d.Node, Commas(d.Missing), Commas(d.Extra))
	}
}

func lostByNode(r history.Report) []string {
	names := make([]string, 0, len(r.LostByNode))
	for n := range r.LostByNode {
		names = append(names, n)
	}
	sort.Slice(names, func(i, j int) bool { return r.LostByNode[names[i]] > r.LostByNode[names[j]] })
	var out []string
	for _, n := range names {
		out = append(out, fmt.Sprintf("%s acknowledged by %s", Commas(r.LostByNode[n]), n))
	}
	if r.LostInFault > 0 {
		out = append(out, fmt.Sprintf("%s were written during the fault", Commas(r.LostInFault)))
	}
	return out
}

// Log prints one timestamped progress line.
func Log(w io.Writer, start time.Time, format string, args ...any) {
	fmt.Fprintf(w, "%s  %s\n", secs(time.Since(start)), fmt.Sprintf(format, args...))
}
