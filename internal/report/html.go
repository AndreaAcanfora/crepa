package report

import (
	"fmt"
	"html/template"
	"io"
	"sort"
	"strings"
	"time"
)

// Ranges compresses sorted values into "a–b" runs.
func Ranges(vals []int64, limit int) (out []string, more int) {
	for i := 0; i < len(vals); {
		j := i
		for j+1 < len(vals) && vals[j+1] == vals[j]+1 {
			j++
		}
		if len(out) == limit {
			more++
		} else if i == j {
			out = append(out, fmt.Sprintf("#%d", vals[i]))
		} else {
			out = append(out, fmt.Sprintf("#%d–#%d", vals[i], vals[j]))
		}
		i = j + 1
	}
	return out, more
}

type chart struct {
	W, H       float64
	Bars       []template.HTML
	Faults     []template.HTML
	Markers    []template.HTML
	Ticks      []template.HTML
	Max        int
	PlotBottom float64
}

func buildChart(run Run) chart {
	const w, h, top, bottom, left = 960.0, 300.0, 30.0, 40.0, 50.0
	c := chart{W: w, H: h, PlotBottom: h - bottom}
	n := len(run.Buckets)
	if n == 0 {
		return c
	}
	for _, b := range run.Buckets {
		if t := b.Acked + b.Failed + b.Unknown; t > c.Max {
			c.Max = t
		}
	}
	if c.Max == 0 {
		c.Max = 1
	}
	bw := (w - left - 10) / float64(n)
	plotH := h - top - bottom
	x := func(i float64) float64 { return left + i*bw }
	scale := func(v int) float64 { return float64(v) / float64(c.Max) * plotH }
	for _, win := range run.Report.Windows {
		x0 := x(float64(win.Start.Sub(run.Start)) / float64(run.Step))
		x1 := x(float64(win.End.Sub(run.Start)) / float64(run.Step))
		c.Faults = append(c.Faults, template.HTML(fmt.Sprintf(
			`<rect class="fault" x="%.1f" y="%.1f" width="%.1f" height="%.1f"><title>%s</title></rect>`,
			x0, top, x1-x0, plotH, template.HTMLEscapeString(win.Text))))
	}
	for i, b := range run.Buckets {
		y := h - bottom
		segs := []struct {
			v     int
			class string
			label string
		}{
			{b.Acked - b.Lost, "kept", "acknowledged and kept"},
			{b.Lost, "lost", "acknowledged, then lost"},
			{b.Unknown, "unknown", "unknown (timeout or broken connection)"},
			{b.Failed, "failed", "refused by the server"},
		}
		for _, s := range segs {
			if s.v == 0 {
				continue
			}
			hh := scale(s.v)
			y -= hh
			c.Bars = append(c.Bars, template.HTML(fmt.Sprintf(
				`<rect class="%s" x="%.1f" y="%.1f" width="%.1f" height="%.1f"><title>%v–%v: %d %s</title></rect>`,
				s.class, x(float64(i))+0.5, y, max(bw-1, 0.5), hh,
				time.Duration(i)*run.Step, time.Duration(i+1)*run.Step, s.v, s.label)))
		}
	}
	every := 10
	for n/every > 12 {
		every *= 2
	}
	for i := 0; i <= n; i += every {
		c.Ticks = append(c.Ticks, template.HTML(fmt.Sprintf(
			`<text class="tick" x="%.1f" y="%.1f">%v</text>`, x(float64(i)), h-bottom+18, time.Duration(i)*run.Step)))
	}
	row := 0
	for _, e := range run.Events {
		if e.Kind != "primary" && e.Kind != "fault" && e.Kind != "heal" {
			continue
		}
		xe := x(float64(e.Time.Sub(run.Start)) / float64(run.Step))
		label := e.Text
		if e.Kind == "heal" {
			label = "healed"
		}
		c.Markers = append(c.Markers, template.HTML(fmt.Sprintf(
			`<line class="marker" x1="%.1f" x2="%.1f" y1="%.1f" y2="%.1f"/><text class="mlabel" x="%.1f" y="%.1f">%s</text>`,
			xe, xe, top-4, h-bottom, xe+4, 12+float64(row%2)*13, template.HTMLEscapeString(label))))
		row++
	}
	return c
}

var page = template.Must(template.New("page").Funcs(template.FuncMap{"commas": Commas}).Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>crepa report</title>
<style>
:root{--bg:#fafaf9;--fg:#1c1917;--muted:#78716c;--card:#fff;--line:#e7e5e4;--kept:#16a34a;--lost:#dc2626;--unknown:#a8a29e;--failed:#f59e0b;--fault:rgba(220,38,38,.08)}
@media (prefers-color-scheme:dark){:root{--bg:#0c0a09;--fg:#f5f5f4;--muted:#a8a29e;--card:#1c1917;--line:#292524;--kept:#22c55e;--lost:#f87171;--unknown:#57534e;--failed:#fbbf24;--fault:rgba(248,113,113,.12)}}
body{margin:0;background:var(--bg);color:var(--fg);font:15px/1.5 ui-sans-serif,system-ui,sans-serif}
main{max-width:1000px;margin:0 auto;padding:24px 16px}
h1{font-size:22px;margin:0 0 4px}.muted{color:var(--muted)}
.verdict{font-size:28px;font-weight:700;margin:20px 0 4px}.bad{color:var(--lost)}.good{color:var(--kept)}
.card{background:var(--card);border:1px solid var(--line);border-radius:10px;padding:16px;margin:16px 0;overflow-x:auto}
svg{width:100%;height:auto;min-width:600px}
.kept{fill:var(--kept)}.lost{fill:var(--lost)}.unknown{fill:var(--unknown)}.failed{fill:var(--failed)}.fault{fill:var(--fault)}
.marker{stroke:var(--muted);stroke-dasharray:3 3}.mlabel,.tick{fill:var(--muted);font-size:11px}
.legend span{display:inline-block;margin-right:14px}.legend i{display:inline-block;width:10px;height:10px;border-radius:2px;margin-right:5px}
table{border-collapse:collapse}td{padding:2px 14px 2px 0}code{font:13px ui-monospace,monospace}
ul{padding-left:20px}
</style></head><body><main>
<h1>{{.Run.Title}}</h1>
<div class="muted">{{.Run.Setup}}</div>
{{with .Run.Report}}
{{if .Lost}}<div class="verdict bad">Lost {{commas (len .Lost)}} of {{commas .Acked}} acknowledged writes</div>
{{else}}<div class="verdict good">No acknowledged write was lost</div>{{end}}
{{end}}
{{range .Run.Explain}}<div>{{.}}</div>{{end}}
<div class="card">
<div class="legend muted"><span><i style="background:var(--kept)"></i>acknowledged, kept</span><span><i style="background:var(--lost)"></i>acknowledged, lost</span><span><i style="background:var(--unknown)"></i>unknown</span><span><i style="background:var(--failed)"></i>refused</span><span><i style="background:var(--fault)"></i>network fault</span></div>
<svg viewBox="0 0 {{.Chart.W}} {{.Chart.H}}" role="img" aria-label="writes per {{.Run.Step}}">
{{range .Chart.Faults}}{{.}}{{end}}{{range .Chart.Bars}}{{.}}{{end}}{{range .Chart.Markers}}{{.}}{{end}}{{range .Chart.Ticks}}{{.}}{{end}}
<text class="tick" x="4" y="34">{{.Chart.Max}}/{{.Run.Step}}</text>
</svg></div>
<div class="card"><table>
{{with .Run.Report}}
<tr><td>attempted</td><td>{{commas .Attempted}}</td></tr>
<tr><td>acknowledged</td><td>{{commas .Acked}}</td></tr>
<tr><td>refused</td><td>{{commas .Failed}}</td></tr>
<tr><td>unknown</td><td>{{commas .Unknown}} ({{commas .UnknownPresent}} made it)</td></tr>
{{range $n, $c := .LostByNode}}<tr><td>lost, acknowledged by {{$n}}</td><td>{{commas $c}}</td></tr>{{end}}
{{range .Divergent}}<tr><td>{{.Node}} never caught up</td><td>missing {{commas .Missing}}, {{commas .Extra}} extra</td></tr>{{end}}
{{end}}
</table></div>
<div class="card"><b>Events</b><table>
{{range .Events}}<tr><td class="muted"><code>{{.At}}</code></td><td>{{.Text}}</td></tr>{{end}}
</table></div>
{{if .Lost}}<div class="card"><b>Lost writes</b><p><code>{{.Lost}}</code>{{if .More}} … and {{.More}} more ranges{{end}}</p></div>{{end}}
<p class="muted">Generated by <a href="https://github.com/AndreaAcanfora/crepa">crepa</a>.</p>
</main></body></html>
`))

// HTML writes a self-contained report page.
func HTML(w io.Writer, run Run) error {
	type ev struct{ At, Text string }
	var evs []ev
	for _, e := range run.Events {
		evs = append(evs, ev{strings.TrimSpace(secs(e.Time.Sub(run.Start))), e.Text})
	}
	vals := make([]int64, len(run.Report.Lost))
	for i, op := range run.Report.Lost {
		vals[i] = op.Value
	}
	sort.Slice(vals, func(i, j int) bool { return vals[i] < vals[j] })
	ranges, more := Ranges(vals, 200)
	return page.Execute(w, map[string]any{
		"Run":    run,
		"Chart":  buildChart(run),
		"Events": evs,
		"Lost":   strings.Join(ranges, " "),
		"More":   more,
	})
}
