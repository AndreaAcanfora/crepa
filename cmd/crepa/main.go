// Command crepa cuts the network inside a docker compose project while clients
// write to it, then checks which acknowledged writes survived.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand/v2"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/AndreaAcanfora/crepa/internal/docker"
	"github.com/AndreaAcanfora/crepa/internal/history"
	"github.com/AndreaAcanfora/crepa/internal/redis"
	"github.com/AndreaAcanfora/crepa/internal/report"
	"github.com/AndreaAcanfora/crepa/internal/resp"
)

var version = "dev"

const usage = `crepa cuts the network inside a docker compose project while clients write to
it, then tells you which acknowledged writes the database lost.

usage: crepa redis [flags]

  crepa starts the compose project under its own name (it never touches a
  project you already run), waits until Sentinel and the replicas are healthy,
  writes unique numbers to a set on whatever node Sentinel calls primary,
  cuts the network as --nemesis says, heals it, and reads the set back.

  Exit status: 0 no acknowledged write was lost, 1 writes were lost, 2 error.

flags:
`

type stringList []string

func (s *stringList) String() string { return strings.Join(*s, ",") }
func (s *stringList) Set(v string) error {
	for _, x := range strings.Split(v, ",") {
		if x = strings.TrimSpace(x); x != "" {
			*s = append(*s, x)
		}
	}
	return nil
}

type config struct {
	files                stringList
	project              string
	nodes, sentinels     stringList
	port, sentinelPort   int
	masterName           string
	nemesis              string
	warmup, fault, recov time.Duration
	cycles               int
	workload             redis.Workload
	reportPath           string
	keep, noUp           bool
	seed                 uint64
}

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) > 0 && (args[0] == "version" || args[0] == "--version") {
		fmt.Println("crepa", version)
		return 0
	}
	if len(args) == 0 || args[0] != "redis" {
		fmt.Fprint(os.Stderr, usage)
		fmt.Fprintln(os.Stderr, "  run \"crepa redis -h\" for the flags")
		return 2
	}
	var cfg config
	fs := flag.NewFlagSet("crepa redis", flag.ContinueOnError)
	fs.Usage = func() { fmt.Fprint(os.Stderr, usage); fs.PrintDefaults() }
	fs.Var(&cfg.files, "f", "compose file (repeatable; default: compose.yaml or docker-compose.yml here)")
	fs.StringVar(&cfg.project, "project", "", "compose project name crepa uses (default: crepa-<directory>)")
	fs.Var(&cfg.nodes, "nodes", "services that run Redis data nodes (default: detected)")
	fs.Var(&cfg.sentinels, "sentinels", "services that run Sentinel (default: detected)")
	fs.IntVar(&cfg.port, "port", 6379, "Redis port inside the containers")
	fs.IntVar(&cfg.sentinelPort, "sentinel-port", 26379, "Sentinel port inside the containers")
	fs.StringVar(&cfg.masterName, "master-name", "", "Sentinel master name (default: asked to Sentinel)")
	fs.StringVar(&cfg.nemesis, "nemesis", "isolate-primary", "fault to inject: isolate-primary, halves or none")
	fs.DurationVar(&cfg.warmup, "warmup", 10*time.Second, "healthy writing before the first fault")
	fs.DurationVar(&cfg.fault, "fault", 20*time.Second, "how long each fault lasts")
	fs.DurationVar(&cfg.recov, "recover", 20*time.Second, "healthy writing after each heal")
	fs.IntVar(&cfg.cycles, "cycles", 1, "fault and recover cycles")
	fs.IntVar(&cfg.workload.Clients, "clients", 5, "concurrent clients")
	fs.Float64Var(&cfg.workload.Rate, "rate", 200, "writes per second, all clients together")
	fs.DurationVar(&cfg.workload.Timeout, "timeout", time.Second, "timeout of each write")
	fs.IntVar(&cfg.workload.WaitReplicas, "wait-replicas", 0, "after each write, WAIT for this many replicas before counting it as acknowledged")
	fs.StringVar(&cfg.reportPath, "report", "crepa-report.html", "write an HTML report here (empty: none)")
	fs.BoolVar(&cfg.keep, "keep", false, "leave the compose project running at the end")
	fs.BoolVar(&cfg.noUp, "no-up", false, "use the project as it is running now: do not start or remove it")
	fs.Uint64Var(&cfg.seed, "seed", 0, "random seed for the halves nemesis (default: random)")
	if err := fs.Parse(args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() > 0 {
		fmt.Fprintf(os.Stderr, "crepa: unexpected argument %q\n", fs.Arg(0))
		return 2
	}
	if err := cfg.validate(); err != nil {
		fmt.Fprintln(os.Stderr, "crepa:", err)
		return 2
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	code, err := runRedis(ctx, cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "crepa:", err)
		return 2
	}
	return code
}

func (c *config) validate() error {
	if c.workload.Clients < 1 || c.workload.Rate <= 0 || c.cycles < 0 {
		return errors.New("--clients and --rate must be positive and --cycles not negative")
	}
	switch c.nemesis {
	case "isolate-primary", "halves", "none":
	default:
		return fmt.Errorf("unknown --nemesis %q (isolate-primary, halves, none)", c.nemesis)
	}
	if len(c.files) == 0 {
		for _, f := range []string{"compose.yaml", "compose.yml", "docker-compose.yml", "docker-compose.yaml"} {
			if _, err := os.Stat(f); err == nil {
				c.files = append(c.files, f)
				break
			}
		}
		if len(c.files) == 0 {
			return errors.New("no compose file here: pass one with -f")
		}
	}
	if c.project == "" {
		abs, _ := filepath.Abs(filepath.Dir(c.files[0]))
		name := strings.ToLower(filepath.Base(abs))
		name = regexp.MustCompile(`[^a-z0-9_-]+`).ReplaceAllString(name, "")
		c.project = "crepa-" + name
	}
	if c.seed == 0 {
		c.seed = rand.Uint64()
	}
	return nil
}

func runRedis(ctx context.Context, cfg config) (code int, err error) {
	out := os.Stdout
	start := time.Now()
	logf := func(format string, args ...any) { report.Log(out, start, format, args...) }
	proj := &docker.Project{Files: cfg.files, Name: cfg.project, Run: docker.CLI}
	nw := &docker.Net{Run: docker.CLI, Project: cfg.project}

	cleanup := func() {
		cctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		if err := nw.Close(cctx); err != nil {
			fmt.Fprintln(os.Stderr, "crepa: removing network helpers:", err)
		}
		if !cfg.noUp && !cfg.keep {
			logf("removing compose project %s", cfg.project)
			if err := proj.Down(cctx); err != nil {
				fmt.Fprintln(os.Stderr, "crepa:", err)
			}
		}
	}
	if !cfg.noUp {
		logf("starting compose project %s from %s", cfg.project, strings.Join(cfg.files, ", "))
		if err := proj.Up(ctx); err != nil {
			cleanup()
			return 2, err
		}
	}
	defer cleanup()

	cs, err := proj.Containers(ctx)
	if err != nil {
		return 2, err
	}
	nodes, sentinels := redis.Classify(cs, cfg.nodes, cfg.sentinels)
	if len(nodes) < 2 || len(sentinels) < 1 {
		return 2, fmt.Errorf("need at least 2 Redis nodes and 1 sentinel, found nodes [%s] and sentinels [%s]; use --nodes and --sentinels",
			redis.Names(nodes), redis.Names(sentinels))
	}
	cl := &redis.Cluster{Nodes: nodes, Sentinels: sentinels, Port: cfg.port, SentinelPort: cfg.sentinelPort,
		MasterName: cfg.masterName, Key: "crepa:set"}
	logf("nodes: %s; sentinels: %s", redis.Names(nodes), redis.Names(sentinels))

	logf("attaching network helpers")
	if err := nw.Attach(ctx, append(append([]docker.Container{}, nodes...), sentinels...)); err != nil {
		return 2, err
	}
	if cl.MasterName == "" {
		deadline := time.Now().Add(60 * time.Second)
		for {
			cl.MasterName, err = cl.DetectMasterName()
			if err == nil || time.Now().After(deadline) || ctx.Err() != nil {
				break
			}
			time.Sleep(time.Second)
		}
		if err != nil {
			return 2, fmt.Errorf("asking Sentinel for the master name: %w", err)
		}
	}
	logf("waiting for master %q to be healthy", cl.MasterName)
	primary, err := cl.WaitHealthy(ctx, 90*time.Second)
	if err != nil {
		return 2, err
	}
	safety := primarySafety(cl, primary)
	if _, err := doOnce(cl, primary, "DEL", cl.Key); err != nil {
		return 2, fmt.Errorf("clearing %s on %s: %v", cl.Key, primary.Name, err)
	}

	h := history.New()
	start = h.Start
	logf("primary is %s; writing %.0f/s from %d clients%s", primary.Name, cfg.workload.Rate, cfg.workload.Clients, waitNote(cfg.workload))
	wctx, stopWriting := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { cl.Run(wctx, cfg.workload, h); close(done) }()
	go cl.Watch(wctx, h, primary)
	stopped := false
	stopAll := func() {
		if !stopped {
			stopWriting()
			<-done
			stopped = true
		}
	}
	defer stopAll()

	rng := rand.New(rand.NewPCG(cfg.seed, cfg.seed^0x9e3779b97f4a7c15))
	sleep := func(d time.Duration) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(d):
			return nil
		}
	}
	if err := sleep(cfg.warmup); err != nil {
		return 2, err
	}
	all := append(append([]docker.Container{}, nodes...), sentinels...)
	for i := 0; i < cfg.cycles && cfg.nemesis != "none"; i++ {
		groups, text, err := nemesis(cfg.nemesis, cl, all, rng)
		if err != nil {
			return 2, err
		}
		if err := nw.Partition(ctx, groups); err != nil {
			return 2, err
		}
		h.Event("fault", text)
		logf("✂ %s", text)
		if err := sleep(cfg.fault); err != nil {
			nw.Heal(context.Background())
			return 2, err
		}
		if err := nw.Heal(ctx); err != nil {
			return 2, err
		}
		h.Event("heal", "network healed")
		logf("network healed")
		if err := sleep(cfg.recov); err != nil {
			return 2, err
		}
	}
	if cfg.nemesis == "none" {
		if err := sleep(cfg.fault + cfg.recov); err != nil {
			return 2, err
		}
	}
	stopAll()
	for _, e := range h.Events() {
		if e.Kind == "primary" {
			logf("(at %.1fs) %s", e.Time.Sub(start).Seconds(), e.Text)
		}
	}
	logf("stopped writing; waiting for the nodes to agree")
	final, err := cl.WaitHealthy(ctx, 60*time.Second)
	if err != nil {
		logf("warning: %v", err)
		if final, err = cl.Primary(); err != nil {
			return 2, err
		}
	}
	set, others, err := cl.Converge(ctx, final, 30*time.Second)
	if err != nil {
		return 2, err
	}
	logf("read %s values back from %s", report.Commas(len(set)), final.Name)

	rep := history.Check(h, set, others)
	step := time.Second
	if total := time.Since(start); total > 100*time.Second {
		step = (total/100 + time.Second - 1).Truncate(time.Second)
	}
	var names []string
	for _, n := range nodes {
		names = append(names, n.Name)
	}
	r := report.Run{
		Title:   fmt.Sprintf("crepa: Redis Sentinel, %d nodes and %d sentinels, nemesis %s", len(nodes), len(sentinels), cfg.nemesis),
		Setup:   fmt.Sprintf("%.0f writes/s from %d clients%s, %s", cfg.workload.Rate, cfg.workload.Clients, waitNote(cfg.workload), safety),
		Start:   start,
		Step:    step,
		Events:  h.Events(),
		Report:  rep,
		Buckets: history.Timeline(h, rep, step),
		Nodes:   names,
		Explain: explain(rep, h.Events(), primary, safety, cfg.workload),
	}
	report.Text(out, r)
	if cfg.reportPath != "" {
		f, err := os.Create(cfg.reportPath)
		if err != nil {
			return 2, err
		}
		if err := report.HTML(f, r); err != nil {
			f.Close()
			return 2, err
		}
		if err := f.Close(); err != nil {
			return 2, err
		}
		fmt.Fprintf(out, "\nreport: %s\n", cfg.reportPath)
	}
	if len(rep.Lost) > 0 || len(rep.Unexpected) > 0 {
		return 1, nil
	}
	return 0, nil
}

func waitNote(w redis.Workload) string {
	if w.WaitReplicas > 0 {
		return fmt.Sprintf(" (each followed by WAIT %d)", w.WaitReplicas)
	}
	return ""
}

func doOnce(cl *redis.Cluster, n docker.Container, args ...string) (any, error) {
	c, err := resp.Dial(fmt.Sprintf("%s:%d", n.IP(), cl.Port), 2*time.Second)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	return c.Do(2*time.Second, args...)
}

// primarySafety describes the primary's min-replicas settings.
func primarySafety(cl *redis.Cluster, p docker.Container) string {
	get := func(name string) string {
		v, err := doOnce(cl, p, "CONFIG", "GET", name)
		if err != nil {
			return ""
		}
		ss, err := resp.Strings(v)
		if err != nil || len(ss) != 2 {
			return ""
		}
		return ss[1]
	}
	n, lag := get("min-replicas-to-write"), get("min-replicas-max-lag")
	if n == "" || n == "0" {
		return "min-replicas-to-write 0"
	}
	return fmt.Sprintf("min-replicas-to-write %s, min-replicas-max-lag %s", n, lag)
}

// nemesis picks the groups for one fault.
func nemesis(kind string, cl *redis.Cluster, all []docker.Container, rng *rand.Rand) ([][]docker.Container, string, error) {
	switch kind {
	case "isolate-primary":
		p, err := cl.Primary()
		if err != nil {
			return nil, "", err
		}
		var rest []docker.Container
		for _, c := range all {
			if c.ID != p.ID {
				rest = append(rest, c)
			}
		}
		return [][]docker.Container{{p}, rest}, fmt.Sprintf("isolated %s (the primary) from %s", p.Name, redis.Names(rest)), nil
	case "halves":
		perm := rng.Perm(len(all))
		var a, b []docker.Container
		for i, j := range perm {
			if i < len(all)/2 {
				a = append(a, all[j])
			} else {
				b = append(b, all[j])
			}
		}
		return [][]docker.Container{a, b}, fmt.Sprintf("split the network: [%s] | [%s]", redis.Names(a), redis.Names(b)), nil
	}
	return nil, "", fmt.Errorf("unknown nemesis %q", kind)
}

// explain says why the writes were lost, for the Redis Sentinel case.
func explain(r history.Report, events []history.Event, first docker.Container, safety string, w redis.Workload) []string {
	if len(r.Lost) == 0 {
		return nil
	}
	var promoted []string
	for _, e := range events {
		if e.Kind == "primary" {
			promoted = append(promoted, e.Text)
		}
	}
	var out []string
	if r.LostByNode[first.Name] > 0 && len(promoted) > 0 {
		out = append(out,
			fmt.Sprintf("%s kept acknowledging writes while it was cut off from the replicas and the sentinels.", first.Name),
			fmt.Sprintf("%s. When the network healed, %s became a replica and threw away what it had accepted alone.", promoted[0], first.Name))
	}
	switch {
	case strings.HasPrefix(safety, "min-replicas-to-write 0") && w.WaitReplicas == 0:
		out = append(out, "hint: min-replicas-to-write 1 with a low min-replicas-max-lag makes an isolated primary refuse writes; WAIT (crepa --wait-replicas 1) makes clients wait for a replica.")
	case w.WaitReplicas == 0:
		out = append(out, "hint: min-replicas-max-lag still lets the isolated primary accept writes until the lag runs out; try crepa --wait-replicas 1.")
	}
	return out
}
