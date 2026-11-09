// Package redis runs crepa's set workload against Redis (or Valkey) with
// Sentinel: clients add unique numbers to a set on whatever node Sentinel
// says is the primary, and at the end crepa reads the set back.
package redis

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/AndreaAcanfora/crepa/internal/docker"
	"github.com/AndreaAcanfora/crepa/internal/history"
	"github.com/AndreaAcanfora/crepa/internal/resp"
)

// Cluster is a Redis replication group watched by Sentinel.
type Cluster struct {
	Nodes        []docker.Container
	Sentinels    []docker.Container
	Port         int
	SentinelPort int
	MasterName   string
	Key          string

	byIP map[string]docker.Container
}

// Classify splits containers into data nodes and sentinels. Explicit service
// lists win; otherwise a container whose command mentions "sentinel" is a
// sentinel and one running a redis or valkey image or server is a node.
func Classify(cs []docker.Container, nodes, sentinels []string) (ns, ss []docker.Container) {
	has := func(list []string, s string) bool {
		for _, x := range list {
			if x == s {
				return true
			}
		}
		return false
	}
	for _, c := range cs {
		cmd := strings.ToLower(c.Command)
		img := strings.ToLower(c.Image)
		isRedis := strings.Contains(img, "redis") || strings.Contains(img, "valkey") ||
			strings.Contains(cmd, "redis-server") || strings.Contains(cmd, "valkey-server")
		switch {
		case len(sentinels) > 0 && has(sentinels, c.Service):
			ss = append(ss, c)
		case len(nodes) > 0 && has(nodes, c.Service):
			ns = append(ns, c)
		case len(sentinels) == 0 && strings.Contains(cmd, "sentinel"):
			ss = append(ss, c)
		case len(nodes) == 0 && isRedis && !strings.Contains(cmd, "sentinel"):
			ns = append(ns, c)
		}
	}
	return ns, ss
}

func (c *Cluster) init() {
	if c.byIP != nil {
		return
	}
	c.byIP = map[string]docker.Container{}
	for _, n := range c.Nodes {
		for _, ip := range n.IPs {
			c.byIP[ip] = n
		}
	}
}

func (c *Cluster) addr(n docker.Container) string {
	return net.JoinHostPort(n.IP(), strconv.Itoa(c.Port))
}

// do runs one command on a fresh connection.
func do(addr string, timeout time.Duration, args ...string) (any, error) {
	conn, err := resp.Dial(addr, timeout)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	return conn.Do(timeout, args...)
}

// DetectMasterName asks the sentinels which master they monitor.
func (c *Cluster) DetectMasterName() (string, error) {
	var lastErr error
	for _, s := range c.Sentinels {
		v, err := do(net.JoinHostPort(s.IP(), strconv.Itoa(c.SentinelPort)), time.Second, "SENTINEL", "masters")
		if err != nil {
			lastErr = err
			continue
		}
		masters, _ := v.([]any)
		for _, m := range masters {
			fields, err := resp.Strings(m)
			if err != nil {
				continue
			}
			for i := 0; i+1 < len(fields); i += 2 {
				if fields[i] == "name" {
					return fields[i+1], nil
				}
			}
		}
		lastErr = fmt.Errorf("sentinel %s monitors no master", s.Name)
	}
	if lastErr == nil {
		lastErr = errors.New("no sentinels")
	}
	return "", lastErr
}

// askSentinel returns the node one sentinel names as primary.
func (c *Cluster) askSentinel(s docker.Container) (docker.Container, error) {
	c.init()
	v, err := do(net.JoinHostPort(s.IP(), strconv.Itoa(c.SentinelPort)), time.Second,
		"SENTINEL", "get-master-addr-by-name", c.MasterName)
	if err != nil {
		return docker.Container{}, err
	}
	addr, err := resp.Strings(v)
	if err != nil || len(addr) != 2 {
		return docker.Container{}, fmt.Errorf("sentinel %s does not know master %q", s.Name, c.MasterName)
	}
	n, ok := c.byIP[addr[0]]
	if !ok {
		return docker.Container{}, fmt.Errorf("sentinel %s names %s, which is not a node", s.Name, addr[0])
	}
	return n, nil
}

// Primary returns the node most sentinels name as primary.
func (c *Cluster) Primary() (docker.Container, error) {
	votes := map[string]int{}
	nodes := map[string]docker.Container{}
	var lastErr error
	for _, s := range c.Sentinels {
		n, err := c.askSentinel(s)
		if err != nil {
			lastErr = err
			continue
		}
		votes[n.Name]++
		nodes[n.Name] = n
	}
	best := ""
	for name, v := range votes {
		if best == "" || v > votes[best] || (v == votes[best] && name < best) {
			best = name
		}
	}
	if best == "" {
		if lastErr == nil {
			lastErr = errors.New("no sentinels")
		}
		return docker.Container{}, lastErr
	}
	return nodes[best], nil
}

// replication parses INFO replication of one node.
func (c *Cluster) replication(n docker.Container) (map[string]string, error) {
	v, err := do(c.addr(n), time.Second, "INFO", "replication")
	if err != nil {
		return nil, err
	}
	s, _ := v.(string)
	out := map[string]string{}
	for _, line := range strings.Split(s, "\n") {
		if k, val, ok := strings.Cut(strings.TrimSpace(line), ":"); ok {
			out[k] = val
		}
	}
	return out, nil
}

// healthy reports whether every node replicates from the primary the
// sentinels agree on, and every sentinel sees all replicas and sentinels.
func (c *Cluster) healthy() (docker.Container, error) {
	primary := docker.Container{}
	for _, s := range c.Sentinels {
		n, err := c.askSentinel(s)
		if err != nil {
			return primary, err
		}
		if primary.ID != "" && n.ID != primary.ID {
			return primary, fmt.Errorf("sentinels disagree: %s and %s", primary.Name, n.Name)
		}
		primary = n
	}
	info, err := c.replication(primary)
	if err != nil {
		return primary, err
	}
	if info["role"] != "master" {
		return primary, fmt.Errorf("%s is %s, not master", primary.Name, info["role"])
	}
	for _, n := range c.Nodes {
		if n.ID == primary.ID {
			continue
		}
		info, err := c.replication(n)
		if err != nil {
			return primary, err
		}
		if info["role"] != "slave" || info["master_link_status"] != "up" || c.byIP[info["master_host"]].ID != primary.ID {
			return primary, fmt.Errorf("%s does not replicate from %s yet", n.Name, primary.Name)
		}
	}
	for _, s := range c.Sentinels {
		addr := net.JoinHostPort(s.IP(), strconv.Itoa(c.SentinelPort))
		for _, what := range []string{"replicas", "sentinels"} {
			v, err := do(addr, time.Second, "SENTINEL", what, c.MasterName)
			if err != nil {
				return primary, err
			}
			got, _ := v.([]any)
			want := len(c.Nodes) - 1
			if what == "sentinels" {
				want = len(c.Sentinels) - 1
			}
			if len(got) < want {
				return primary, fmt.Errorf("%s sees %d of %d %s", s.Name, len(got), want, what)
			}
		}
	}
	return primary, nil
}

// WaitHealthy polls until the group is healthy or timeout passes.
func (c *Cluster) WaitHealthy(ctx context.Context, timeout time.Duration) (docker.Container, error) {
	c.init()
	deadline := time.Now().Add(timeout)
	for {
		p, err := c.healthy()
		if err == nil {
			return p, nil
		}
		if time.Now().After(deadline) {
			return p, fmt.Errorf("cluster not healthy after %v: %w", timeout, err)
		}
		select {
		case <-ctx.Done():
			return p, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
}

// Members reads the set from one node.
func (c *Cluster) Members(n docker.Container) (map[int64]bool, error) {
	v, err := do(c.addr(n), 30*time.Second, "SMEMBERS", c.Key)
	if err != nil {
		return nil, err
	}
	ss, err := resp.Strings(v)
	if err != nil {
		return nil, err
	}
	out := make(map[int64]bool, len(ss))
	for _, s := range ss {
		if x, err := strconv.ParseInt(s, 10, 64); err == nil {
			out[x] = true
		}
	}
	return out, nil
}

// Converge waits until every node holds the same number of elements as the
// primary, then reads the set from every node.
func (c *Cluster) Converge(ctx context.Context, primary docker.Container, timeout time.Duration) (final map[int64]bool, others map[string]map[int64]bool, err error) {
	deadline := time.Now().Add(timeout)
	for {
		same := true
		var want int64 = -1
		for _, n := range append([]docker.Container{primary}, c.Nodes...) {
			v, err := do(c.addr(n), time.Second, "SCARD", c.Key)
			x, _ := v.(int64)
			if err != nil {
				same = false
				break
			}
			if want < 0 {
				want = x
			} else if x != want {
				same = false
				break
			}
		}
		if same || time.Now().After(deadline) {
			break
		}
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		case <-time.After(500 * time.Millisecond):
		}
	}
	final, err = c.Members(primary)
	if err != nil {
		return nil, nil, fmt.Errorf("reading the set from %s: %w", primary.Name, err)
	}
	others = map[string]map[int64]bool{}
	for _, n := range c.Nodes {
		if n.ID == primary.ID {
			continue
		}
		if m, err := c.Members(n); err == nil {
			others[n.Name] = m
		}
	}
	return final, others, nil
}

// Workload is the set workload's settings.
type Workload struct {
	Clients      int
	Rate         float64 // writes per second, all clients together
	Timeout      time.Duration
	WaitReplicas int // if > 0, a write counts only after WAIT confirms it reached this many replicas
}

// Run writes until ctx ends, recording every write in h.
func (c *Cluster) Run(ctx context.Context, w Workload, h *history.History) {
	c.init()
	var next atomic.Int64
	var wg sync.WaitGroup
	interval := time.Duration(float64(time.Second) * float64(w.Clients) / w.Rate)
	for i := 0; i < w.Clients; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			c.client(ctx, i, w, interval, &next, h)
		}()
	}
	wg.Wait()
}

func (c *Cluster) client(ctx context.Context, id int, w Workload, interval time.Duration, next *atomic.Int64, h *history.History) {
	var conn *resp.Conn
	var node docker.Container
	defer func() {
		if conn != nil {
			conn.Close()
		}
	}()
	tick := time.NewTicker(interval)
	defer tick.Stop()
	// Spread the clients over the first interval.
	time.Sleep(interval * time.Duration(id) / time.Duration(w.Clients))
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		if conn == nil {
			// Like a Sentinel-aware client: ask Sentinel where the primary
			// is, then stay on that connection until it fails.
			p, err := c.Primary()
			if err != nil {
				continue
			}
			cn, err := resp.Dial(c.addr(p), w.Timeout)
			if err != nil {
				continue
			}
			conn, node = cn, p
		}
		v := next.Add(1)
		op := history.Op{Value: v, Client: id, Node: node.Name, Start: time.Now()}
		_, err := conn.Do(w.Timeout, "SADD", c.Key, strconv.FormatInt(v, 10))
		written := err == nil
		if written && w.WaitReplicas > 0 {
			var n any
			n, err = conn.Do(w.Timeout, "WAIT", strconv.Itoa(w.WaitReplicas), strconv.FormatInt(w.Timeout.Milliseconds(), 10))
			if got, _ := n.(int64); err == nil && got < int64(w.WaitReplicas) {
				err = fmt.Errorf("WAIT: reached %d of %d replicas", got, w.WaitReplicas)
			}
		}
		op.End = time.Now()
		var refused resp.Error
		switch {
		case err == nil:
			op.Result = history.Ack
		case !written && errors.As(err, &refused):
			op.Result, op.Err = history.Fail, string(refused)
		default:
			// Timeouts, broken connections and writes that did not
			// reach enough replicas: the write may or may not be there.
			op.Result, op.Err = history.Unknown, err.Error()
		}
		h.Add(op)
		if err != nil {
			conn.Close()
			conn = nil
		}
	}
}

// Watch records an event every time the primary the sentinels agree on
// changes, until ctx ends.
func (c *Cluster) Watch(ctx context.Context, h *history.History, start docker.Container) {
	cur := start.Name
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(250 * time.Millisecond):
		}
		p, err := c.Primary()
		if err != nil || p.Name == cur {
			continue
		}
		h.Event("primary", fmt.Sprintf("Sentinel promoted %s (was %s)", p.Name, cur))
		cur = p.Name
	}
}

// Names lists container names, for messages.
func Names(cs []docker.Container) string {
	out := make([]string, len(cs))
	for i, c := range cs {
		out[i] = c.Name
	}
	sort.Strings(out)
	return strings.Join(out, ", ")
}
