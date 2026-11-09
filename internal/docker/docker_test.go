package docker

import (
	"context"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
)

const inspectJSON = `[
 {"Id":"bbb","Name":"/p-redis-2","Config":{"Image":"redis:7.4","Cmd":["redis-server","--replicaof","redis-1","6379"],
  "Labels":{"com.docker.compose.service":"redis-2","com.docker.compose.container-number":"1"}},
  "NetworkSettings":{"Networks":{"p_default":{"IPAddress":"172.20.0.3"}}}},
 {"Id":"aaa","Name":"/p-redis-1","Config":{"Image":"redis:7.4","Cmd":["redis-server"],
  "Labels":{"com.docker.compose.service":"redis-1","com.docker.compose.container-number":"1"}},
  "NetworkSettings":{"Networks":{"p_default":{"IPAddress":"172.20.0.2"},"p_other":{"IPAddress":"172.21.0.2"}}}},
 {"Id":"s1","Name":"/p-sentinel-1","Config":{"Image":"redis:7.4","Cmd":["redis-sentinel","/etc/sentinel.conf"],
  "Labels":{"com.docker.compose.service":"sentinel","com.docker.compose.container-number":"1"}},
  "NetworkSettings":{"Networks":{"p_default":{"IPAddress":"172.20.0.5"}}}},
 {"Id":"s2","Name":"/p-sentinel-2","Config":{"Image":"redis:7.4","Cmd":["redis-sentinel","/etc/sentinel.conf"],
  "Labels":{"com.docker.compose.service":"sentinel","com.docker.compose.container-number":"2"}},
  "NetworkSettings":{"Networks":{"p_default":{"IPAddress":"172.20.0.6"}}}}
]`

func TestParseInspect(t *testing.T) {
	cs, err := parseInspect(inspectJSON)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, c := range cs {
		names = append(names, c.Name)
	}
	if !reflect.DeepEqual(names, []string{"redis-1", "redis-2", "sentinel-1", "sentinel-2"}) {
		t.Fatalf("names = %q", names)
	}
	if !reflect.DeepEqual(cs[0].IPs, []string{"172.20.0.2", "172.21.0.2"}) {
		t.Fatalf("ips = %q", cs[0].IPs)
	}
	if cs[2].Command != "redis-sentinel /etc/sentinel.conf" || cs[2].Service != "sentinel" {
		t.Fatalf("sentinel = %+v", cs[2])
	}
}

// recorder answers docker commands like a docker with nothing running.
type recorder struct {
	mu   sync.Mutex
	cmds []string
}

func (r *recorder) run(_ context.Context, _ string, args ...string) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.cmds = append(r.cmds, strings.Join(args, " "))
	if args[0] == "run" {
		return "helper-" + strings.TrimPrefix(args[6], "container:") + "\n", nil
	}
	return "", nil
}

func (r *recorder) execs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []string
	for _, c := range r.cmds {
		if strings.HasPrefix(c, "exec ") {
			out = append(out, c)
		}
	}
	r.cmds = nil
	sort.Strings(out)
	return out
}

func TestPartition(t *testing.T) {
	cs, _ := parseInspect(inspectJSON)
	rec := &recorder{}
	n := &Net{Run: rec.run, Project: "p"}
	if err := n.Attach(context.Background(), cs); err != nil {
		t.Fatal(err)
	}
	if got := rec.execs(); len(got) != 4 || !strings.Contains(got[0], "iptables -N CREPA") {
		t.Fatalf("attach execs = %q", got)
	}

	// Isolate redis-1 from redis-2 and sentinel-1; sentinel-2 is in no group.
	err := n.Partition(context.Background(), [][]Container{{cs[0]}, {cs[1], cs[2]}})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{
		"exec helper-aaa sh -c iptables -F CREPA && " +
			"iptables -A CREPA -s 172.20.0.3 -j DROP && iptables -A CREPA -d 172.20.0.3 -j DROP && " +
			"iptables -A CREPA -s 172.20.0.5 -j DROP && iptables -A CREPA -d 172.20.0.5 -j DROP",
		"exec helper-bbb sh -c iptables -F CREPA && " +
			"iptables -A CREPA -s 172.20.0.2 -j DROP && iptables -A CREPA -d 172.20.0.2 -j DROP && " +
			"iptables -A CREPA -s 172.21.0.2 -j DROP && iptables -A CREPA -d 172.21.0.2 -j DROP",
		"exec helper-s1 sh -c iptables -F CREPA && " +
			"iptables -A CREPA -s 172.20.0.2 -j DROP && iptables -A CREPA -d 172.20.0.2 -j DROP && " +
			"iptables -A CREPA -s 172.21.0.2 -j DROP && iptables -A CREPA -d 172.21.0.2 -j DROP",
		"exec helper-s2 sh -c iptables -F CREPA",
	}
	if got := rec.execs(); !reflect.DeepEqual(got, want) {
		t.Fatalf("partition execs:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}
