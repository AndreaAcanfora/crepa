package redis

import (
	"testing"

	"github.com/AndreaAcanfora/crepa/internal/docker"
)

func TestClassify(t *testing.T) {
	cs := []docker.Container{
		{Name: "app", Service: "app", Image: "myapp:latest", Command: "./server"},
		{Name: "redis-1", Service: "redis-1", Image: "redis:7.4-alpine", Command: "redis-server"},
		{Name: "cache", Service: "cache", Image: "valkey/valkey:8", Command: "valkey-server --port 6379"},
		{Name: "sentinel-1", Service: "sentinel-1", Image: "redis:7.4-alpine", Command: "sh -c exec redis-sentinel /tmp/s.conf"},
	}
	ns, ss := Classify(cs, nil, nil)
	if Names(ns) != "cache, redis-1" || Names(ss) != "sentinel-1" {
		t.Fatalf("detected nodes [%s] sentinels [%s]", Names(ns), Names(ss))
	}
	ns, ss = Classify(cs, []string{"redis-1"}, []string{"sentinel-1"})
	if Names(ns) != "redis-1" || Names(ss) != "sentinel-1" {
		t.Fatalf("explicit nodes [%s] sentinels [%s]", Names(ns), Names(ss))
	}
}

func TestLookup(t *testing.T) {
	c := &Cluster{Nodes: []docker.Container{
		{ID: "a", Name: "redis-1", Service: "redis-1", IPs: []string{"172.20.0.2"}},
		{ID: "b", Name: "redis-2", Service: "redis-2", IPs: []string{"172.20.0.3"}},
	}}
	c.init()
	if c.lookup("172.20.0.3").ID != "b" || c.lookup("redis-1").ID != "a" || c.lookup("nope").ID != "" {
		t.Fatal("lookup")
	}
}
