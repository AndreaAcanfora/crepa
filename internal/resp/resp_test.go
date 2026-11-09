package resp

import (
	"bufio"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"
)

// fakeServer answers each command with the next canned reply.
func fakeServer(t *testing.T, replies ...string) (addr string, got chan []string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { ln.Close() })
	got = make(chan []string, len(replies))
	go func() {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		defer c.Close()
		r := bufio.NewReader(c)
		for _, reply := range replies {
			line, err := r.ReadString('\n')
			if err != nil {
				return
			}
			var n int
			for _, ch := range strings.TrimSpace(line[1:]) {
				n = n*10 + int(ch-'0')
			}
			args := make([]string, n)
			for i := range args {
				r.ReadString('\n')
				s, _ := r.ReadString('\n')
				args[i] = strings.TrimSuffix(s, "\r\n")
			}
			got <- args
			c.Write([]byte(reply))
		}
	}()
	return ln.Addr().String(), got
}

func TestDo(t *testing.T) {
	addr, got := fakeServer(t,
		":1\r\n",
		"*2\r\n$8\r\n10.0.0.2\r\n$4\r\n6379\r\n",
		"-READONLY You can't write against a read only replica.\r\n",
		"$-1\r\n",
		"+OK\r\n",
	)
	c, err := Dial(addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	v, err := c.Do(time.Second, "SADD", "crepa", "1")
	if err != nil || v != int64(1) {
		t.Fatalf("SADD = %v, %v", v, err)
	}
	if args := <-got; !reflect.DeepEqual(args, []string{"SADD", "crepa", "1"}) {
		t.Fatalf("server got %q", args)
	}

	v, err = c.Do(time.Second, "SENTINEL", "get-master-addr-by-name", "mymaster")
	if err != nil {
		t.Fatal(err)
	}
	ss, err := Strings(v)
	if err != nil || !reflect.DeepEqual(ss, []string{"10.0.0.2", "6379"}) {
		t.Fatalf("addr = %q, %v", ss, err)
	}

	_, err = c.Do(time.Second, "SADD", "crepa", "2")
	if e, ok := err.(Error); !ok || !strings.HasPrefix(string(e), "READONLY") {
		t.Fatalf("err = %v", err)
	}

	v, err = c.Do(time.Second, "GET", "missing")
	if err != nil || v != nil {
		t.Fatalf("GET = %v, %v", v, err)
	}

	v, err = c.Do(time.Second, "PING")
	if err != nil || v != "OK" {
		t.Fatalf("PING = %v, %v", v, err)
	}
}

func TestDeadline(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		c, _ := ln.Accept()
		if c != nil {
			time.Sleep(2 * time.Second)
			c.Close()
		}
	}()
	c, err := Dial(ln.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	start := time.Now()
	if _, err := c.Do(100*time.Millisecond, "PING"); err == nil {
		t.Fatal("expected timeout")
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("timeout took %v", d)
	}
}
