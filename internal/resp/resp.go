// Package resp is a minimal client for the Redis protocol (RESP2). It covers
// what crepa needs: send a command, read one reply, with a deadline.
package resp

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"time"
)

// Error is an error reply from the server, like "-READONLY ...".
type Error string

func (e Error) Error() string { return string(e) }

// Conn is one connection to a Redis-protocol server. It is not safe for
// concurrent use.
type Conn struct {
	c net.Conn
	r *bufio.Reader
	w *bufio.Writer
}

// Dial connects to addr within timeout.
func Dial(addr string, timeout time.Duration) (*Conn, error) {
	c, err := net.DialTimeout("tcp", addr, timeout)
	if err != nil {
		return nil, err
	}
	return &Conn{c: c, r: bufio.NewReader(c), w: bufio.NewWriter(c)}, nil
}

// Close closes the connection.
func (c *Conn) Close() error { return c.c.Close() }

// Do sends one command and reads its reply. The whole round trip must finish
// within timeout. The reply is nil, int64, string, Error or []any.
// An Error reply is returned as the error.
func (c *Conn) Do(timeout time.Duration, args ...string) (any, error) {
	if err := c.c.SetDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}
	fmt.Fprintf(c.w, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(c.w, "$%d\r\n%s\r\n", len(a), a)
	}
	if err := c.w.Flush(); err != nil {
		return nil, err
	}
	v, err := c.read()
	if err != nil {
		return nil, err
	}
	if e, ok := v.(Error); ok {
		return nil, e
	}
	return v, nil
}

func (c *Conn) read() (any, error) {
	line, err := c.r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if len(line) < 3 || line[len(line)-2] != '\r' {
		return nil, fmt.Errorf("resp: malformed line %q", line)
	}
	kind, body := line[0], line[1:len(line)-2]
	switch kind {
	case '+':
		return body, nil
	case '-':
		return Error(body), nil
	case ':':
		return strconv.ParseInt(body, 10, 64)
	case '$':
		n, err := strconv.Atoi(body)
		if err != nil {
			return nil, err
		}
		if n < 0 {
			return nil, nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(c.r, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*':
		n, err := strconv.Atoi(body)
		if err != nil {
			return nil, err
		}
		if n < 0 {
			return nil, nil
		}
		out := make([]any, n)
		for i := range out {
			v, err := c.read()
			if err != nil {
				return nil, err
			}
			out[i] = v
		}
		return out, nil
	}
	return nil, fmt.Errorf("resp: unknown reply type %q", kind)
}

// Strings converts an array reply of bulk strings to []string.
func Strings(v any) ([]string, error) {
	arr, ok := v.([]any)
	if !ok {
		if v == nil {
			return nil, nil
		}
		return nil, fmt.Errorf("resp: expected array, got %T", v)
	}
	out := make([]string, 0, len(arr))
	for _, x := range arr {
		s, ok := x.(string)
		if !ok {
			return nil, fmt.Errorf("resp: expected string element, got %T", x)
		}
		out = append(out, s)
	}
	return out, nil
}
