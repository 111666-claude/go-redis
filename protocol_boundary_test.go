package redis

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/maintnotifications"
)

// pipeServer is a scriptable fake Redis server: every client dial creates a
// net.Pipe pair and runs handler on the server end with the 1-based dial
// index, so tests can program per-connection behavior (malformed replies,
// truncation, close mid-frame, auth rejection).
type pipeServer struct {
	dials   atomic.Int32
	handler func(conn net.Conn, dial int)
}

func (s *pipeServer) dialer(_ context.Context, _, _ string) (net.Conn, error) {
	client, server := net.Pipe()
	dial := int(s.dials.Add(1))
	go s.handler(server, dial)
	return client, nil
}

// readPipeCommand reads one RESP command (array of bulk strings) from the
// server side of a pipe.
func readPipeCommand(rd *bufio.Reader) ([]string, error) {
	header, err := rd.ReadString('\n')
	if err != nil {
		return nil, err
	}
	if !strings.HasPrefix(header, "*") {
		return nil, fmt.Errorf("unexpected command header %q", header)
	}
	n, err := strconv.Atoi(strings.TrimSpace(header[1:]))
	if err != nil || n <= 0 {
		return nil, fmt.Errorf("bad command array length %q", header)
	}
	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		lenLine, err := rd.ReadString('\n')
		if err != nil {
			return nil, err
		}
		if !strings.HasPrefix(lenLine, "$") {
			return nil, fmt.Errorf("unexpected bulk header %q", lenLine)
		}
		l, err := strconv.Atoi(strings.TrimSpace(lenLine[1:]))
		if err != nil {
			return nil, err
		}
		buf := make([]byte, l+2)
		if _, err := io.ReadFull(rd, buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:l]))
	}
	return args, nil
}

// servePipeHandshake answers the connection handshake (HELLO, and anything
// else the client sends before the first user command) with a minimal valid
// reply, then returns. Commands the test cares about are handled afterwards.
func servePipeHandshake(rd *bufio.Reader, conn net.Conn) error {
	for {
		args, err := readPipeCommand(rd)
		if err != nil {
			return err
		}
		switch strings.ToLower(args[0]) {
		case "hello":
			if _, err := conn.Write([]byte("%0\r\n")); err != nil {
				return err
			}
			return nil
		default:
			// CLIENT SETINFO and friends (DisableIdentity is set in these
			// tests, but stay permissive).
			if _, err := conn.Write([]byte("+OK\r\n")); err != nil {
				return err
			}
		}
	}
}

func newPipeTestClient(s *pipeServer, extra ...func(*Options)) *Client {
	opt := &Options{
		Protocol:        3,
		MaxRetries:      -1,
		DisableIdentity: true,
		Dialer:          s.dialer,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	}
	for _, fn := range extra {
		fn(opt)
	}
	return NewClient(opt)
}

// A reply the client cannot parse (unknown RESP type byte) must poison the
// connection: it is removed from the pool and closed, and the next command
// runs on a freshly dialed connection — the leftover bytes of the bad frame
// must never be decoded as another request's response.
func TestProtocolBoundaryInvalidReplyConnNotReused(t *testing.T) {
	conn1Closed := make(chan struct{}, 1)
	s := &pipeServer{}
	s.handler = func(conn net.Conn, dial int) {
		defer conn.Close()
		rd := bufio.NewReader(conn)
		if err := servePipeHandshake(rd, conn); err != nil {
			return
		}
		for {
			args, err := readPipeCommand(rd)
			if err != nil {
				if dial == 1 {
					conn1Closed <- struct{}{}
				}
				return
			}
			var reply string
			switch strings.ToLower(args[0]) {
			case "get":
				if dial == 1 {
					reply = "?bogus\r\n" // unknown RESP type byte
				} else {
					reply = "$5\r\nvalue\r\n"
				}
			case "ping":
				reply = "+PONG\r\n"
			default:
				reply = "+OK\r\n"
			}
			if _, err := conn.Write([]byte(reply)); err != nil {
				return
			}
		}
	}
	client := newPipeTestClient(s)
	defer client.Close()
	ctx := context.Background()

	if err := client.Get(ctx, "k").Err(); err == nil {
		t.Fatal("Get with unparseable reply succeeded; want protocol error")
	}
	select {
	case <-conn1Closed:
	case <-time.After(3 * time.Second):
		t.Fatal("client did not close the connection that sent an invalid reply")
	}

	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("Ping after protocol error: %v", err)
	}
	if got := s.dials.Load(); got != 2 {
		t.Fatalf("dials = %d, want 2 (bad conn must not be reused)", got)
	}
	// And the fresh connection serves reads correctly — no leftover bytes
	// from the poisoned frame leak into later replies.
	if got := client.Get(ctx, "k").Val(); got != "value" {
		t.Fatalf("Get on fresh conn = %q, want %q", got, "value")
	}
}

// A truncated bulk payload followed by silence must fail the command at the
// read deadline and retire the connection; the half-read frame must not be
// resumed by the next caller.
func TestProtocolBoundaryTruncatedReplyConnNotReused(t *testing.T) {
	s := &pipeServer{}
	s.handler = func(conn net.Conn, dial int) {
		defer conn.Close()
		rd := bufio.NewReader(conn)
		if err := servePipeHandshake(rd, conn); err != nil {
			return
		}
		for {
			args, err := readPipeCommand(rd)
			if err != nil {
				return
			}
			if dial == 1 {
				// Advertise 10 bytes, send 3, then go silent: the client must
				// time out mid-frame and drop the connection.
				if _, err := conn.Write([]byte("$10\r\nabc")); err != nil {
					return
				}
				select {} // block until the client closes the pipe
			}
			reply := "+PONG\r\n"
			if strings.ToLower(args[0]) != "ping" {
				reply = "+OK\r\n"
			}
			if _, err := conn.Write([]byte(reply)); err != nil {
				return
			}
		}
	}
	client := newPipeTestClient(s, func(o *Options) { o.ReadTimeout = 200 * time.Millisecond })
	defer client.Close()
	ctx := context.Background()

	if err := client.Get(ctx, "k").Err(); err == nil {
		t.Fatal("Get with truncated reply succeeded; want timeout error")
	}
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("Ping after truncated reply: %v", err)
	}
	if got := s.dials.Load(); got != 2 {
		t.Fatalf("dials = %d, want 2 (conn with half-read frame must not be reused)", got)
	}
}

// A server that closes the connection mid-frame forces a reconnect; the new
// connection starts from a clean buffer — bytes half-read from the old one
// must not leak into the next handshake or reply.
func TestProtocolBoundaryReconnectDropsHalfReadBuffer(t *testing.T) {
	s := &pipeServer{}
	s.handler = func(conn net.Conn, dial int) {
		defer conn.Close()
		rd := bufio.NewReader(conn)
		if err := servePipeHandshake(rd, conn); err != nil {
			return
		}
		for {
			args, err := readPipeCommand(rd)
			if err != nil {
				return
			}
			if dial == 1 {
				// Write a partial frame and hang up.
				conn.Write([]byte("$5\r\nhe"))
				return
			}
			reply := "+PONG\r\n"
			if strings.ToLower(args[0]) != "ping" {
				reply = "+OK\r\n"
			}
			if _, err := conn.Write([]byte(reply)); err != nil {
				return
			}
		}
	}
	client := newPipeTestClient(s)
	defer client.Close()
	ctx := context.Background()

	if err := client.Get(ctx, "k").Err(); err == nil {
		t.Fatal("Get on conn closed mid-frame succeeded; want EOF error")
	}
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("Ping after reconnect: %v", err)
	}
	if got := s.dials.Load(); got != 2 {
		t.Fatalf("dials = %d, want 2", got)
	}
}

// A failed AUTH during the handshake must close the connection rather than
// pool it: reusing it would silently hand later callers a connection whose
// authentication state does not match the configured credentials.
func TestProtocolBoundaryFailedAuthConnNotReused(t *testing.T) {
	conn1Closed := make(chan struct{}, 1)
	s := &pipeServer{}
	s.handler = func(conn net.Conn, dial int) {
		defer conn.Close()
		rd := bufio.NewReader(conn)
		for {
			args, err := readPipeCommand(rd)
			if err != nil {
				if dial == 1 {
					conn1Closed <- struct{}{}
				}
				return
			}
			switch strings.ToLower(args[0]) {
			case "hello", "auth":
				conn.Write([]byte("-WRONGPASS invalid username-password pair\r\n"))
			default:
				conn.Write([]byte("+OK\r\n"))
			}
		}
	}
	client := newPipeTestClient(s, func(o *Options) { o.Password = "secret" })
	defer client.Close()
	ctx := context.Background()

	err := client.Ping(ctx).Err()
	if err == nil {
		t.Fatal("Ping with rejected credentials succeeded; want auth error")
	}
	if !IsAuthError(err) {
		t.Fatalf("Ping error = %v, want an auth error", err)
	}
	select {
	case <-conn1Closed:
	case <-time.After(3 * time.Second):
		t.Fatal("client did not close the connection whose AUTH was rejected")
	}

	// The next command must dial again — the rejected connection is gone.
	_ = client.Ping(ctx).Err()
	if got := s.dials.Load(); got != 2 {
		t.Fatalf("dials = %d, want 2 (auth-failed conn must not be reused)", got)
	}
}

// Concurrent commands over a shared pool: every caller must receive the
// response to its own request. Run with -race to catch buffer sharing.
func TestProtocolBoundaryConcurrentRequestsNoCrossTalk(t *testing.T) {
	s := &pipeServer{}
	s.handler = func(conn net.Conn, dial int) {
		defer conn.Close()
		rd := bufio.NewReader(conn)
		if err := servePipeHandshake(rd, conn); err != nil {
			return
		}
		for {
			args, err := readPipeCommand(rd)
			if err != nil {
				return
			}
			if len(args) == 2 && strings.EqualFold(args[0], "echo") {
				fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(args[1]), args[1])
			} else {
				conn.Write([]byte("+OK\r\n"))
			}
		}
	}
	client := newPipeTestClient(s, func(o *Options) { o.PoolSize = 8 })
	defer client.Close()
	ctx := context.Background()

	const goroutines = 16
	const iterations = 25
	var wg sync.WaitGroup
	errs := make(chan error, goroutines*iterations)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				want := fmt.Sprintf("val-%d-%d", g, i)
				got, err := client.Do(ctx, "echo", want).Text()
				if err != nil {
					errs <- fmt.Errorf("echo %q: %w", want, err)
					return
				}
				if got != want {
					errs <- fmt.Errorf("echo %q got %q (response cross-talk)", want, got)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if got := s.dials.Load(); got == 0 || got > 8 {
		t.Fatalf("dials = %d, want between 1 and PoolSize(8)", got)
	}
}
