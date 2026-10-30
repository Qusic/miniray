package tunnel

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Qusic/miniray/internal/netutil"
)

// The delay before each new attempt doubles up to maxBackoff, whether the hub
// was unreachable or rejected the registration by closing the stream, and
// starts over once the hub has acknowledged a registration with a ready
// message. Run ends with the cause of its context.
func TestSpokeReconnects(t *testing.T) {
	bubble(t, func(t *testing.T) {
		start := time.Now()
		var attempts []time.Duration
		ctx, cancel := context.WithCancelCause(t.Context())
		stop := errors.New("stop")
		spoke := Spoke{
			Name: "lab",
			Log:  quiet,
			DialHub: func(_ context.Context, host netutil.Address, port uint16) (net.Conn, error) {
				if host.Domain() != "lab.tunnel" || port != 0 {
					t.Errorf("registered at %s:%d, want lab.tunnel:0", host, port)
				}
				attempts = append(attempts, time.Since(start))
				conn, hub := net.Pipe()
				// reply has the hub send line, then drop the stream.
				reply := func(line string) (net.Conn, error) {
					go func() {
						hub.Write([]byte(line + "\n"))
						hub.Close()
					}()
					return conn, nil
				}
				switch len(attempts) {
				case 1, 3, 5, 7:
					return nil, errors.New("hub unreachable")
				case 6:
					return reply(`{"type":"later"}`) // no acknowledgment
				case 8:
					return reply(`{"type":"ready"}`) // accepted
				case 10:
					cancel(stop)
				}
				hub.Close() // rejected
				return conn, nil
			},
		}
		if err := spoke.Run(ctx); err != stop {
			t.Errorf("Run = %v, want the cause %v", err, stop)
		}
		s := time.Second
		want := []time.Duration{0, 1 * s, 3 * s, 7 * s, 15 * s, 31 * s, 61 * s, 91 * s, 92 * s, 94 * s}
		if !slices.Equal(attempts, want) {
			t.Errorf("attempts at %v, want %v", attempts, want)
		}
	})
}

// halfConn is one end of a connection with real half-close, like TCP, built
// from two one-way net.Pipes.
type halfConn struct {
	net.Conn // reads
	writer   net.Conn
}

func halfPair() (*halfConn, *halfConn) {
	r1, w1 := net.Pipe()
	r2, w2 := net.Pipe()
	return &halfConn{r1, w2}, &halfConn{r2, w1}
}

func (c *halfConn) Write(b []byte) (int, error) { return c.writer.Write(b) }
func (c *halfConn) CloseWrite() error           { return c.writer.Close() }
func (c *halfConn) Close() error                { return errors.Join(c.Conn.Close(), c.writer.Close()) }

// The alias is the target of the stream the hub asked for: once the alias has
// finished, the stream, which cannot half-close, ends within a moment, rather
// than after the long wait that Join gives a target's response.
func TestSpokeEndsStreamSoonAfterAlias(t *testing.T) {
	bubble(t, func(t *testing.T) {
		hub := NewHub("hub", quiet)
		spoke, _ := spokeServing(t, hub, "lab")
		alias, server := halfPair()
		defer server.Close()
		spoke.DialAlias = func(context.Context, netutil.Label, uint16) (net.Conn, error) {
			return alias, nil
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go spoke.Run(ctx)
		synctest.Wait()
		conn, err := hub.Dial(t.Context(), "lab", "nas", 22)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		go func() {
			server.Write([]byte("response"))
			server.CloseWrite()
		}()
		got := make([]byte, len("response"))
		if _, err := io.ReadFull(conn, got); err != nil || string(got) != "response" {
			t.Fatalf("read %q, %v; want the response", got, err)
		}
		start := time.Now()
		if n, err := conn.Read(make([]byte, 1)); n != 0 || err != io.EOF {
			t.Fatalf("read %d, %v after the response; want EOF", n, err)
		}
		// Join lingers about a second, and would give a response half a minute.
		if elapsed := time.Since(start); elapsed > 5*time.Second {
			t.Errorf("stream ended %v after the alias finished, want within a moment", elapsed)
		}
	})
}

// Connections already open outlive the control stream, while the hub stops
// routing to the spoke at once.
func TestSpokeKeepsOpenConnections(t *testing.T) {
	bubble(t, func(t *testing.T) {
		hub := NewHub("hub", quiet)
		spoke, _ := spokeServing(t, hub, "lab")
		ctx, cancel := context.WithCancel(t.Context())
		stopped := make(chan error)
		go func() { stopped <- spoke.Run(ctx) }()
		synctest.Wait()
		conn, err := hub.Dial(t.Context(), "lab", "nas", 22)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		cancel()
		<-stopped
		synctest.Wait()
		if _, err := hub.Dial(t.Context(), "lab", "nas", 22); err == nil || !strings.Contains(err.Error(), "not registered") {
			t.Errorf("Dial to a spoke that left = %v, want not registered", err)
		}
		exchange(t, conn, "still open")
	})
}

// A spoke logs its registration at a level shown without debug, and each
// connection it opens for the hub only at debug level.
func TestSpokeLogs(t *testing.T) {
	var logs bytes.Buffer
	bubble(t, func(t *testing.T) {
		hub := NewHub("hub", quiet)
		spoke, _ := spokeServing(t, hub, "lab")
		spoke.Log = textLog(&logs)
		ctx, cancel := context.WithCancel(t.Context())
		stopped := make(chan error)
		go func() { stopped <- spoke.Run(ctx) }()
		synctest.Wait()
		conn, err := hub.Dial(t.Context(), "lab", "nas", 22)
		if err != nil {
			t.Fatal(err)
		}
		exchange(t, conn, "hello")
		time.Sleep(time.Second)
		conn.Close()
		cancel()
		<-stopped
	})
	// Read the log only now that the bubble has ended: the spoke logs the end
	// of a connection just before that goroutine exits, and only the end of the
	// bubble orders such an exit before this goroutine goes on.
	want := `level=INFO msg="registered with the hub"
level=DEBUG msg="request started" alias=nas port=22
level=DEBUG msg="request ended" alias=nas port=22 duration=1s err=<nil>
`
	if got := logs.String(); got != want {
		t.Errorf("log:\n%s\nwant:\n%s", got, want)
	}
}
