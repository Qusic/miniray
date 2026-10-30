package tunnel

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"runtime/debug"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Qusic/miniray/internal/netutil"
)

// bubble runs f in a synctest bubble, and panics if f is still running after
// an hour of fake time, far longer than any test here takes. A spoke retrying
// its registration keeps waking the bubble, so synctest never finds a stuck
// test deadlocked, and it would run on until the test binary times out.
func bubble(t *testing.T, f func(*testing.T)) {
	synctest.Test(t, func(t *testing.T) {
		stuck := time.AfterFunc(time.Hour, func() {
			debug.SetTraceback("all") // to show where the test is stuck
			panic(t.Name() + " still running after an hour of fake time")
		})
		t.Cleanup(func() { stuck.Stop() }) // after f's cleanups, which may get stuck too
		f(t)
	})
}

// quiet discards the logs of hubs and spokes under test.
var quiet = slog.New(slog.DiscardHandler)

// textLog returns a logger that writes every line to w, debug included, as
// text without the time.
func textLog(w io.Writer) *slog.Logger {
	return slog.New(slog.NewTextHandler(w, &slog.HandlerOptions{
		Level: slog.LevelDebug,
		ReplaceAttr: func(_ []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.Attr{}
			}
			return a
		},
	}))
}

// served wires streams to hub the way Serve expects its caller to: each stream
// opened for a request is served by Hub.Serve and closed once Serve returns.
func served(hub *Hub) func(ctx context.Context, host netutil.Address, port uint16) (net.Conn, error) {
	return func(ctx context.Context, host netutil.Address, port uint16) (net.Conn, error) {
		target, err := ParseTarget(host.Domain(), port)
		if err != nil {
			return nil, err
		}
		conn, peer := net.Pipe()
		go func() {
			hub.Serve(context.Background(), peer, target)
			peer.Close()
		}()
		return conn, nil
	}
}

// echo returns a connection that echoes what is written to it.
func echo() net.Conn {
	conn, peer := net.Pipe()
	go func() {
		io.Copy(peer, peer)
		peer.Close()
	}()
	return conn
}

// spokeServing returns a spoke registered as name with hub, whose aliases are
// echo services, and reports each alias:port it dials.
func spokeServing(t *testing.T, hub *Hub, name netutil.Label) (Spoke, <-chan string) {
	dialed := make(chan string, 8)
	return Spoke{
		Name:    name,
		DialHub: served(hub),
		DialAlias: func(_ context.Context, alias netutil.Label, port uint16) (net.Conn, error) {
			dialed <- fmt.Sprintf("%s:%d", alias, port)
			return echo(), nil
		},
		Log: quiet,
	}, dialed
}

// rawSpoke plays a spoke by hand: it registers under a name and passes on what
// the hub sends besides the acknowledgment. Serve's result arrives on served
// once the registration ends. Closing the control ends it.
type rawSpoke struct {
	control  *control
	received chan message
	served   chan error
}

func register(t *testing.T, hub *Hub, name netutil.Label) rawSpoke {
	t.Helper()
	start := time.Now()
	conn, peer := net.Pipe()
	s := rawSpoke{control: newControl(conn), received: make(chan message, 8), served: make(chan error, 1)}
	go func() {
		s.served <- hub.Serve(context.Background(), peer, Target{Kind: Control, Node: name})
		peer.Close()
	}()
	acked := make(chan struct{})
	go func() {
		s.control.run(context.Background(), func(m message) {
			switch m.Type {
			case typeReady:
				close(acked)
			default:
				s.received <- m
			}
		})
		close(s.received)
	}()
	await(t, acked)
	if elapsed := time.Since(start); elapsed != 0 {
		t.Errorf("registration acknowledged after %v, want at once", elapsed)
	}
	return s
}

// next returns the next message other than the acknowledgment.
func (s rawSpoke) next(t *testing.T) message {
	t.Helper()
	select {
	case m, ok := <-s.received:
		if !ok {
			t.Fatal("control stream ended")
		}
		return m
	case <-time.After(time.Minute):
		t.Fatal("no message from the hub")
	}
	return message{}
}

// await receives from ch, or fails after a minute of fake time, so that a stuck
// test reports which wait never ended.
func await[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(time.Minute):
		t.Fatal("nothing arrived within a minute")
	}
	var zero T
	return zero
}

func exchange(t *testing.T, conn net.Conn, text string) {
	t.Helper()
	if _, err := conn.Write([]byte(text)); err != nil {
		t.Fatal(err)
	}
	got := make([]byte, len(text))
	if _, err := io.ReadFull(conn, got); err != nil || string(got) != text {
		t.Fatalf("read %q, %v; want %q", got, err, text)
	}
}

func TestHubRoutesThroughSpoke(t *testing.T) {
	bubble(t, func(t *testing.T) {
		hub := NewHub("hub", quiet)
		spoke, dialed := spokeServing(t, hub, "lab")
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go spoke.Run(ctx)
		synctest.Wait()

		start := time.Now()
		conn, err := hub.Dial(t.Context(), "lab", "nas", 22)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if got := <-dialed; got != "nas:22" {
			t.Errorf("spoke dialed %s, want nas:22", got)
		}
		if elapsed := time.Since(start); elapsed != 0 {
			t.Errorf("opening took %v", elapsed)
		}
		exchange(t, conn, "hello")
	})
}

func TestHubDialFails(t *testing.T) {
	for _, tt := range []struct {
		name string
		// spoke registers a spoke named "lab", if any.
		spoke func(t *testing.T, hub *Hub, ctx context.Context)
		ctx   time.Duration // a deadline for Dial, if not zero
		want  string
		after time.Duration
	}{
		{"unregistered spoke", nil, 0, "spoke lab is not registered", 0},
		{"spoke cannot dial", func(t *testing.T, hub *Hub, ctx context.Context) {
			spoke, _ := spokeServing(t, hub, "lab")
			spoke.DialAlias = func(context.Context, netutil.Label, uint16) (net.Conn, error) {
				return nil, errors.New("connection refused")
			}
			go spoke.Run(ctx)
			synctest.Wait()
		}, 0, "spoke lab: connection refused", 0},
		// The spoke gives up on its dial in time to tell the hub why.
		{"spoke dial hangs", func(t *testing.T, hub *Hub, ctx context.Context) {
			spoke, _ := spokeServing(t, hub, "lab")
			spoke.DialAlias = func(ctx context.Context, _ netutil.Label, _ uint16) (net.Conn, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			go spoke.Run(ctx)
			synctest.Wait()
		}, 0, "spoke lab: context deadline exceeded", dialTimeout},
		{"spoke cannot open the data stream", func(t *testing.T, hub *Hub, ctx context.Context) {
			spoke, _ := spokeServing(t, hub, "lab")
			dialHub := spoke.DialHub
			spoke.DialHub = func(ctx context.Context, host netutil.Address, port uint16) (net.Conn, error) {
				if port != 0 {
					return nil, errors.New("no data stream")
				}
				return dialHub(ctx, host, port)
			}
			go spoke.Run(ctx)
			synctest.Wait()
		}, 0, "spoke lab: no data stream", 0},
		// Dialing the alias and opening the data stream share one dialTimeout.
		{"spoke runs out of time", func(t *testing.T, hub *Hub, ctx context.Context) {
			spoke, _ := spokeServing(t, hub, "lab")
			dialAlias, dialHub := spoke.DialAlias, spoke.DialHub
			spoke.DialAlias = func(ctx context.Context, alias netutil.Label, port uint16) (net.Conn, error) {
				time.Sleep(dialTimeout / 2)
				return dialAlias(ctx, alias, port)
			}
			spoke.DialHub = func(ctx context.Context, host netutil.Address, port uint16) (net.Conn, error) {
				if port != 0 {
					<-ctx.Done()
					return nil, ctx.Err()
				}
				return dialHub(ctx, host, port)
			}
			go spoke.Run(ctx)
			synctest.Wait()
		}, 0, "spoke lab: context deadline exceeded", dialTimeout},
		{"spoke never opens", func(t *testing.T, hub *Hub, ctx context.Context) {
			s := register(t, hub, "lab")
			context.AfterFunc(ctx, func() { s.control.conn.Close() })
		}, 0, "did not open nas:22 within", openTimeout},
		{"spoke goes away", func(t *testing.T, hub *Hub, ctx context.Context) {
			s := register(t, hub, "lab")
			go func() {
				s.next(t)
				s.control.conn.Close()
			}()
		}, 0, "spoke lab went away", 0},
		{"caller gives up", func(t *testing.T, hub *Hub, ctx context.Context) {
			s := register(t, hub, "lab")
			context.AfterFunc(ctx, func() { s.control.conn.Close() })
		}, 3 * time.Second, context.DeadlineExceeded.Error(), 3 * time.Second},
	} {
		t.Run(tt.name, func(t *testing.T) {
			bubble(t, func(t *testing.T) {
				hub := NewHub("hub", quiet)
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				if tt.spoke != nil {
					tt.spoke(t, hub, ctx)
				}
				dialCtx := t.Context()
				if tt.ctx != 0 {
					var cancel context.CancelFunc
					dialCtx, cancel = context.WithTimeout(dialCtx, tt.ctx)
					defer cancel()
				}
				start := time.Now()
				conn, err := hub.Dial(dialCtx, "lab", "nas", 22)
				if conn != nil {
					conn.Close()
				}
				if err == nil || !strings.Contains(err.Error(), tt.want) || time.Since(start) != tt.after {
					t.Errorf("Dial = %v after %v, want %q after %v", err, time.Since(start), tt.want, tt.after)
				}
			})
		})
	}
}

func TestHubRegistration(t *testing.T) {
	t.Run("own name", func(t *testing.T) {
		bubble(t, func(t *testing.T) {
			hub := NewHub("hub", quiet)
			conn, peer := net.Pipe()
			defer conn.Close()
			served := make(chan error, 1)
			go func() {
				served <- hub.Serve(t.Context(), peer, Target{Kind: Control, Node: "hub"})
				peer.Close()
			}()
			// Rejected without an acknowledgment.
			if n, err := conn.Read(make([]byte, 1)); n != 0 || err != io.EOF {
				t.Fatalf("rejected spoke read %d, %v; want the stream closed", n, err)
			}
			if err := <-served; err == nil || !strings.Contains(err.Error(), "own name") {
				t.Errorf("registering the hub's name = %v", err)
			}
			if _, err := hub.Dial(t.Context(), "hub", "nas", 22); err == nil || !strings.Contains(err.Error(), "not registered") {
				t.Errorf("Dial to the hub's own name = %v, want not registered", err)
			}
		})
	})
	// A new registration replaces the old one, and the old one ending does not
	// remove its replacement.
	t.Run("replacement", func(t *testing.T) {
		bubble(t, func(t *testing.T) {
			hub := NewHub("hub", quiet)
			old := register(t, hub, "lab")
			current := register(t, hub, "lab")
			defer current.control.conn.Close()
			if err := await(t, old.served); !errors.Is(err, errReplaced) {
				t.Errorf("replaced registration ended with %v, want %v", err, errReplaced)
			}
			for range 2 {
				go hub.Dial(t.Context(), "lab", "nas", 22)
				if m := current.next(t); m.Type != typeOpen {
					t.Errorf("current spoke got %+v, want an open", m)
				}
			}
		})
	})
}

// The hub logs each registration it accepts, turns away, or ends, at a level
// shown without debug and with the reason for turning it away or ending it.
func TestHubLogs(t *testing.T) {
	bubble(t, func(t *testing.T) {
		var logs bytes.Buffer
		hub := NewHub("hub", textLog(&logs))
		s := register(t, hub, "lab")
		s.control.conn.Close()
		await(t, s.served)
		conn, peer := net.Pipe()
		defer conn.Close()
		hub.Serve(t.Context(), peer, Target{Kind: Control, Node: "hub"})
		peer.Close()
		want := `level=INFO msg="spoke registered" spoke=lab
level=WARN msg="spoke disconnected" spoke=lab err=EOF
level=WARN msg="registration rejected" spoke=hub err="tunnel: hub is the hub's own name"
`
		if got := logs.String(); got != want {
			t.Errorf("log:\n%s\nwant:\n%s", got, want)
		}
	})
}

func TestHubDataStreams(t *testing.T) {
	bubble(t, func(t *testing.T) {
		hub := NewHub("hub", quiet)
		spoke := register(t, hub, "lab")
		defer spoke.control.conn.Close()
		other := register(t, hub, "other")
		defer other.control.conn.Close()

		type result struct {
			conn net.Conn
			err  error
		}
		dialed := make(chan result, 1)
		go func() {
			conn, err := hub.Dial(t.Context(), "lab", "nas", 22)
			dialed <- result{conn, err}
		}()
		id := spoke.next(t).ID

		deliver := func(name netutil.Label, id uint16) (net.Conn, <-chan error) {
			conn, peer := net.Pipe()
			served := make(chan error, 1)
			go func() {
				served <- hub.Serve(t.Context(), peer, Target{Kind: Data, Node: name, Port: id})
				peer.Close()
			}()
			return conn, served
		}
		// Only the spoke asked may deliver, and only the id it was asked for.
		for _, tt := range []struct {
			name netutil.Label
			id   uint16
		}{{"other", id}, {"lab", id + 1}} {
			conn, served := deliver(tt.name, tt.id)
			if err := await(t, served); err == nil || !strings.Contains(err.Error(), "no pending connection") {
				t.Errorf("data stream %d from %s = %v, want no pending connection", tt.id, tt.name, err)
			}
			conn.Close()
		}

		conn, served := deliver("lab", id)
		defer conn.Close()
		r := await(t, dialed)
		if r.err != nil {
			t.Fatal(r.err)
		}
		go io.Copy(conn, conn)
		exchange(t, r.conn, "hello")
		// The hub holds the stream until its user closes it.
		synctest.Wait()
		select {
		case err := <-served:
			t.Fatalf("data stream released while in use: %v", err)
		default:
		}
		r.conn.Close()
		if err := await(t, served); err != nil {
			t.Errorf("data stream ended with %v", err)
		}
		// Dial has returned, so the id is no longer pending.
		late, lateServed := deliver("lab", id)
		defer late.Close()
		if err := await(t, lateServed); err == nil || !strings.Contains(err.Error(), "no pending connection") {
			t.Errorf("second delivery = %v, want no pending connection", err)
		}
	})
}

// A data stream that its Dial stops waiting for before taking it is turned
// away, rather than held for a taker that never comes.
func TestHubTurnsAwayUntakenStreams(t *testing.T) {
	bubble(t, func(t *testing.T) {
		hub := NewHub("hub", quiet)
		gone := make(chan struct{})
		hub.pending[1] = &pending{spoke: "lab", result: make(chan dialResult), gone: gone}
		conn, peer := net.Pipe()
		defer conn.Close()
		served := make(chan error, 1)
		go func() { served <- hub.Serve(t.Context(), peer, Target{Kind: Data, Node: "lab", Port: 1}) }()
		synctest.Wait()
		close(gone)
		if err := await(t, served); err == nil || !strings.Contains(err.Error(), "came too late") {
			t.Errorf("Serve = %v, want came too late", err)
		}
	})
}

// Connection ids skip 0, which registers, wrap around, and skip ids in use.
func TestHubConnectionIDs(t *testing.T) {
	hub := NewHub("hub", quiet)
	hub.lastID = 65534
	hub.pending[65535] = &pending{}
	hub.pending[1] = &pending{}
	if id, err := hub.nextID(); id != 2 || err != nil {
		t.Errorf("nextID = %d, %v; want 2", id, err)
	}
	for id := range uint16(65535) {
		hub.pending[id+1] = &pending{}
	}
	if id, err := hub.nextID(); err == nil {
		t.Errorf("nextID with every id in use = %d", id)
	}
}
