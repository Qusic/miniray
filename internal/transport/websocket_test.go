package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"runtime/debug"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Qusic/miniray/internal/netutil"
	"github.com/coder/websocket"
)

// bubble runs f in a synctest bubble, and panics if f is still running after
// a day of fake time, far longer than any test here takes. Heartbeats keep
// waking the bubble, so synctest never finds a stuck test deadlocked, and it
// would run on until the test binary times out.
func bubble(t *testing.T, f func(*testing.T)) {
	synctest.Test(t, func(t *testing.T) {
		stuck := time.AfterFunc(24*time.Hour, func() {
			debug.SetTraceback("all") // to show where the test is stuck
			panic(t.Name() + " still running after a day of fake time")
		})
		t.Cleanup(func() { stuck.Stop() }) // after f's cleanups, which may get stuck too
		f(t)
	})
}

// dial opens a WebSocket stream to srv over a connection from its in-memory
// network. ctx bounds only the handshake.
func dial(t *testing.T, ctx context.Context, srv *httptest.Server) (net.Conn, error) {
	t.Helper()
	conn, err := srv.Client().Transport.(*http.Transport).DialContext(ctx, "tcp", "example.com:80")
	if err != nil {
		t.Fatal(err)
	}
	return ClientWebSocket(ctx, conn, "ws://example.com/")
}

func write(t *testing.T, w io.Writer, data string) {
	t.Helper()
	if n, err := io.WriteString(w, data); err != nil || n != len(data) {
		t.Fatalf("write = %d, %v", n, err)
	}
}

// acceptServer accepts streams of this package and hands them over, keeping
// each open until the test ends.
func acceptServer(t *testing.T) (*httptest.Server, <-chan net.Conn) {
	conns := make(chan net.Conn, 1)
	srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := AcceptWebSocket(w, r)
		if err != nil {
			t.Error(err)
			return
		}
		conns <- conn
		<-t.Context().Done()
	}))
	return srv, conns
}

// sides lists the two ways to open a stream of this package, accepted from or
// dialed to a plain coder/websocket peer that asks for or accepts protocols.
// Both last until the test ends.
var sides = []struct {
	name string
	open func(t *testing.T, protocols ...string) (net.Conn, *websocket.Conn)
}{
	{"accepted", acceptedStream},
	{"dialed", dialedStream},
}

func acceptedStream(t *testing.T, protocols ...string) (net.Conn, *websocket.Conn) {
	t.Helper()
	srv, conns := acceptServer(t)
	peer, _, err := websocket.Dial(t.Context(), "ws://example.com/", &websocket.DialOptions{
		HTTPClient:   srv.Client(),
		Subprotocols: protocols,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { peer.CloseNow() })
	return <-conns, peer
}

func dialedStream(t *testing.T, protocols ...string) (net.Conn, *websocket.Conn) {
	t.Helper()
	peers := make(chan *websocket.Conn, 1)
	srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		peer, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: protocols})
		if err != nil {
			t.Error(err)
			return
		}
		peers <- peer
	}))
	conn, err := dial(t, t.Context(), srv)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	peer := <-peers
	t.Cleanup(func() { peer.CloseNow() })
	return conn, peer
}

type readResult struct {
	data string
	err  error
}

// pendingRead starts a read of conn and reports what it returns.
func pendingRead(conn net.Conn) <-chan readResult {
	result := make(chan readResult, 1)
	go func() {
		buf := make([]byte, 16)
		n, err := conn.Read(buf)
		result <- readResult{string(buf[:n]), err}
	}()
	return result
}

func TestWebSocketStream(t *testing.T) {
	bubble(t, func(t *testing.T) {
		start := time.Now()
		srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, err := AcceptWebSocket(w, r)
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			buf := make([]byte, 5)
			if _, err := io.ReadFull(conn, buf); err != nil {
				t.Error(err)
				return
			}
			if _, err := conn.Write(buf); err != nil {
				t.Error(err)
				return
			}
			if err := conn.Close(); err != nil {
				t.Error(err)
			}
		}))

		ctx, cancel := context.WithCancel(t.Context())
		conn, err := dial(t, ctx, srv)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		cancel() // the dial context bounds only the handshake.

		// Message boundaries must not affect the byte stream.
		write(t, conn, "he")
		write(t, conn, "llo")
		if got, err := io.ReadAll(conn); err != nil || string(got) != "hello" {
			t.Fatalf("read %q, %v; want hello then EOF", got, err)
		}
		if elapsed := time.Since(start); elapsed != 0 {
			t.Errorf("responsive peer took %v to close", elapsed)
		}
	})
}

// A peer that drops the connection between frames, without the closing
// handshake, reads as io.EOF; one that drops it inside a frame is an error.
func TestWebSocketDroppedPeer(t *testing.T) {
	for _, tt := range []struct {
		name string
		raw  []byte // sent on the bare connection before it drops
		eof  bool
	}{
		{"between frames", nil, true},
		// A masked binary frame that announces 10 bytes and carries 3.
		{"inside a frame", []byte{0x82, 0x80 | 10, 1, 2, 3, 4, 'a', 'b', 'c'}, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			bubble(t, func(t *testing.T) {
				read := make(chan error, 1)
				srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					conn, err := AcceptWebSocket(w, r)
					if err != nil {
						read <- err
						return
					}
					_, err = conn.Read(make([]byte, 16))
					read <- err
				}))
				base, err := srv.Client().Transport.(*http.Transport).DialContext(t.Context(), "tcp", "example.com:80")
				if err != nil {
					t.Fatal(err)
				}
				conn, err := ClientWebSocket(t.Context(), base, "ws://example.com/")
				if err != nil {
					t.Fatal(err)
				}
				defer conn.Close()
				base.Write(tt.raw)
				base.Close()
				if err := <-read; err == nil || (err == io.EOF) != tt.eof {
					t.Errorf("read = %v; want io.EOF: %t", err, tt.eof)
				}
			})
		})
	}
}

// ClientWebSocket takes over its connection, so a failed handshake closes it.
func TestClientWebSocketClosesOnFailure(t *testing.T) {
	bubble(t, func(t *testing.T) {
		conn, peer := net.Pipe()
		defer peer.Close()
		go func() {
			// Answer the upgrade request with a plain response.
			buf := make([]byte, 4096)
			peer.Read(buf)
			io.WriteString(peer, "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")
		}()
		if ws, err := ClientWebSocket(t.Context(), conn, "ws://example.com/"); err == nil {
			ws.Close()
			t.Fatal("handshake succeeded without an upgrade")
		}
		if _, err := conn.Write([]byte("x")); !errors.Is(err, io.ErrClosedPipe) {
			t.Errorf("write after a failed handshake = %v, want the connection closed", err)
		}
	})
}

// ClientWebSocket's connection leads to one host, so a redirect fails the
// handshake without a second request.
func TestClientWebSocketRefusesRedirect(t *testing.T) {
	bubble(t, func(t *testing.T) {
		var requests atomic.Int32
		srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requests.Add(1)
			http.Redirect(w, r, "/elsewhere", http.StatusFound)
		}))
		if ws, err := dial(t, t.Context(), srv); err == nil {
			ws.Close()
			t.Fatal("handshake followed a redirect into a stream")
		}
		if n := requests.Load(); n != 1 {
			t.Errorf("server got %d requests, want 1", n)
		}
	})
}

func TestWebSocketAcceptLifetime(t *testing.T) {
	bubble(t, func(t *testing.T) {
		accepted := make(chan net.Conn, 1)
		srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, err := AcceptWebSocket(w, r)
			if err != nil {
				t.Error(err)
				return
			}
			accepted <- conn
			// Returning must also close an otherwise idle connection.
		}))
		peer, err := dial(t, t.Context(), srv)
		if err != nil {
			t.Fatal(err)
		}
		defer peer.Close()
		conn := <-accepted
		defer conn.Close()
		if _, err := peer.Read(make([]byte, 1)); err == nil {
			t.Fatal("connection survived its handler")
		}
	})
}

func TestWebSocketRejectsText(t *testing.T) {
	bubble(t, func(t *testing.T) {
		done := make(chan error, 1)
		srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, err := AcceptWebSocket(w, r)
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			_, err = conn.Read(make([]byte, 1))
			done <- err
		}))
		peer, _, err := websocket.Dial(t.Context(), "ws://example.com/", &websocket.DialOptions{HTTPClient: srv.Client()})
		if err != nil {
			t.Fatal(err)
		}
		defer peer.CloseNow()
		if err := peer.Write(t.Context(), websocket.MessageText, []byte("wrong type")); err != nil {
			t.Fatal(err)
		}
		if _, _, err := peer.Read(t.Context()); websocket.CloseStatus(err) != websocket.StatusUnsupportedData {
			t.Fatalf("close = %v, want unsupported data", err)
		}
		if err := <-done; err == nil {
			t.Fatal("text accepted as binary")
		}
	})
}

func TestWebSocketCopyErrorClosesBothAndWaitsForPeerTimeout(t *testing.T) {
	bubble(t, func(t *testing.T) {
		b, target := net.Pipe()
		defer target.Close()
		done := make(chan error, 1)
		srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, err := AcceptWebSocket(w, r)
			if err != nil {
				t.Error(err)
				return
			}
			done <- netutil.Join(conn, b)
		}))
		peer, err := dial(t, t.Context(), srv)
		if err != nil {
			t.Fatal(err)
		}
		defer peer.Close()
		write(t, peer, "x")
		// One copy is blocked in b.Write, the other in b.Read. The peer
		// deliberately never reads or acknowledges a close frame.
		synctest.Wait()
		start := time.Now()
		if err := b.SetReadDeadline(start); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if n, err := target.Read(make([]byte, 1)); n != 0 || err != io.EOF {
			t.Fatalf("target = %d, %v; want EOF", n, err)
		}
		if elapsed := time.Since(start); elapsed != 0 {
			t.Fatalf("WS handshake delayed closing the TCP side by %v", elapsed)
		}
		if err := <-done; !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("copy error lost: %v", err)
		}
		// Normal close waits for an unresponsive peer, but must stay bounded;
		// do not pin the exact timeout chosen by the WebSocket library.
		if elapsed := time.Since(start); elapsed <= 0 || elapsed > 30*time.Second {
			t.Fatalf("normal close with no reply took %v, want between 0 and 30s", elapsed)
		}
	})
}

// halfConn is one end of a target connection with real half-close, like TCP,
// built from two one-way net.Pipes.
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

// joinedServer joins each accepted stream with a target that sends response
// and then ends, as when a proxied request has finished.
func joinedServer(t *testing.T, response string) (srv *httptest.Server, targetDone, joined <-chan struct{}) {
	targetFinished, joinFinished := make(chan struct{}), make(chan struct{})
	srv = httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(joinFinished)
		conn, err := AcceptWebSocket(w, r)
		if err != nil {
			t.Error(err)
			return
		}
		target, peer := halfPair()
		go func() {
			defer close(targetFinished)
			defer peer.Close()
			if _, err := io.WriteString(peer, response); err != nil {
				t.Error(err)
			}
		}()
		netutil.Join(conn, target)
	}))
	return srv, targetFinished, joinFinished
}

func readString(t *testing.T, r io.Reader, want string) {
	t.Helper()
	got := make([]byte, len(want))
	if n, err := io.ReadFull(r, got); err != nil || string(got) != want {
		t.Fatalf("read %q, %v; want %q", got[:n], err, want)
	}
}

// The close that ends a stream follows the response: a client that reads late,
// after Join has closed its idle stream, still gets the whole response, then
// EOF.
func TestWebSocketCloseFollowsResponse(t *testing.T) {
	bubble(t, func(t *testing.T) {
		srv, targetDone, joined := joinedServer(t, "response")
		conn, err := dial(t, t.Context(), srv)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		<-targetDone
		// Past Join's one-second linger, so that the client reads only after Join
		// has closed the stream.
		synctest.Sleep(2 * time.Second)
		readString(t, conn, "response")
		if n, err := conn.Read(make([]byte, 1)); n != 0 || err != io.EOF {
			t.Fatalf("read after the response = %d, %v; want EOF", n, err)
		}
		<-joined
	})
}

func TestWebSocketConcurrentClose(t *testing.T) {
	bubble(t, func(t *testing.T) {
		done := make(chan error, 1)
		srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			conn, err := AcceptWebSocket(w, r)
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			_, err = io.Copy(io.Discard, conn)
			done <- err
		}))
		peer, err := dial(t, t.Context(), srv)
		if err != nil {
			t.Fatal(err)
		}
		defer peer.Close()

		start := time.Now()
		results := make(chan error, 8)
		var wg sync.WaitGroup
		for range 8 {
			wg.Go(func() { results <- peer.Close() })
		}
		wg.Wait()
		close(results)
		for err := range results {
			if err != nil {
				t.Error(err)
			}
		}
		if err := <-done; err != nil {
			t.Fatalf("normal close did not produce EOF: %v", err)
		}
		if elapsed := time.Since(start); elapsed != 0 {
			t.Errorf("concurrent closes took %v", elapsed)
		}
	})
}

// Between miniray nodes, each end of a stream sends an empty message every
// heartbeatInterval.
func TestWebSocketHeartbeat(t *testing.T) {
	for _, side := range sides {
		t.Run(side.name, func(t *testing.T) {
			bubble(t, func(t *testing.T) {
				_, peer := side.open(t, subprotocol)
				start := time.Now()
				// Stop short of the heartbeat due at the very end.
				ctx, cancel := context.WithTimeout(t.Context(), time.Minute-time.Second)
				defer cancel()
				var got, want []time.Duration
				for {
					_, data, err := peer.Read(ctx)
					if errors.Is(err, context.DeadlineExceeded) {
						break
					}
					if err != nil || len(data) != 0 {
						t.Fatalf("read %q, %v; want empty messages", data, err)
					}
					got = append(got, time.Since(start))
				}
				for at := heartbeatInterval; at < time.Minute; at += heartbeatInterval {
					want = append(want, at)
				}
				if !slices.Equal(got, want) {
					t.Errorf("heartbeats at %v, want %v", got, want)
				}
			})
		})
	}
}

// A heartbeat that falls due while a write is stuck, as behind a peer that
// reads nothing, waits its turn rather than breaking the stream, and the
// heartbeats go on once the peer reads again.
func TestWebSocketHeartbeatWaitsBehindWrites(t *testing.T) {
	bubble(t, func(t *testing.T) {
		srv, accepted := acceptServer(t)
		var base net.Conn
		client := &http.Client{Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				conn, err := srv.Client().Transport.(*http.Transport).DialContext(ctx, network, addr)
				base = conn
				return conn, err
			},
		}}
		peer, _, err := websocket.Dial(t.Context(), "ws://example.com/", &websocket.DialOptions{
			HTTPClient:   client,
			Subprotocols: []string{subprotocol},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer peer.CloseNow()
		conn := <-accepted
		// The in-memory network holds back writes to a full read buffer.
		buffer, ok := base.(interface{ SetReadBufferSize(int) })
		if !ok {
			t.Fatalf("%T cannot limit its read buffer", base)
		}
		buffer.SetReadBufferSize(0)
		wrote := make(chan error, 1)
		go func() {
			_, err := conn.Write([]byte("data"))
			wrote <- err
		}()
		time.Sleep(time.Minute)
		select {
		case err := <-wrote:
			t.Fatalf("write finished while the peer read nothing: %v", err)
		default:
		}
		buffer.SetReadBufferSize(-1)
		if err := <-wrote; err != nil {
			t.Fatalf("write once the peer reads again = %v", err)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 2*heartbeatInterval)
		defer cancel()
		if _, data, err := peer.Read(ctx); err != nil || string(data) != "data" {
			t.Fatalf("peer read %q, %v; want data", data, err)
		}
		// The heartbeats that waited arrive at once; one that comes later shows
		// that they go on.
		for resumed := time.Now(); time.Since(resumed) == 0; {
			if _, data, err := peer.Read(ctx); err != nil || len(data) != 0 {
				t.Fatalf("peer read %q, %v; want heartbeats", data, err)
			}
		}
	})
}

// Heartbeats keep an idle stream between miniray nodes open while both ends
// wait to read.
func TestWebSocketIdleStream(t *testing.T) {
	bubble(t, func(t *testing.T) {
		srv, accepted := acceptServer(t)
		client, err := dial(t, t.Context(), srv)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		server := <-accepted
		atServer, atClient := pendingRead(server), pendingRead(client)
		time.Sleep(time.Hour)
		write(t, client, "up")
		write(t, server, "down")
		if got := <-atServer; got != (readResult{"up", nil}) {
			t.Errorf("server read %q, %v; want up", got.data, got.err)
		}
		if got := <-atClient; got != (readResult{"down", nil}) {
			t.Errorf("client read %q, %v; want down", got.data, got.err)
		}
	})
}

// Once a read has waited silenceTimeout without a byte arriving, the peer is
// gone: the stream ends, and reads and writes fail with errSilent. Whatever
// arrives starts the wait over, and time spent not reading does not count.
func TestWebSocketSilence(t *testing.T) {
	for _, side := range sides {
		t.Run(side.name, func(t *testing.T) {
			bubble(t, func(t *testing.T) {
				conn, peer := side.open(t, subprotocol)
				send := func(data string) {
					t.Helper()
					if err := peer.Write(t.Context(), websocket.MessageBinary, []byte(data)); err != nil {
						t.Fatal(err)
					}
				}
				// The peer sends no heartbeat, but no read waits.
				time.Sleep(time.Hour)
				read := pendingRead(conn)
				time.Sleep(silenceTimeout - time.Second)
				send("data")
				if got := <-read; got != (readResult{"data", nil}) {
					t.Fatalf("read %q, %v; want data", got.data, got.err)
				}
				// Heartbeats, which the read skips, keep it waiting.
				read = pendingRead(conn)
				for range 3 {
					time.Sleep(silenceTimeout - time.Second)
					send("")
				}
				last := time.Now()
				var got readResult
				select {
				case got = <-read:
				case <-time.After(time.Hour):
					t.Fatal("read still waiting an hour after the peer went silent")
				}
				if quiet := time.Since(last); !errors.Is(got.err, errSilent) || quiet != silenceTimeout {
					t.Errorf("read %q, %v after %v of silence; want %v after %v", got.data, got.err, quiet, errSilent, silenceTimeout)
				}
				if _, err := conn.Write([]byte("late")); !errors.Is(err, errSilent) {
					t.Errorf("write after the silence = %v, want %v", err, errSilent)
				}
			})
		})
	}
}

// A peer that does not take up the subprotocol, such as a phone, gets no
// heartbeat, and its silence ends nothing.
func TestWebSocketWithoutSubprotocol(t *testing.T) {
	for _, side := range sides {
		t.Run(side.name, func(t *testing.T) {
			bubble(t, func(t *testing.T) {
				conn, peer := side.open(t)
				read := pendingRead(conn)
				got := make(chan []byte, 1)
				go func() {
					if _, data, err := peer.Read(t.Context()); err == nil {
						got <- data
					}
				}()
				time.Sleep(time.Hour)
				select {
				case data := <-got:
					t.Fatalf("the peer got %q", data)
				default:
				}
				if err := peer.Write(t.Context(), websocket.MessageBinary, []byte("data")); err != nil {
					t.Fatal(err)
				}
				if got := <-read; got != (readResult{"data", nil}) {
					t.Errorf("read %q, %v after an hour of silence; want data", got.data, got.err)
				}
			})
		})
	}
}
