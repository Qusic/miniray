package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Qusic/miniray/internal/netutil"
	"github.com/coder/websocket"
)

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

func TestWebSocketStream(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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
			synctest.Test(t, func(t *testing.T) {
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
}

// ClientWebSocket's connection leads to one host, so a redirect fails the
// handshake without a second request.
func TestClientWebSocketRefusesRedirect(t *testing.T) {
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
}

func TestWebSocketAcceptLifetime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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
	synctest.Test(t, func(t *testing.T) {
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
	synctest.Test(t, func(t *testing.T) {
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

func TestWebSocketWaitsForClient(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		srv, targetDone, joined := joinedServer(t, "response")
		conn, err := dial(t, t.Context(), srv)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		<-targetDone
		synctest.Wait()

		// The target's EOF must not end the stream before the client reads.
		select {
		case <-joined:
			t.Fatal("stream ended before the client read the response")
		default:
		}
		synctest.Sleep(2 * time.Second)
		select {
		case <-joined:
			t.Fatal("stream ended while the client was idle")
		default:
		}
		readString(t, conn, "response")
		// An early WS close can leave the server waiting for an acknowledgment,
		// so server liveness alone does not prove the client still has a stream.
		type result struct {
			n   int
			err error
		}
		next := make(chan result, 1)
		go func() {
			var b [1]byte
			n, err := conn.Read(b[:])
			next <- result{n, err}
		}()
		synctest.Wait()
		select {
		case got := <-next:
			t.Fatalf("stream ended before the client closed: %d bytes, %v", got.n, got.err)
		default:
		}
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}
		if got := <-next; got.n != 0 || got.err == nil {
			t.Errorf("read after client close = %d, %v; want connection error", got.n, got.err)
		}
		<-joined
	})
}

func TestWebSocketConcurrentClose(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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
