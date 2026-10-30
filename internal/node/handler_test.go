package node

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Qusic/miniray/internal/netutil"
	"github.com/Qusic/miniray/internal/protocol"
	"github.com/Qusic/miniray/internal/transport"
)

// dialServer opens a WebSocket stream to srv.
func dialServer(t *testing.T, srv *httptest.Server) net.Conn {
	t.Helper()
	base, err := memoryDial(srv)(t.Context(), domainAddress(t, "example.com"), 80)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := transport.ClientWebSocket(t.Context(), base, "ws://example.com/")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

func request(t *testing.T, w io.Writer, p protocol.Trojan, command protocol.TrojanCommand, host string, port uint16) {
	t.Helper()
	if err := p.WriteRequest(w, protocol.TrojanRequest{Command: command, Host: domainAddress(t, host), Port: port}); err != nil {
		t.Fatal(err)
	}
}

func TestTrojanHandler(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := protocol.NewTrojan("secret")
		srv, targets := echoServer(t, p)

		// A header alone must reach dial, without waiting for payload.
		conn := dialServer(t, srv)
		request(t, conn, p, protocol.TrojanConnect, "ssh.example.com", 22)
		if got := <-targets; got != "ssh.example.com:22" {
			t.Errorf("dialed %s, want ssh.example.com:22", got)
		}
		write(t, conn, []byte("hello"))
		read(t, conn, "hello")
		if err := conn.Close(); err != nil {
			t.Fatal(err)
		}

		// Header and payload in one message, then split across messages.
		for _, split := range []bool{false, true} {
			conn := dialServer(t, srv)
			var msg bytes.Buffer
			request(t, &msg, p, protocol.TrojanConnect, "example.com", 443)
			msg.WriteString("hello")
			wire := msg.Bytes()
			if split {
				write(t, conn, wire[:10])
				write(t, conn, wire[10:60])
				wire = wire[60:]
			}
			write(t, conn, wire)
			if got := <-targets; got != "example.com:443" {
				t.Errorf("dialed %s, want example.com:443", got)
			}
			read(t, conn, "hello")
			if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
		}
	})
}

func TestTrojanHandlerServesOnlyItsPath(t *testing.T) {
	handler := trojanHandler{path: "/tunnel", log: quiet}
	// Whether each path reaches the WebSocket upgrade, which refuses these plain
	// requests with a 4xx. Other paths get an empty 200.
	for path, reaches := range map[string]bool{
		"/tunnel":   true,
		"/%74unnel": true, // the decoded path is compared
		"/":         false,
		"/tunnel/":  false,
		"/tunnelx":  false,
		"/TUNNEL":   false,
	} {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if reaches {
			if w.Code/100 != 4 {
				t.Errorf("GET %s = %d, want the upgrade to refuse a plain request", path, w.Code)
			}
		} else if w.Code != http.StatusOK || w.Body.Len() != 0 {
			t.Errorf("GET %s = %d with %d body bytes, want an empty 200", path, w.Code, w.Body.Len())
		}
	}
	// Without a path nothing is served, not even a request whose own path is
	// empty, like an authority-form CONNECT.
	w := httptest.NewRecorder()
	trojanHandler{}.ServeHTTP(w, httptest.NewRequest(http.MethodConnect, "example.com:443", nil))
	if w.Code != http.StatusOK || w.Body.Len() != 0 {
		t.Errorf("handler without a path = %d with %d body bytes, want an empty 200", w.Code, w.Body.Len())
	}
}

func TestTrojanHandlerRejectsRequest(t *testing.T) {
	p := protocol.NewTrojan("secret")
	tests := []struct {
		name    string
		client  protocol.Trojan
		command protocol.TrojanCommand // zero sends no request
		after   time.Duration
	}{
		{"wrong password", protocol.NewTrojan("other"), protocol.TrojanConnect, 0},
		// Only CONNECT is executed.
		{"UDP associate", p, protocol.TrojanUDPAssociate, 0},
		{"unknown command", p, 0xff, 0},
		{"no request", p, 0, setupTimeout},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				finished := make(chan struct{})
				routed := make(chan struct{}, 1)
				handler := trojanHandler{
					path:   "/",
					trojan: p,
					log:    quiet,
					route: func(context.Context, net.Conn, netutil.Address, uint16) error {
						routed <- struct{}{}
						return errors.New("unexpected route")
					},
				}
				srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					defer close(finished)
					handler.ServeHTTP(w, r)
				}))
				conn := dialServer(t, srv)
				start := time.Now()
				if tt.command != 0 {
					request(t, conn, tt.client, tt.command, "example.com", 443)
				}
				if n, err := conn.Read(make([]byte, 1)); n != 0 || err == nil {
					t.Fatalf("rejected connection read = %d, %v; want close without payload", n, err)
				}
				if elapsed := time.Since(start); elapsed != tt.after {
					t.Errorf("closed after %v, want %v", elapsed, tt.after)
				}
				<-finished // No route can run after ServeHTTP returns.
				select {
				case <-routed:
					t.Error("routed a rejected request")
				default:
				}
			})
		})
	}
}

// An HTTP proxy joins the stream of a CONNECT request with its target, bytes
// sent ahead of the answer first, and passes a request for an http URL on to
// its server. It answers 502 when the target cannot be reached, and 400,
// without dialing, to a malformed target or any other request.
func TestHTTPProxyHandler(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		web := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			io.WriteString(w, "served "+r.Host+r.URL.Path)
		}))
		var dialed []string
		var peer net.Conn // the target's end of the CONNECT stream
		proxy := httptest.NewTestServer(t, newHTTPProxyHandler(func(ctx context.Context, host netutil.Address, port uint16) (net.Conn, error) {
			kind := "domain"
			if host.IP().IsValid() {
				kind = "ip"
			}
			dialed = append(dialed, kind+" "+joinHostPort(host, port))
			switch host.String() {
			case "ssh.example.com":
				var conn net.Conn
				conn, peer = net.Pipe()
				return conn, nil
			case "web.example.com":
				return memoryDial(web)(ctx, host, port)
			}
			return nil, errors.New("unreachable")
		}, quiet))
		dialProxy := func() net.Conn {
			conn, err := memoryDial(proxy)(t.Context(), domainAddress(t, "proxy.example.com"), 3128)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { conn.Close() })
			return conn
		}

		status, reader := connect(t, dialProxy(), "ssh.example.com:22", "early")
		if status != http.StatusOK {
			t.Fatalf("CONNECT = %d, want 200", status)
		}
		read(t, peer, "early")
		write(t, peer, []byte("hello"))
		read(t, reader, "hello")
		// The target hanging up ends the stream.
		peer.Close()
		if n, err := reader.Read(make([]byte, 1)); n != 0 || err != io.EOF {
			t.Errorf("client read %d, %v after the target hung up; want EOF", n, err)
		}

		send := func(raw string) (int, string) {
			conn := dialProxy()
			write(t, conn, []byte(raw))
			resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer resp.Body.Close()
			body, err := io.ReadAll(resp.Body)
			if err != nil {
				t.Fatal(err)
			}
			return resp.StatusCode, string(body)
		}
		// The server sees the host and path the client asked for.
		status, body := send("GET http://web.example.com/hello HTTP/1.1\r\nHost: web.example.com\r\n\r\n")
		if want := "served web.example.com/hello"; status != http.StatusOK || body != want {
			t.Errorf("GET through the proxy = %d %q, want 200 %q", status, body, want)
		}
		for _, raw := range []string{
			"CONNECT nowhere.example.com:22 HTTP/1.1\r\nHost: nowhere.example.com:22\r\n\r\n",
			"CONNECT [2001:db8::1]:22 HTTP/1.1\r\nHost: [2001:db8::1]:22\r\n\r\n",
			"GET http://nowhere.example.com/ HTTP/1.1\r\nHost: nowhere.example.com\r\n\r\n",
		} {
			if status, _ := send(raw); status != http.StatusBadGateway {
				t.Errorf("%q = %d, want 502", raw, status)
			}
		}
		for _, raw := range []string{
			"CONNECT nowhere.example.com HTTP/1.1\r\nHost: nowhere.example.com\r\n\r\n",
			"CONNECT nowhere.example.com:0 HTTP/1.1\r\nHost: nowhere.example.com:0\r\n\r\n",
			"GET /hello HTTP/1.1\r\nHost: proxy.example.com\r\n\r\n",
			"GET https://web.example.com/ HTTP/1.1\r\nHost: web.example.com\r\n\r\n",
		} {
			if status, _ := send(raw); status != http.StatusBadRequest {
				t.Errorf("%q = %d, want 400", raw, status)
			}
		}
		// The requests answered 400 dial nothing, and an IP literal is dialed
		// as an IP.
		want := []string{
			"domain ssh.example.com:22",
			"domain web.example.com:80",
			"domain nowhere.example.com:22",
			"ip [2001:db8::1]:22",
			"domain nowhere.example.com:80",
		}
		if !slices.Equal(dialed, want) {
			t.Errorf("dialed %q, want %q", dialed, want)
		}
	})
}

// The stream of a CONNECT request is the client: once it has sent its request
// and half-closed, a target that cannot half-close may still take a while to
// respond.
func TestHTTPProxyConnectWaitsForResponse(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		target, server := net.Pipe()
		defer server.Close()
		proxy := httptest.NewTestServer(t, newHTTPProxyHandler(func(context.Context, netutil.Address, uint16) (net.Conn, error) {
			return target, nil
		}, quiet))
		conn, err := memoryDial(proxy)(t.Context(), domainAddress(t, "proxy.example.com"), 3128)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		status, reader := connect(t, conn, "ssh.example.com:22", "request")
		if status != http.StatusOK {
			t.Fatalf("CONNECT = %d, want 200", status)
		}
		if err := conn.(interface{ CloseWrite() error }).CloseWrite(); err != nil {
			t.Fatal(err)
		}
		read(t, server, "request")
		// Join gives a response half a minute, and lingers only about a second.
		time.Sleep(10 * time.Second)
		write(t, server, []byte("response"))
		read(t, reader, "response")
	})
}

// testListener accepts the connections and reports the errors that a test
// sends it, until the test closes it.
type testListener struct {
	conns  chan net.Conn
	errs   chan error
	closed chan struct{}
}

func (l testListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.conns:
		return conn, nil
	case err := <-l.errs:
		return nil, err
	case <-l.closed:
		return nil, net.ErrClosed
	}
}

func (l testListener) Close() error {
	close(l.closed)
	return nil
}

func (l testListener) Addr() net.Addr { return &net.TCPAddr{} }

// tcpHandler joins each connection it accepts with its fixed target, closes
// the connection at once when the target cannot be reached, and keeps
// accepting after a failed accept until its listener is closed.
func TestTCPHandler(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var dialed []string
		unreachable := false
		h := tcpHandler{
			host: domainAddress(t, "ssh.example.com"),
			port: 22,
			dial: func(_ context.Context, host netutil.Address, port uint16) (net.Conn, error) {
				dialed = append(dialed, joinHostPort(host, port))
				if unreachable {
					return nil, errors.New("unreachable")
				}
				return echo(), nil
			},
			log: quiet,
		}
		l := testListener{conns: make(chan net.Conn), errs: make(chan error), closed: make(chan struct{})}
		served := make(chan error, 1)
		go func() { served <- h.serve(l) }()
		accept := func() net.Conn {
			conn, peer := net.Pipe()
			l.conns <- peer
			return conn
		}

		conn := accept()
		write(t, conn, []byte("hello"))
		read(t, conn, "hello")
		conn.Close()

		unreachable = true
		conn = accept()
		if n, err := conn.Read(make([]byte, 1)); n != 0 || err != io.EOF {
			t.Errorf("connection to an unreachable target read %d, %v; want it closed", n, err)
		}
		conn.Close()

		unreachable = false
		start := time.Now()
		l.errs <- errors.New("too many open files")
		conn = accept()
		if elapsed := time.Since(start); elapsed != acceptPause {
			t.Errorf("accepted again after %v, want %v", elapsed, acceptPause)
		}
		write(t, conn, []byte("again"))
		read(t, conn, "again")
		conn.Close()

		l.Close()
		if err := <-served; !errors.Is(err, net.ErrClosed) {
			t.Errorf("serve = %v after its listener closed, want %v", err, net.ErrClosed)
		}
		if want := []string{"ssh.example.com:22", "ssh.example.com:22", "ssh.example.com:22"}; !slices.Equal(dialed, want) {
			t.Errorf("dialed %q, want %q", dialed, want)
		}
	})
}

// The accepted connection is the client: once it has sent its request and
// half-closed, a target that cannot half-close may still take a while to
// respond.
func TestTCPHandlerWaitsForResponse(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		target, server := net.Pipe()
		defer server.Close()
		h := tcpHandler{
			host: domainAddress(t, "ssh.example.com"),
			port: 22,
			dial: func(context.Context, netutil.Address, uint16) (net.Conn, error) { return target, nil },
			log:  quiet,
		}
		client, accepted := halfPair()
		defer client.Close()
		go h.handle(accepted)

		write(t, client, []byte("request"))
		if err := client.CloseWrite(); err != nil {
			t.Fatal(err)
		}
		read(t, server, "request")
		// Join gives a response half a minute, and lingers only about a second.
		time.Sleep(10 * time.Second)
		write(t, server, []byte("response"))
		read(t, client, "response")
	})
}

// Handlers log the start and end of each request at debug level, so that only
// debug logs every request. The lines name the client and the target, and the
// last says how long the request took and why it ended.
func TestHandlerLogs(t *testing.T) {
	refused := errors.New("refused")
	// A refusal takes a second, for the request to take as long.
	refuse := func(context.Context, netutil.Address, uint16) (net.Conn, error) {
		time.Sleep(time.Second)
		return nil, refused
	}
	// trojan serves one request for ssh.example.com:22 from a client with
	// password, and refuses it in route.
	trojan := func(password string) func(*testing.T, *slog.Logger) {
		return func(t *testing.T, log *slog.Logger) {
			handler := trojanHandler{
				path:   "/",
				trojan: protocol.NewTrojan("secret"),
				log:    log,
				route: func(ctx context.Context, _ net.Conn, host netutil.Address, port uint16) error {
					_, err := refuse(ctx, host, port)
					return err
				},
			}
			finished := make(chan struct{})
			srv := httptest.NewTestServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				defer close(finished)
				handler.ServeHTTP(w, r)
			}))
			conn := dialServer(t, srv)
			client := protocol.NewTrojan(password)
			request(t, conn, client, protocol.TrojanConnect, "ssh.example.com", 22)
			<-finished
		}
	}
	refusedLog := `level=DEBUG msg="request started" remote=* target=ssh.example.com:22
level=DEBUG msg="request ended" remote=* target=ssh.example.com:22 duration=1s err=refused
`
	for _, tt := range []struct {
		name  string
		serve func(*testing.T, *slog.Logger) // serves one request, logging to the logger
		want  string
	}{
		{"trojan", trojan("secret"), refusedLog},
		{"trojan with the wrong password", trojan("other"),
			`level=DEBUG msg="trojan request rejected" remote=* err="trojan: wrong password"` + "\n"},
		{"HTTP proxy CONNECT", func(t *testing.T, log *slog.Logger) {
			r := httptest.NewRequest(http.MethodConnect, "ssh.example.com:22", nil)
			newHTTPProxyHandler(refuse, log).ServeHTTP(httptest.NewRecorder(), r)
		}, refusedLog},
		{"HTTP proxy for an http URL", func(t *testing.T, log *slog.Logger) {
			r := httptest.NewRequest(http.MethodGet, "http://ssh.example.com:22/", nil)
			newHTTPProxyHandler(refuse, log).ServeHTTP(httptest.NewRecorder(), r)
		}, refusedLog},
		{"TCP", func(t *testing.T, log *slog.Logger) {
			conn, peer := net.Pipe()
			defer peer.Close()
			handler := tcpHandler{
				host: domainAddress(t, "ssh.example.com"),
				port: 22,
				dial: refuse,
				log:  log,
			}
			handler.handle(conn)
		}, refusedLog},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var logs bytes.Buffer
				tt.serve(t, textLog(&logs))
				if got := logs.String(); got != tt.want {
					t.Errorf("log:\n%s\nwant:\n%s", got, tt.want)
				}
			})
		})
	}
}
