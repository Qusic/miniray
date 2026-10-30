package node

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Qusic/miniray/internal/netutil"
	"github.com/Qusic/miniray/internal/protocol"
)

// dialDirect checks the address before dialing. The context is already
// canceled, so an address that does reach dialing fails with context.Canceled,
// and no case touches the network.
func TestDialDirectChecksAddress(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	for _, tt := range []struct {
		name string
		host netutil.Address
		want string // rejection message, or "" when the address reaches dialing
	}{
		{"zero address", netutil.Address{}, "missing host"},
		{"virtual name", domainAddress(t, "web.hub.tunnel"), ".tunnel names are not dialed directly"},
		{"ipv4", ipAddress(t, "127.0.0.1"), ""},
		{"ipv6", ipAddress(t, "::1"), ""},
	} {
		conn, err := dialDirect(ctx, tt.host, 1)
		if conn != nil {
			conn.Close()
			t.Fatalf("%s: opened a connection", tt.name)
		}
		if tt.want == "" {
			if !errors.Is(err, context.Canceled) {
				t.Errorf("%s: got %v, want the canceled dial", tt.name, err)
			}
		} else if err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("%s: got %v, want %q before dialing", tt.name, err, tt.want)
		}
	}
}

func TestDialDirectConnects(t *testing.T) {
	for _, tt := range []struct {
		name, network, listen string
		host                  netutil.Address
	}{
		{"ipv4", "tcp4", "127.0.0.1:0", ipAddress(t, "127.0.0.1")},
		{"numeric domain", "tcp4", "127.0.0.1:0", domainAddress(t, "127.0.0.1")},
		{"ipv6", "tcp6", "[::1]:0", ipAddress(t, "::1")},
	} {
		t.Run(tt.name, func(t *testing.T) {
			listener, err := net.Listen(tt.network, tt.listen)
			if err != nil {
				if tt.network == "tcp6" {
					t.Skipf("IPv6 loopback unavailable: %v", err)
				}
				t.Fatal(err)
			}
			defer listener.Close()
			if err := listener.(*net.TCPListener).SetDeadline(time.Now().Add(5 * time.Second)); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			conn, err := dialDirect(ctx, tt.host, uint16(listener.Addr().(*net.TCPAddr).Port))
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			peer, err := listener.Accept()
			if err != nil {
				t.Fatal(err)
			}
			peer.Close()
		})
	}
}

// trojanClient is the client side of trojanHandler: the handler dials what the
// client asks for, with the client's own connection made directly or through
// another client.
func TestTrojanClient(t *testing.T) {
	t.Run("direct", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			srv, targets := echoServer(t, protocol.NewTrojan("secret"))
			c := newTrojanClient(mustURL(t, "ws://:secret@example.com/"), memoryDial(srv))
			conn, err := c.dial(t.Context(), domainAddress(t, "ssh.example.com"), 22)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if got := <-targets; got != "ssh.example.com:22" {
				t.Errorf("dialed %s, want ssh.example.com:22", got)
			}
			write(t, conn, []byte("hello"))
			read(t, conn, "hello")
		})
	})
	// A handler at another path refuses the WebSocket handshake.
	t.Run("wrong path", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			srv, targets := echoServer(t, protocol.NewTrojan("secret"))
			c := newTrojanClient(mustURL(t, "ws://:secret@example.com/other"), memoryDial(srv))
			if conn, err := c.dial(t.Context(), domainAddress(t, "ssh.example.com"), 22); conn != nil || err == nil {
				t.Fatalf("dial = %v, %v; want an error", conn, err)
			}
			select {
			case got := <-targets:
				t.Errorf("routed %s without a handshake", got)
			default:
			}
		})
	})
	t.Run("through relay", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			hubSrv, targets := echoServer(t, protocol.NewTrojan("tunnel-secret"))
			relaySrv, relayed := relayServer(t, hubSrv)
			relay := newTrojanClient(mustURL(t, "ws://:relay-secret@relay.example.com/"), memoryDial(relaySrv))
			c := newTrojanClient(mustURL(t, "ws://:tunnel-secret@hub.example.com/"), relay.dial)
			conn, err := c.dial(t.Context(), domainAddress(t, "ssh.example.com"), 22)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			if got := <-relayed; got != "hub.example.com:80" {
				t.Errorf("relay dialed %s, want hub.example.com:80", got)
			}
			if got := <-targets; got != "ssh.example.com:22" {
				t.Errorf("hub dialed %s, want ssh.example.com:22", got)
			}
			write(t, conn, []byte("hello"))
			read(t, conn, "hello")
		})
	})
	// An error through a relay names the handlers on the way, ending with the
	// one that failed.
	t.Run("error through relay", func(t *testing.T) {
		for _, tt := range []struct{ relayPath, hubPath, names string }{
			{"/other", "/", "hub.example.com: relay.example.com: "},
			{"/", "/other", "hub.example.com: "},
		} {
			synctest.Test(t, func(t *testing.T) {
				hubSrv, _ := echoServer(t, protocol.NewTrojan("tunnel-secret"))
				relaySrv, _ := relayServer(t, hubSrv)
				relayURL := "ws://:relay-secret@relay.example.com" + tt.relayPath
				relay := newTrojanClient(mustURL(t, relayURL), memoryDial(relaySrv))
				hubURL := "ws://:tunnel-secret@hub.example.com" + tt.hubPath
				c := newTrojanClient(mustURL(t, hubURL), relay.dial)
				_, err := c.dial(t.Context(), domainAddress(t, "ssh.example.com"), 22)
				// The handlers named, and after them no other.
				rest, named := strings.CutPrefix(fmt.Sprint(err), tt.names)
				if err == nil || !named || strings.Contains(rest, ".example.com: ") {
					t.Errorf("relay at %s, hub at %s: dial = %v, want an error naming %q",
						tt.relayPath, tt.hubPath, err, tt.names)
				}
			})
		}
	})
}

// relayServer serves a relay's Trojan handler, which reports each host:port it
// is asked for and reaches it on hubSrv's network.
func relayServer(t *testing.T, hubSrv *httptest.Server) (*httptest.Server, <-chan string) {
	relayed := make(chan string, 1)
	srv := httptest.NewTestServer(t, trojanHandler{
		path:   "/",
		trojan: protocol.NewTrojan("relay-secret"),
		log:    quiet,
		route: func(ctx context.Context, conn net.Conn, host netutil.Address, port uint16) error {
			relayed <- joinHostPort(host, port)
			target, err := memoryDial(hubSrv)(ctx, host, port)
			if err != nil {
				return err
			}
			return netutil.Join(conn, target)
		},
	})
	return srv, relayed
}

// A client reaches its remote handler where the URL says, on the scheme's
// default port if the URL has none. It speaks only ws and wss, so any other
// scheme fails before dialing, with or without a port, as does an invalid port.
func TestTrojanClientAddress(t *testing.T) {
	for _, tt := range []struct {
		url, want string
		err       string // the error before dialing, if any
	}{
		{"wss://:secret@hub.example.com/tunnel", "hub.example.com:443", ""},
		{"ws://:secret@hub.example.com/tunnel", "hub.example.com:80", ""},
		{"wss://:secret@hub.example.com:8443/tunnel", "hub.example.com:8443", ""},
		{"ws://:secret@[2001:db8::1]/tunnel", "[2001:db8::1]:80", ""},
		{"http://:secret@hub.example.com/tunnel", "", "unsupported scheme"},
		{"https://:secret@hub.example.com:8443/tunnel", "", "unsupported scheme"},
		{"ws://:secret@hub.example.com:0/tunnel", "", "invalid port"},
		{"ws://:secret@hub.example.com:65536/tunnel", "", "invalid port"},
	} {
		reached := errors.New("reached the remote handler")
		got := ""
		c := newTrojanClient(mustURL(t, tt.url), func(_ context.Context, host netutil.Address, port uint16) (net.Conn, error) {
			got = joinHostPort(host, port)
			return nil, reached
		})
		_, err := c.dial(t.Context(), domainAddress(t, "ssh.example.com"), 22)
		if tt.err != "" {
			if got != "" || err == nil || !strings.Contains(err.Error(), tt.err) {
				t.Errorf("%s: dialed %q, %v; want %q before dialing", tt.url, got, err, tt.err)
			}
		} else if !errors.Is(err, reached) || got != tt.want {
			t.Errorf("%s: dialed %s, %v; want %s", tt.url, got, err, tt.want)
		}
	}
}
