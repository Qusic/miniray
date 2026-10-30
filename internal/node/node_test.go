package node

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Qusic/miniray/internal/config"
	"github.com/Qusic/miniray/internal/netutil"
	"github.com/Qusic/miniray/internal/protocol"
)

// serving runs s on a local port and returns its address.
func serving(t *testing.T, s server) string {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	go s.serve(listener)
	return listener.Addr().String()
}

// echoing runs a TCP echo server on a local port and returns its address.
func echoing(t *testing.T) *net.TCPAddr {
	t.Helper()
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { listener.Close() })
	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				io.Copy(conn, conn)
				conn.Close()
			}()
		}
	}()
	return listener.Addr().(*net.TCPAddr)
}

// addresses lists each server by its name and address.
func addresses(servers []server) []string {
	var got []string
	for _, s := range servers {
		got = append(got, s.name+" "+s.addr)
	}
	return got
}

func credential(t *testing.T, text string) config.Credential {
	t.Helper()
	var c config.Credential
	if err := c.UnmarshalText([]byte(text)); err != nil {
		t.Fatal(err)
	}
	return c
}

// A node with nothing to run says so instead of idling.
func TestRunWithNothingToRun(t *testing.T) {
	err := Run(config.Config{Name: "desk"})
	if err == nil || !strings.Contains(err.Error(), "nothing to run") {
		t.Errorf("Run = %v, want nothing to run", err)
	}
}

// On the hub, assemble gives the tunnel ingress the route to the hub's own
// aliases, while the public ingress dials ordinary hosts but refuses every
// .tunnel name. option.local binds both to 127.0.0.1 instead of every
// interface, and the hub runs no spoke.
func TestAssembleIngresses(t *testing.T) {
	target, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer target.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		for {
			conn, err := target.Accept()
			if err != nil {
				return
			}
			accepted <- conn
		}
	}()
	cfg := config.Config{
		Name: "hub",
		Credential: config.Credentials{
			Tunnel: credential(t, "wss://:tunnel-secret@hub.example.com/tunnel"),
			Relay:  credential(t, "wss://:relay-secret@hub.example.com/relay"),
		},
		Node: config.Node{
			Ingress: config.Ingress{Tunnel: 8080, Public: 8081},
			Egress:  map[netutil.Label]netutil.Address{"web": ipAddress(t, "127.0.0.1")},
		},
	}
	servers, _ := assemble(cfg)
	if got, want := addresses(servers), []string{"ingress.tunnel :8080", "ingress.public :8081"}; !slices.Equal(got, want) {
		t.Errorf("servers %q, want %q", got, want)
	}
	cfg.Option.Local = true
	servers, spoke := assemble(cfg)
	if spoke != nil {
		t.Error("the hub registers with itself")
	}
	if got, want := addresses(servers), []string{"ingress.tunnel 127.0.0.1:8080", "ingress.public 127.0.0.1:8081"}; !slices.Equal(got, want) {
		t.Fatalf("local servers %q, want %q", got, want)
	}

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	web, port := domainAddress(t, "web.hub.tunnel"), uint16(target.Addr().(*net.TCPAddr).Port)
	tunnel := newTrojanClient(mustURL(t, "ws://:tunnel-secret@"+serving(t, servers[0])+"/tunnel"), dialDirect)
	conn, err := tunnel.dial(ctx, web, port)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	select {
	case peer := <-accepted:
		write(t, peer, []byte("hello"))
		read(t, conn, "hello")
		peer.Close()
	case <-ctx.Done():
		t.Fatal("the tunnel ingress did not reach the hub's own alias")
	}

	public := newTrojanClient(mustURL(t, "ws://:relay-secret@"+serving(t, servers[1])+"/relay"), dialDirect)
	conn, err = public.dial(ctx, web, port)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	if n, err := conn.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		t.Errorf("public ingress read %d, %v; want the stream ended at once", n, err)
	}

	conn, err = public.dial(ctx, ipAddress(t, "127.0.0.1"), port)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	select {
	case peer := <-accepted:
		write(t, peer, []byte("hello"))
		read(t, conn, "hello")
		peer.Close()
	case <-ctx.Done():
		t.Fatal("the public ingress did not reach an ordinary host")
	}
}

// private and proxy are HTTP proxies: private dials straight from this node,
// so it refuses .tunnel names, while proxy reaches every target through the
// relay's public ingress.
func TestAssembleHTTPProxies(t *testing.T) {
	target := echoing(t)
	relayed := make(chan string, 1)
	relay := httptest.NewServer(trojanHandler{
		path:   "/relay",
		trojan: protocol.NewTrojan("relay-secret"),
		log:    quiet,
		// The relay echoes what it gets, then hangs up.
		route: func(_ context.Context, conn net.Conn, host netutil.Address, port uint16) error {
			relayed <- joinHostPort(host, port)
			_, err := io.Copy(conn, io.LimitReader(conn, int64(len("hello"))))
			return err
		},
	})
	defer relay.Close()

	servers, _ := assemble(config.Config{
		Name:       "desk",
		Credential: config.Credentials{Relay: credential(t, "ws://:relay-secret@"+relay.Listener.Addr().String()+"/relay")},
		Node:       config.Node{Ingress: config.Ingress{Private: 3128, Proxy: 3129}},
	})
	if got, want := addresses(servers), []string{"ingress.private :3128", "ingress.proxy :3129"}; !slices.Equal(got, want) {
		t.Fatalf("servers %q, want %q", got, want)
	}
	private, proxy := serving(t, servers[0]), serving(t, servers[1])

	for _, tt := range []struct {
		name, proxy, target string
		status              int
		relayed             string // what the relay is asked for, if anything
	}{
		{"private", private, target.String(), http.StatusOK, ""},
		{"private virtual name", private, "web.hub.tunnel:80", http.StatusBadGateway, ""},
		{"proxy", proxy, "ssh.example.com:22", http.StatusOK, "ssh.example.com:22"},
	} {
		conn, err := net.DialTimeout("tcp", tt.proxy, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		status, reader := connect(t, conn, tt.target, "")
		if status != tt.status {
			t.Errorf("%s: CONNECT %s = %d, want %d", tt.name, tt.target, status, tt.status)
		} else if status == http.StatusOK {
			write(t, conn, []byte("hello"))
			read(t, reader, "hello")
		}
		conn.Close()
		select {
		case got := <-relayed:
			if got != tt.relayed {
				t.Errorf("%s: the relay was asked for %s, want %q", tt.name, got, tt.relayed)
			}
		default:
			if tt.relayed != "" {
				t.Errorf("%s: bypassed the relay", tt.name)
			}
		}
	}
}

// forward and link servers listen on their local ports in a fixed order. A
// forward dials its egress host directly; a link reaches its alias by virtual
// name, from a node other than the hub through the hub's tunnel ingress.
func TestAssembleForwardAndLink(t *testing.T) {
	web := echoing(t)
	targets := make(chan string, 1)
	hub := httptest.NewServer(trojanHandler{
		path:   "/tunnel",
		trojan: protocol.NewTrojan("tunnel-secret"),
		log:    quiet,
		route: func(_ context.Context, conn net.Conn, host netutil.Address, port uint16) error {
			targets <- joinHostPort(host, port)
			return netutil.Join(conn, echo())
		},
	})
	defer hub.Close()

	servers, _ := assemble(config.Config{
		Name:       "lab",
		Credential: config.Credentials{Tunnel: credential(t, "ws://:tunnel-secret@"+hub.Listener.Addr().String()+"/tunnel")},
		Node: config.Node{
			Egress:  map[netutil.Label]netutil.Address{"web": ipAddress(t, "127.0.0.1")},
			Forward: map[netutil.Label][]config.PortMapping{"web": {{Listen: 2001, Target: uint16(web.Port)}}},
			Link: map[netutil.Label]map[netutil.Label][]config.PortMapping{
				"other": {"nas": {{Listen: 2003, Target: 22}}},
				"hub":   {"web": {{Listen: 2004, Target: 80}, {Listen: 2002, Target: 8080}}},
			},
			Option: config.Options{Local: true},
		},
	})
	want := []string{"forward.web 127.0.0.1:2001", "link.hub.web 127.0.0.1:2004", "link.hub.web 127.0.0.1:2002", "link.other.nas 127.0.0.1:2003"}
	if got := addresses(servers); !slices.Equal(got, want) {
		t.Fatalf("servers %q, want %q", got, want)
	}

	for _, tt := range []struct {
		server server
		hub    string // what the hub is asked for, if the server goes through it
	}{
		{servers[0], ""},
		{servers[3], "nas.other.tunnel:22"},
	} {
		conn, err := net.DialTimeout("tcp", serving(t, tt.server), 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		conn.SetDeadline(time.Now().Add(5 * time.Second))
		write(t, conn, []byte("hello"))
		read(t, conn, "hello")
		conn.Close()
		select {
		case got := <-targets:
			if got != tt.hub {
				t.Errorf("%s: the hub was asked for %s, want %q", tt.server.name, got, tt.hub)
			}
		default:
			if tt.hub != "" {
				t.Errorf("%s: bypassed the hub", tt.server.name)
			}
		}
	}
}

// Every other node with egress aliases runs a spoke. It reaches the hub's
// tunnel ingress at the address the credential gives, directly or with
// option.relay through the relay's public ingress, and neither ingress sees a
// password outside the Trojan request. It dials the node's own aliases.
func TestAssembleSpoke(t *testing.T) {
	noAuth := func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if auth := r.Header.Get("Authorization"); auth != "" {
				t.Errorf("%s received Authorization: %s", r.URL.Path, auth)
			}
			h.ServeHTTP(w, r)
		})
	}
	targets := make(chan string, 1)
	hub := httptest.NewServer(noAuth(trojanHandler{
		path:   "/tunnel",
		trojan: protocol.NewTrojan("tunnel-secret"),
		log:    quiet,
		route: func(_ context.Context, conn net.Conn, host netutil.Address, port uint16) error {
			targets <- joinHostPort(host, port)
			return netutil.Join(conn, echo())
		},
	}))
	defer hub.Close()
	relayed := make(chan string, 1)
	relay := httptest.NewServer(noAuth(trojanHandler{
		path:   "/relay",
		trojan: protocol.NewTrojan("relay-secret"),
		log:    quiet,
		route: func(ctx context.Context, conn net.Conn, host netutil.Address, port uint16) error {
			relayed <- joinHostPort(host, port)
			target, err := dialDirect(ctx, host, port)
			if err != nil {
				return err
			}
			return netutil.Join(conn, target)
		},
	}))
	defer relay.Close()

	for _, viaRelay := range []bool{false, true} {
		cfg := config.Config{
			Name:       "spoke",
			Credential: config.Credentials{Tunnel: credential(t, "ws://:tunnel-secret@"+hub.Listener.Addr().String()+"/tunnel")},
			Node:       config.Node{Egress: map[netutil.Label]netutil.Address{"web": ipAddress(t, "192.0.2.1")}},
		}
		if viaRelay {
			cfg.Credential.Relay = credential(t, "ws://:relay-secret@"+relay.Listener.Addr().String()+"/relay")
			cfg.Option.Relay = true
		}
		servers, spoke := assemble(cfg)
		if len(servers) != 0 || spoke == nil || spoke.Name != "spoke" {
			t.Fatalf("relay %t: assembled %d servers and spoke %+v, want only the spoke", viaRelay, len(servers), spoke)
		}
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		conn, err := spoke.DialHub(ctx, domainAddress(t, "ssh.example.com"), 22)
		cancel()
		if err != nil {
			t.Fatalf("relay %t: %v", viaRelay, err)
		}
		write(t, conn, []byte("hello"))
		read(t, conn, "hello")
		conn.Close()
		if got := <-targets; got != "ssh.example.com:22" {
			t.Errorf("relay %t: hub dialed %s, want ssh.example.com:22", viaRelay, got)
		}
		// The relay, if used, dialed the hub before the hub saw the request.
		select {
		case got := <-relayed:
			if !viaRelay || got != hub.Listener.Addr().String() {
				t.Errorf("relay %t: relay dialed %s", viaRelay, got)
			}
		default:
			if viaRelay {
				t.Error("option.relay bypassed the relay")
			}
		}
		// The spoke's own alias reaches dialing; the context is already canceled.
		canceled, cancel := context.WithCancel(t.Context())
		cancel()
		if _, err := spoke.DialAlias(canceled, "web", 22); !errors.Is(err, context.Canceled) {
			t.Errorf("relay %t: spoke dialing its alias = %v, want the canceled dial", viaRelay, err)
		}
	}
}
