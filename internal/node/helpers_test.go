package node

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strconv"
	"testing"

	"github.com/Qusic/miniray/internal/netutil"
	"github.com/Qusic/miniray/internal/protocol"
)

// quiet discards the logs of handlers, hubs, and spokes under test.
var quiet = slog.New(slog.DiscardHandler)

// echo returns a connection to a target that echoes its stream back.
func echo() net.Conn {
	conn, peer := net.Pipe()
	go func() {
		io.Copy(peer, peer)
		peer.Close()
	}()
	return conn
}

// echoServer reports each requested target and joins the stream with an echo.
func echoServer(t *testing.T, trojan protocol.Trojan) (*httptest.Server, <-chan string) {
	targets := make(chan string, 1)
	srv := httptest.NewTestServer(t, trojanHandler{
		path:   "/",
		trojan: trojan,
		log:    quiet,
		route: func(_ context.Context, conn net.Conn, host netutil.Address, port uint16) error {
			targets <- net.JoinHostPort(host.String(), strconv.Itoa(int(port)))
			return netutil.Join(conn, echo())
		},
	})
	return srv, targets
}

// memoryDial dials srv over its in-memory network, whatever the address.
func memoryDial(srv *httptest.Server) dialFunc {
	dial := srv.Client().Transport.(*http.Transport).DialContext
	return func(ctx context.Context, host netutil.Address, port uint16) (net.Conn, error) {
		return dial(ctx, "tcp", net.JoinHostPort(host.String(), strconv.Itoa(int(port))))
	}
}

func mustURL(t *testing.T, text string) url.URL {
	t.Helper()
	u, err := url.Parse(text)
	if err != nil {
		t.Fatal(err)
	}
	return *u
}

func write(t *testing.T, w io.Writer, data []byte) {
	t.Helper()
	if n, err := w.Write(data); err != nil || n != len(data) {
		t.Fatalf("write = %d, %v", n, err)
	}
}

func read(t *testing.T, r io.Reader, want string) {
	t.Helper()
	got := make([]byte, len(want))
	n, err := io.ReadFull(r, got)
	if err != nil || string(got) != want {
		t.Fatalf("read %q (%d bytes), %v; want %q", got[:n], n, err, want)
	}
}

func domainAddress(t *testing.T, name string) netutil.Address {
	t.Helper()
	address, err := netutil.DomainAddress(name)
	if err != nil {
		t.Fatal(err)
	}
	return address
}

func ipAddress(t *testing.T, text string) netutil.Address {
	t.Helper()
	address, err := netutil.IPAddress(netip.MustParseAddr(text))
	if err != nil {
		t.Fatal(err)
	}
	return address
}

// connect sends a CONNECT request for target over conn, with early written
// right after it, and returns the status of the answer and a reader for what
// follows it.
func connect(t *testing.T, conn net.Conn, target, early string) (int, *bufio.Reader) {
	t.Helper()
	write(t, conn, []byte("CONNECT "+target+" HTTP/1.1\r\nHost: "+target+"\r\n\r\n"+early))
	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode, reader
}
