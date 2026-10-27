package node

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"

	"github.com/Qusic/miniray/internal/netutil"
	"github.com/Qusic/miniray/internal/protocol"
	"github.com/Qusic/miniray/internal/transport"
	"github.com/Qusic/miniray/internal/tunnel"
)

// dialFunc opens a connection to host:port along one way: directly, or
// through a remote handler.
type dialFunc func(ctx context.Context, host netutil.Address, port uint16) (net.Conn, error)

// dialDirect dials host:port over TCP, resolving domain names with DNS. It
// refuses .tunnel names, so virtual names never reach DNS.
func dialDirect(ctx context.Context, host netutil.Address, port uint16) (net.Conn, error) {
	if !host.IsValid() {
		return nil, errors.New("missing host")
	}
	if tunnel.IsName(host.Domain()) {
		return nil, fmt.Errorf("%s: .tunnel names are not dialed directly", host)
	}
	dialer := net.Dialer{Timeout: setupTimeout}
	return dialer.DialContext(ctx, "tcp", net.JoinHostPort(host.String(), strconv.Itoa(int(port))))
}

// trojanClient is the client side of trojanHandler: it speaks Trojan over
// WebSocket to a remote handler, which it reaches along via.
type trojanClient struct {
	url    url.URL // the remote handler, without the password
	trojan protocol.Trojan
	via    dialFunc
}

// newTrojanClient returns a client for the remote handler at the credential
// URL u, reached along via. The password goes only into Trojan requests: left
// in the URL, it would be sent as HTTP Basic auth.
func newTrojanClient(u url.URL, via dialFunc) trojanClient {
	password, _ := u.User.Password()
	u.User = nil
	return trojanClient{url: u, trojan: protocol.NewTrojan(password), via: via}
}

// dial reaches host:port through the remote handler: via opens the connection
// to the handler, and connect builds the stream on top.
func (c trojanClient) dial(ctx context.Context, host netutil.Address, port uint16) (net.Conn, error) {
	ctx, cancel := context.WithTimeout(ctx, setupTimeout)
	defer cancel()
	serverHost, serverPort, err := hostPort(c.url)
	if err != nil {
		return nil, err
	}
	base, err := c.via(ctx, serverHost, serverPort)
	if err != nil {
		return nil, err
	}
	return c.connect(ctx, base, host, port)
}

// connect asks the remote handler, reached over base, to connect a new stream
// to host:port. It takes over base, and ctx bounds the handshake. Trojan has
// no reply, so a failure there shows up only as the stream ending.
func (c trojanClient) connect(ctx context.Context, base net.Conn, host netutil.Address, port uint16) (net.Conn, error) {
	conn, err := transport.ClientWebSocket(ctx, base, c.url.String())
	if err != nil {
		return nil, err
	}
	request := protocol.TrojanRequest{Command: protocol.TrojanConnect, Host: host, Port: port}
	if err := c.trojan.WriteRequest(conn, request); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

// hostPort returns where the ws:// or wss:// URL u is served, on the scheme's
// default port if u has none.
func hostPort(u url.URL) (netutil.Address, uint16, error) {
	var defaultPort uint16
	switch u.Scheme {
	case "ws":
		defaultPort = 80
	case "wss":
		defaultPort = 443
	default:
		return netutil.Address{}, 0, fmt.Errorf("unsupported scheme %q in %s", u.Scheme, u.Redacted())
	}
	host, err := netutil.ParseAddress(u.Hostname())
	if err != nil {
		return netutil.Address{}, 0, err
	}
	if u.Port() == "" {
		return host, defaultPort, nil
	}
	port, err := strconv.ParseUint(u.Port(), 10, 16)
	if err != nil || port == 0 {
		return netutil.Address{}, 0, fmt.Errorf("invalid port %q in %s", u.Port(), u.Redacted())
	}
	return host, uint16(port), nil
}
