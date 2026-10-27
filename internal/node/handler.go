package node

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httputil"
	"net/netip"
	"net/url"
	"strconv"
	"time"

	"github.com/Qusic/miniray/internal/netutil"
	"github.com/Qusic/miniray/internal/protocol"
	"github.com/Qusic/miniray/internal/transport"
)

// trojanHandler serves Trojan over WebSocket at one path. It upgrades the
// connection, reads the Trojan request under a deadline, and hands the stream
// of a CONNECT request to route.
type trojanHandler struct {
	// path is the only request path served. Every other path, and every path
	// when path is empty, gets an empty 200, like a Caddy site without a
	// matching route, so the endpoint looks like the rest of its front proxy.
	path   string
	trojan protocol.Trojan
	route  routeFunc
	log    *slog.Logger
}

// newTrojanHandler returns a handler for the path and password of the
// credential URL u that hands requests to route and logs to log.
func newTrojanHandler(u url.URL, route routeFunc, log *slog.Logger) trojanHandler {
	password, _ := u.User.Password()
	return trojanHandler{
		path:   u.Path,
		trojan: protocol.NewTrojan(password),
		route:  route,
		log:    log,
	}
}

func (h trojanHandler) serve(l net.Listener) error {
	return serveHTTP(l, h)
}

func (h trojanHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if h.path == "" || r.URL.Path != h.path {
		w.WriteHeader(http.StatusOK)
		return
	}
	log := h.log.With("remote", r.RemoteAddr)
	conn, err := transport.AcceptWebSocket(w, r)
	if err != nil {
		log.Debug("websocket upgrade failed", "err", err)
		return
	}
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(setupTimeout))
	request, err := h.trojan.ReadRequest(conn)
	if err == nil && request.Command != protocol.TrojanConnect {
		err = fmt.Errorf("trojan: unsupported command %#x", request.Command)
	}
	if err != nil {
		if !errors.Is(err, io.EOF) {
			log.Warn("trojan request rejected", "err", err)
		}
		return
	}
	conn.SetReadDeadline(time.Time{})

	if err := h.route(r.Context(), conn, request.Host, request.Port); err != nil {
		log.Warn("request ended", "err", err)
	}
}

// httpProxyHandler serves an HTTP proxy without authentication. A CONNECT
// request has its stream joined with the target, and a request for an http URL
// is passed on to the target's server.
type httpProxyHandler struct {
	proxy *httputil.ReverseProxy // passes requests on, dialing with dial
	dial  dialFunc
	log   *slog.Logger
}

// newHTTPProxyHandler returns a proxy that reaches its targets with dial and
// logs to log.
func newHTTPProxyHandler(dial dialFunc, log *slog.Logger) httpProxyHandler {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, addr string) (net.Conn, error) {
			host, port, err := parseHostPort(addr)
			if err != nil {
				return nil, err
			}
			return dial(ctx, host, port)
		},
		// Every request gets a connection of its own, so none lingers idle.
		DisableKeepAlives: true,
	}
	return httpProxyHandler{
		proxy: &httputil.ReverseProxy{
			// The request names its target already, so Rewrite changes nothing.
			Rewrite:   func(*httputil.ProxyRequest) {},
			Transport: transport,
			ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
				log.Warn("request ended", "remote", r.RemoteAddr, "err", err)
				w.WriteHeader(http.StatusBadGateway)
			},
		},
		dial: dial,
		log:  log,
	}
}

func (h httpProxyHandler) serve(l net.Listener) error {
	return serveHTTP(l, h)
}

func (h httpProxyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodConnect:
		if err := h.connect(w, r); err != nil {
			h.log.Warn("request ended", "remote", r.RemoteAddr, "err", err)
		}
	// Clients send plain http requests in absolute form and use CONNECT for
	// https, so TLS stays between the client and the target.
	case r.URL.Scheme == "http" && r.URL.Host != "":
		h.proxy.ServeHTTP(w, r)
	default:
		w.WriteHeader(http.StatusBadRequest)
	}
}

// connect answers a CONNECT request with 200 once dial reaches its target, and
// joins the client's connection with it. It answers 400 for a malformed target
// and 502 when dial fails.
func (h httpProxyHandler) connect(w http.ResponseWriter, r *http.Request) error {
	host, port, err := parseHostPort(r.Host)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return err
	}
	target, err := h.dial(r.Context(), host, port)
	if err != nil {
		w.WriteHeader(http.StatusBadGateway)
		return err
	}
	defer target.Close()
	w.WriteHeader(http.StatusOK)
	conn, buffered, err := http.NewResponseController(w).Hijack()
	if err != nil {
		return err
	}
	defer conn.Close()
	// What the client sent after its request, already buffered, goes first.
	if _, err := io.CopyN(target, buffered, int64(buffered.Reader.Buffered())); err != nil {
		return err
	}
	return netutil.Join(conn, target)
}

// parseHostPort parses a host:port that a client asks for: an IP literal is an
// IP, and anything else a domain name.
func parseHostPort(text string) (netutil.Address, uint16, error) {
	host, portText, err := net.SplitHostPort(text)
	if err != nil {
		return netutil.Address{}, 0, err
	}
	port, err := strconv.ParseUint(portText, 10, 16)
	if err != nil || port == 0 {
		return netutil.Address{}, 0, fmt.Errorf("invalid port %q in %q", portText, text)
	}
	if ip, err := netip.ParseAddr(host); err == nil {
		address, err := netutil.IPAddress(ip)
		return address, uint16(port), err
	}
	address, err := netutil.DomainAddress(host)
	return address, uint16(port), err
}

// serveHTTP serves h over HTTP on l, giving each client setupTimeout to send
// its request headers.
func serveHTTP(l net.Listener, h http.Handler) error {
	return (&http.Server{Handler: h, ReadHeaderTimeout: setupTimeout}).Serve(l)
}

// acceptPause is how long a TCP server waits after a failed accept, such as
// when the process runs out of file descriptors, before it accepts again.
const acceptPause = time.Second

// tcpHandler joins every connection accepted on a local port with a fixed
// target. A plain TCP connection, unlike a Trojan request, names no target, so
// there is nothing to route: the way to the one target is chosen once, as dial.
type tcpHandler struct {
	host netutil.Address
	port uint16
	dial dialFunc
	log  *slog.Logger
}

// serve handles the connections l accepts until l is closed. Other accept
// errors pass after acceptPause.
func (h tcpHandler) serve(l net.Listener) error {
	for {
		conn, err := l.Accept()
		if errors.Is(err, net.ErrClosed) {
			return err
		}
		if err != nil {
			h.log.Warn("accept failed", "err", err)
			time.Sleep(acceptPause)
			continue
		}
		go h.handle(conn)
	}
}

func (h tcpHandler) handle(conn net.Conn) {
	defer conn.Close()
	target, err := h.dial(context.Background(), h.host, h.port)
	if err == nil {
		err = netutil.Join(conn, target)
	}
	if err != nil {
		h.log.Warn("request ended", "remote", conn.RemoteAddr().String(), "err", err)
	}
}
