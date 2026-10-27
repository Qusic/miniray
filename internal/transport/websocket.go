// Package transport adapts network transports to net.Conn.
package transport

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"

	"github.com/coder/websocket"
)

// AcceptWebSocket upgrades r to a binary WebSocket stream. On failure it has
// already written an HTTP response. The connection ends when r.Context is
// canceled, including when the handler returns: keep the handler alive for as
// long as the stream is used.
func AcceptWebSocket(w http.ResponseWriter, r *http.Request) (net.Conn, error) {
	ws, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		// Proxy clients authenticate inside the stream, not via browser cookies.
		InsecureSkipVerify: true,
	})
	if err != nil {
		return nil, err
	}
	return wrapWebSocket(r.Context(), ws), nil
}

// ClientWebSocket runs the client side of the handshake for a ws:// or wss://
// url over conn, with TLS first for wss, and returns the binary stream. It
// takes over conn, closing it on failure; closing the stream closes conn. ctx
// bounds only the handshake.
func ClientWebSocket(ctx context.Context, conn net.Conn, url string) (net.Conn, error) {
	// The library dials through an HTTP client, so hand it conn as its
	// connection; net/http then adds TLS for wss. conn leads to one host, so a
	// redirect fails the handshake rather than being followed.
	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(context.Context, string, string) (net.Conn, error) { return conn, nil },
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	ws, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{HTTPClient: client})
	if err != nil {
		conn.Close()
		return nil, err
	}
	return wrapWebSocket(context.Background(), ws), nil
}

// webSocketConn has no CloseWrite, since WebSocket cannot half-close. Close
// runs the library's close handshake, and an I/O deadline that expires during
// a read or write ends the whole connection, unlike TCP.
type webSocketConn struct {
	net.Conn
	close func() error
	stop  func() bool
}

func wrapWebSocket(ctx context.Context, ws *websocket.Conn) net.Conn {
	stream := websocket.NetConn(ctx, ws, websocket.MessageBinary)
	closeConn := sync.OnceValue(func() error {
		err := ws.Close(websocket.StatusNormalClosure, "")
		stream.Close()
		return err
	})
	stop := context.AfterFunc(ctx, func() { closeConn() })
	return &webSocketConn{Conn: stream, close: closeConn, stop: stop}
}

// Read reports a peer that drops the connection between frames, without the
// closing handshake, as io.EOF, the same as a peer that closes properly; many
// clients end their connections that way. A connection cut inside a frame
// still fails.
func (c *webSocketConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if errors.Is(err, io.EOF) {
		err = io.EOF
	}
	return n, err
}

func (c *webSocketConn) Close() error {
	c.stop()
	return c.close()
}
