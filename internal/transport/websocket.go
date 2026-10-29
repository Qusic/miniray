// Package transport adapts network transports to net.Conn.
package transport

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
)

// Streams between miniray nodes keep up a heartbeat, which both ends agree on
// as a WebSocket subprotocol: silence means a peer is gone only if that peer
// promised to send heartbeats. Other clients, such as phones, make no such
// promise and get none. Each end sends an empty message every
// heartbeatInterval, and takes the peer to be gone once a read has waited
// silenceTimeout without a byte arriving. Time counts only while a read waits,
// so that a stream left unread, to hold back a sender faster than its reader,
// does not pass for a silent one. Readers never see a heartbeat, as NetConn
// skips empty messages.
//
// A WebSocket ping would not do: the library answers it on the read path, and
// an answer stuck behind a long write ends the stream within seconds.
const (
	subprotocol       = "miniray"
	heartbeatInterval = 10 * time.Second
	silenceTimeout    = 30 * time.Second
)

var errSilent = errors.New("transport: peer went silent")

// AcceptWebSocket upgrades r to a binary WebSocket stream, with a heartbeat if
// the client asks for one. On failure it has already written an HTTP response.
// The connection ends when r.Context is canceled, including when the handler
// returns: keep the handler alive for as long as the stream is used.
func AcceptWebSocket(w http.ResponseWriter, r *http.Request) (net.Conn, error) {
	hb := new(heartbeat)
	ws, err := websocket.Accept(hijacker{w, hb}, r, &websocket.AcceptOptions{
		Subprotocols: []string{subprotocol},
		// Proxy clients authenticate inside the stream, not via browser cookies.
		InsecureSkipVerify: true,
	})
	if err != nil {
		return nil, err
	}
	return newStream(r.Context(), ws, hb), nil
}

// hijacker hands the library the connection it hijacks as a rawConn.
type hijacker struct {
	http.ResponseWriter
	heartbeat *heartbeat
}

func (h hijacker) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	conn, rw, err := http.NewResponseController(h.ResponseWriter).Hijack()
	if err != nil {
		return nil, nil, err
	}
	return rawConn{conn, h.heartbeat}, rw, nil
}

// ClientWebSocket runs the client side of the handshake for a ws:// or wss://
// url over conn, with TLS first for wss, and returns the binary stream, with a
// heartbeat if the server agrees to one. It takes over conn, closing it on
// failure; closing the stream closes conn. ctx bounds only the handshake.
func ClientWebSocket(ctx context.Context, conn net.Conn, url string) (net.Conn, error) {
	hb := new(heartbeat)
	// The library dials through an HTTP client, so hand it conn, as a rawConn,
	// for its connection; net/http then adds TLS for wss. conn leads to one
	// host, so a redirect fails the handshake rather than being followed.
	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(context.Context, string, string) (net.Conn, error) {
				return rawConn{conn, hb}, nil
			},
		},
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
	ws, _, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		HTTPClient:   client,
		Subprotocols: []string{subprotocol},
	})
	if err != nil {
		conn.Close()
		return nil, err
	}
	return newStream(context.Background(), ws, hb), nil
}

// stream is what callers get. Its layers, top down:
//
//	stream          ours: errors and closing as callers expect
//	NetConn         the library's: binary messages as a byte stream
//	websocket.Conn  the library's: frames and messages
//	TLS             for wss only
//	rawConn         ours: the connection under it all, timing out reads
//
// stream has no CloseWrite, since WebSocket cannot half-close. Close runs the
// library's close handshake, and an I/O deadline that expires during a read
// or write ends the whole connection, unlike TCP. Once the heartbeat is lost,
// reads and writes fail with errSilent.
type stream struct {
	net.Conn
	heartbeat *heartbeat
	close     func() error
	stop      func() bool
}

func newStream(ctx context.Context, ws *websocket.Conn, hb *heartbeat) net.Conn {
	netConn := websocket.NetConn(ctx, ws, websocket.MessageBinary)
	done := make(chan struct{})
	closeStream := sync.OnceValue(func() error {
		close(done)
		err := ws.Close(websocket.StatusNormalClosure, "")
		netConn.Close()
		return err
	})
	stop := context.AfterFunc(ctx, func() { closeStream() })
	if ws.Subprotocol() == subprotocol {
		hb.agreed.Store(true)
		go sendHeartbeats(done, ws)
	}
	return &stream{Conn: netConn, heartbeat: hb, close: closeStream, stop: stop}
}

// Read reports a peer that drops the connection between frames, without the
// closing handshake, as io.EOF, the same as a peer that closes properly; many
// clients end their connections that way. A connection cut inside a frame
// still fails.
func (s *stream) Read(p []byte) (int, error) {
	n, err := s.Conn.Read(p)
	if errors.Is(err, io.EOF) {
		err = io.EOF
	}
	if err != nil && s.heartbeat.lost.Load() {
		err = errSilent
	}
	return n, err
}

func (s *stream) Write(p []byte) (int, error) {
	n, err := s.Conn.Write(p)
	if err != nil && s.heartbeat.lost.Load() {
		err = errSilent
	}
	return n, err
}

func (s *stream) Close() error {
	s.stop()
	return s.close()
}

// sendHeartbeats writes an empty message to ws every heartbeatInterval, until
// done or a write fails.
func sendHeartbeats(done <-chan struct{}, ws *websocket.Conn) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-done:
			return
		case <-ticker.C:
			if err := ws.Write(context.Background(), websocket.MessageBinary, nil); err != nil {
				return
			}
		}
	}
}

// rawConn is the connection under a stream. The library reads it only while a
// read of the stream waits, and any byte that arrives ends such a read, so one
// that lasts silenceTimeout means the peer has been silent that long.
type rawConn struct {
	net.Conn
	heartbeat *heartbeat
}

func (c rawConn) Read(p []byte) (int, error) {
	if !c.heartbeat.agreed.Load() {
		return c.Conn.Read(p)
	}
	c.SetReadDeadline(time.Now().Add(silenceTimeout))
	n, err := c.Conn.Read(p)
	if errors.Is(err, os.ErrDeadlineExceeded) {
		// Mark the loss before closing, so that every failure the close causes
		// reports it. The library leaves a stream open after a failed read.
		c.heartbeat.lost.Store(true)
		c.Close()
	}
	return n, err
}

// heartbeat is shared by a stream and the connection under it, since the
// library between them tells the connection nothing of the subprotocol, nor
// the stream why the connection failed.
type heartbeat struct {
	agreed atomic.Bool // both ends keep it up
	lost   atomic.Bool // the peer went silent, ending the stream
}
