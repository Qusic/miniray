package tunnel

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/Qusic/miniray/internal/netutil"
)

// openTimeout bounds how long Dial waits for a spoke to open the connection it
// asked for. It must exceed the spoke's dialTimeout, so that a spoke that
// cannot open the connection reports why before the hub gives up.
const openTimeout = 10 * time.Second

var errReplaced = errors.New("tunnel: replaced by a newer registration")

// Hub keeps the control streams of registered spokes and opens connections
// through them. A spoke registers under its own name; a new registration
// under a taken name replaces the old one, since that is how a restarted node
// comes back. Holders of the tunnel password are trusted alike: any of them
// may register under any name but the hub's own, and deliver any connection
// the hub is waiting for.
type Hub struct {
	self netutil.Label
	log  *slog.Logger

	mu      sync.Mutex
	spokes  map[netutil.Label]*session // the latest registration of each spoke
	pending map[uint16]*pending        // what each waiting Dial waits for, by id
	lastID  uint16
}

// session is the control stream of a registered spoke.
type session struct {
	control *control
	cancel  context.CancelCauseFunc // ends the session early
	done    <-chan struct{}         // closed once the session ends
}

// pending is the connection that one Dial asked a spoke to open. It is in
// Hub.pending exactly while that Dial waits; the first result for it settles
// it, and anything later finds Dial gone.
type pending struct {
	spoke  netutil.Label   // the only spoke that may settle it
	result chan dialResult // hands Dial the result
	gone   chan struct{}   // closed once Dial stops waiting
}

// dialResult is what a spoke opened for Dial: a data stream, or why it could
// not open one.
type dialResult struct {
	conn net.Conn
	err  error
}

// NewHub returns the hub of the node named self, a name no spoke can take.
func NewHub(self netutil.Label, log *slog.Logger) *Hub {
	return &Hub{
		self:    self,
		log:     log,
		spokes:  make(map[netutil.Label]*session),
		pending: make(map[uint16]*pending),
	}
}

// Serve takes over a control or data stream that a spoke opened to the hub and
// returns once the hub is done with it; the caller then closes the stream. A
// control stream lasts as long as the registration. A data stream lasts until
// the caller of the matching Dial closes it, or ctx ends.
func (h *Hub) Serve(ctx context.Context, conn net.Conn, t Target) error {
	switch t.Kind {
	case Control:
		return h.register(ctx, conn, t.Node)
	case Data:
		return h.deliver(ctx, conn, t.Node, t.Port)
	}
	return fmt.Errorf("tunnel: %s is a route, which is dialed, not served", t.Domain())
}

// register keeps spoke registered for as long as its control stream lasts.
// It acknowledges the registration with a ready message; a rejected spoke
// gets none before its stream closes.
func (h *Hub) register(ctx context.Context, conn net.Conn, spoke netutil.Label) error {
	if spoke == h.self {
		return fmt.Errorf("tunnel: %s is the hub's own name", spoke)
	}
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)
	s := &session{control: newControl(conn), cancel: cancel, done: ctx.Done()}

	h.mu.Lock()
	old := h.spokes[spoke]
	h.spokes[spoke] = s
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		if h.spokes[spoke] == s {
			delete(h.spokes, spoke)
		}
		h.mu.Unlock()
	}()
	if old != nil {
		old.cancel(errReplaced)
	}
	h.log.Info("spoke registered", "spoke", spoke, "replaced", old != nil)

	if err := s.control.send(message{Type: typeReady}); err != nil {
		return fmt.Errorf("spoke %s: %w", spoke, err)
	}
	err := s.control.run(ctx, func(m message) {
		if m.Type == typeFail {
			h.settle(spoke, m.ID, dialResult{err: fmt.Errorf("tunnel: spoke %s: %s", spoke, m.Error)})
		}
	})
	return fmt.Errorf("spoke %s: %w", spoke, err)
}

// Dial asks spoke to connect to its alias at port, and returns the data stream
// the spoke opens back for it. The caller must close the stream, which also
// lets the Serve call that delivered it return.
func (h *Hub) Dial(ctx context.Context, spoke, alias netutil.Label, port uint16) (net.Conn, error) {
	h.mu.Lock()
	s := h.spokes[spoke]
	if s == nil {
		h.mu.Unlock()
		return nil, fmt.Errorf("tunnel: spoke %s is not registered", spoke)
	}
	id, err := h.nextID()
	if err != nil {
		h.mu.Unlock()
		return nil, err
	}
	p := &pending{spoke: spoke, result: make(chan dialResult), gone: make(chan struct{})}
	h.pending[id] = p
	h.mu.Unlock()
	defer func() {
		h.mu.Lock()
		delete(h.pending, id)
		h.mu.Unlock()
		close(p.gone)
	}()

	if err := s.control.send(message{Type: typeOpen, ID: id, Alias: string(alias), Port: port}); err != nil {
		return nil, fmt.Errorf("spoke %s: %w", spoke, err)
	}
	ctx, cancel := context.WithTimeoutCause(ctx, openTimeout,
		fmt.Errorf("tunnel: spoke %s did not open %s:%d within %v", spoke, alias, port, openTimeout))
	defer cancel()
	select {
	case r := <-p.result:
		return r.conn, r.err
	case <-s.done:
		return nil, fmt.Errorf("tunnel: spoke %s went away", spoke)
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}

// nextID returns the next connection id not in use, skipping 0, which
// registers. The caller holds h.mu.
func (h *Hub) nextID() (uint16, error) {
	for range 1 << 16 {
		h.lastID++
		if _, used := h.pending[h.lastID]; h.lastID != 0 && !used {
			return h.lastID, nil
		}
	}
	return 0, errors.New("tunnel: too many pending connections")
}

// settle hands r to the Dial waiting for connection id, if spoke is the one
// it asked.
func (h *Hub) settle(spoke netutil.Label, id uint16, r dialResult) error {
	h.mu.Lock()
	p := h.pending[id]
	h.mu.Unlock()
	if p == nil || p.spoke != spoke {
		return fmt.Errorf("tunnel: spoke %s has no pending connection %d", spoke, id)
	}
	select {
	case p.result <- r:
		return nil
	case <-p.gone:
		return fmt.Errorf("tunnel: connection %d from spoke %s came too late", id, spoke)
	}
}

// deliver hands the data stream for connection id to the Dial waiting for it,
// then holds the stream until it is closed there or ctx ends.
func (h *Hub) deliver(ctx context.Context, conn net.Conn, spoke netutil.Label, id uint16) error {
	stream := &dataStream{Conn: conn, closed: make(chan struct{})}
	if err := h.settle(spoke, id, dialResult{conn: stream}); err != nil {
		return err
	}
	select {
	case <-stream.closed:
	case <-ctx.Done():
	}
	return nil
}

// dataStream tells deliver when the caller of Dial closes the stream.
type dataStream struct {
	net.Conn
	once   sync.Once
	closed chan struct{}
}

func (s *dataStream) Close() error {
	err := s.Conn.Close()
	s.once.Do(func() { close(s.closed) })
	return err
}
