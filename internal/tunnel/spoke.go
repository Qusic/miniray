package tunnel

import (
	"context"
	"log/slog"
	"net"
	"time"

	"github.com/Qusic/miniray/internal/netutil"
)

// dialTimeout bounds how long a spoke takes to open a connection for the hub:
// to dial the alias and open the data stream. It must stay below the hub's
// openTimeout, so that the hub learns why a connection failed before it gives
// up.
const dialTimeout = 5 * time.Second

// The delay before reconnecting to the hub starts at minBackoff, doubles after
// each further failure up to maxBackoff, and starts over once the hub has
// accepted a registration.
const (
	minBackoff = time.Second
	maxBackoff = 30 * time.Second
)

// Spoke keeps a node registered with the hub and opens the connections the
// hub asks for to the node's aliases.
type Spoke struct {
	Name netutil.Label
	// DialHub opens a stream to the hub that carries a request for host:port.
	DialHub func(ctx context.Context, host netutil.Address, port uint16) (net.Conn, error)
	// DialAlias connects to an alias of the node.
	DialAlias func(ctx context.Context, alias netutil.Label, port uint16) (net.Conn, error)
	Log       *slog.Logger
}

// Run keeps the node registered until ctx ends, reconnecting after each
// failure, and then returns the cause. Connections already opened outlive the
// control stream that asked for them.
func (s Spoke) Run(ctx context.Context) error {
	backoff := minBackoff
	for {
		registered, err := s.session(ctx)
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		if registered {
			backoff = minBackoff
			s.Log.Warn("hub connection lost", "err", err, "retry", backoff)
		} else {
			s.Log.Warn("registration failed", "err", err, "retry", backoff)
		}
		select {
		case <-time.After(backoff):
		case <-ctx.Done():
			return context.Cause(ctx)
		}
		backoff = min(2*backoff, maxBackoff)
	}
}

// session registers once and serves the hub until the control stream ends.
// registered reports whether the hub accepted the registration, which it
// acknowledges with a ready message.
func (s Spoke) session(ctx context.Context) (registered bool, err error) {
	conn, err := s.dialHub(ctx, Target{Kind: Control, Node: s.Name})
	if err != nil {
		return false, err
	}
	c := newControl(conn)
	err = c.run(ctx, func(m message) {
		switch m.Type {
		case typeReady:
			registered = true
			s.Log.Info("registered with the hub")
		case typeOpen:
			go s.open(ctx, c, m)
		}
	})
	return registered, err
}

// open connects to the alias the hub asked for and joins it with a new data
// stream to the hub, or tells the hub why it could not.
func (s Spoke) open(ctx context.Context, c *control, m message) {
	log := s.Log.With("alias", m.Alias, "port", m.Port)
	target, stream, err := s.connect(ctx, m)
	if err != nil {
		c.send(failMessage(m.ID, err))
		log.Warn("request ended", "err", err)
		return
	}
	if err := netutil.Join(stream, target); err != nil {
		log.Warn("request ended", "err", err)
	}
}

// connect dials the alias the hub asked for, then opens the data stream that
// carries it to the hub, both within dialTimeout.
func (s Spoke) connect(ctx context.Context, m message) (target, stream net.Conn, err error) {
	ctx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()
	target, err = s.DialAlias(ctx, netutil.Label(m.Alias), m.Port)
	if err != nil {
		return nil, nil, err
	}
	stream, err = s.dialHub(ctx, Target{Kind: Data, Node: s.Name, Port: m.ID})
	if err != nil {
		target.Close()
		return nil, nil, err
	}
	return target, stream, nil
}

// dialHub opens a stream to the hub for a control or data target.
func (s Spoke) dialHub(ctx context.Context, t Target) (net.Conn, error) {
	host, err := netutil.DomainAddress(t.Domain())
	if err != nil {
		return nil, err
	}
	return s.DialHub(ctx, host, t.Port)
}
