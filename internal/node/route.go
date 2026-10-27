package node

import (
	"context"
	"fmt"
	"net"

	"github.com/Qusic/miniray/internal/netutil"
	"github.com/Qusic/miniray/internal/tunnel"
)

// routeFunc takes over a client stream that asks for host:port and returns
// once the stream is done; the caller then closes it.
type routeFunc func(ctx context.Context, conn net.Conn, host netutil.Address, port uint16) error

// routeDirect is the route of the public ingress: it dials the target straight
// from this node. dialDirect refuses .tunnel names, so relay users cannot reach
// the tunnel.
func routeDirect(ctx context.Context, conn net.Conn, host netutil.Address, port uint16) error {
	target, err := dialDirect(ctx, host, port)
	if err != nil {
		return err
	}
	return netutil.Join(conn, target)
}

// router holds what the node knows of the tunnel network: its own name and
// aliases, and the hub or the way to it. dialTunnel reaches an alias by its
// virtual name with them, and routeTunnel serves the hub's tunnel ingress.
type router struct {
	self    netutil.Label
	aliases aliases
	hub     *tunnel.Hub // only on the hub
	toHub   dialFunc    // on other nodes: the hub's tunnel ingress
}

// routeTunnel is the route of the hub's tunnel ingress. Spokes' control and
// data streams go to the hub. Other requests are dialed: aliases by their
// virtual names, and ordinary hosts directly.
func (r router) routeTunnel(ctx context.Context, conn net.Conn, host netutil.Address, port uint16) error {
	dial := dialFunc(dialDirect)
	if tunnel.IsName(host.Domain()) {
		t, err := tunnel.ParseTarget(host.Domain(), port)
		if err != nil {
			return err
		}
		if t.Kind != tunnel.Route {
			return r.hub.Serve(ctx, conn, t)
		}
		dial = r.dialTunnel
	}
	target, err := dial(ctx, host, port)
	if err != nil {
		return err
	}
	return netutil.Join(conn, target)
}

// dialTunnel connects to an alias by its virtual name, alias.node.tunnel: to
// the node's own aliases directly, and to other nodes' through the hub, on the
// hub itself or by way of it.
func (r router) dialTunnel(ctx context.Context, host netutil.Address, port uint16) (net.Conn, error) {
	t, err := tunnel.ParseTarget(host.Domain(), port)
	if err != nil {
		return nil, err
	}
	switch {
	case t.Kind != tunnel.Route:
		return nil, fmt.Errorf("%s names no alias", host)
	case t.Node == r.self:
		return r.aliases.dial(ctx, t.Alias, t.Port)
	case r.hub != nil:
		return r.hub.Dial(ctx, t.Node, t.Alias, t.Port)
	}
	return r.toHub(ctx, host, port)
}

// aliases maps the node's aliases to their hosts.
type aliases map[netutil.Label]netutil.Address

// dial dials alias:port on the alias's host directly. It routes the hub's own
// aliases and the connections a spoke opens for the hub.
func (a aliases) dial(ctx context.Context, alias netutil.Label, port uint16) (net.Conn, error) {
	host, ok := a[alias]
	if !ok {
		return nil, fmt.Errorf("no egress alias %s", alias)
	}
	return dialDirect(ctx, host, port)
}
