// Package tunnel connects the hub with its spokes, the nodes that register with
// it to serve their aliases. A spoke keeps a control stream open to the hub; to
// reach one of the spoke's aliases, the hub asks over that stream, and the
// spoke dials the alias and opens a data stream back. Streams and dial
// functions come from the caller, so this package knows nothing of WebSocket,
// Trojan, or the config.
package tunnel

import (
	"fmt"
	"strings"

	"github.com/Qusic/miniray/internal/netutil"
)

const suffix = ".tunnel"

// IsName reports whether domain is a virtual name under .tunnel, in any case
// and with or without the root dot.
func IsName(domain string) bool {
	return strings.HasSuffix(normalize(domain), suffix)
}

func normalize(domain string) string {
	return strings.TrimSuffix(strings.ToLower(domain), ".")
}

// Kind is what a request for a virtual name asks for.
type Kind uint8

const (
	// Route reaches an alias of a node: alias.node.tunnel:port.
	Route Kind = iota + 1
	// Control registers a node with the hub: node.tunnel:0.
	Control
	// Data delivers a connection the hub asked a node to open: node.tunnel:id,
	// where the id is 1 to 65535.
	Data
)

// Target is a parsed request for a virtual name.
type Target struct {
	Kind  Kind
	Node  netutil.Label
	Alias netutil.Label // only for Route
	// Port is the alias's port for Route and the connection id for Data.
	Port uint16
}

// ParseTarget parses a request for domain:port, where domain is under .tunnel.
// A malformed name is an error, not an ordinary host: virtual names must never
// reach DNS.
func ParseTarget(domain string, port uint16) (Target, error) {
	name, ok := strings.CutSuffix(normalize(domain), suffix)
	parts := strings.Split(name, ".")
	if !ok || len(parts) > 2 {
		return Target{}, fmt.Errorf("tunnel: %q is neither node.tunnel nor alias.node.tunnel", domain)
	}
	labels := make([]netutil.Label, len(parts))
	for i, part := range parts {
		label, err := netutil.ParseLabel(part)
		if err != nil {
			return Target{}, fmt.Errorf("tunnel: %q: %w", domain, err)
		}
		labels[i] = label
	}
	switch {
	case len(labels) == 1 && port == 0:
		return Target{Kind: Control, Node: labels[0]}, nil
	case len(labels) == 1:
		return Target{Kind: Data, Node: labels[0], Port: port}, nil
	case port == 0:
		return Target{}, fmt.Errorf("tunnel: %s has no port", domain)
	}
	return Target{Kind: Route, Alias: labels[0], Node: labels[1], Port: port}, nil
}

// Domain returns the virtual name that requests for t use: alias.node.tunnel
// for a route, node.tunnel otherwise.
func (t Target) Domain() string {
	if t.Kind == Route {
		return string(t.Alias) + "." + string(t.Node) + suffix
	}
	return string(t.Node) + suffix
}
