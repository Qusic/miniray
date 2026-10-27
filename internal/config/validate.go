package config

import (
	"errors"
	"fmt"
	"maps"
	"slices"
)

// validate checks the topology as a whole: its trust domains, every node's
// config, and that each link names an egress alias of an existing node.
func (t Topology) validate() error {
	if len(t.Node) == 0 {
		return errors.New("no nodes")
	}
	if err := t.Credential.validate(); err != nil {
		return err
	}
	for _, name := range slices.Sorted(maps.Keys(t.Node)) {
		if err := t.config(name).validate(); err != nil {
			return fmt.Errorf("node %s: %w", name, err)
		}
		for peer, aliases := range t.Node[name].Link {
			target, ok := t.Node[peer]
			if !ok {
				return fmt.Errorf("node %s: link.%s: no such node", name, peer)
			}
			for alias := range aliases {
				if _, ok := target.Egress[alias]; !ok {
					return fmt.Errorf("node %s: link.%s.%s: node %s has no egress alias %s", name, peer, alias, peer, alias)
				}
			}
		}
	}
	return nil
}

// validate checks that the config names its node and holds exactly the
// credentials the node uses.
func (c Config) validate() error {
	if c.Name == "" {
		return errors.New("name is required")
	}
	if err := c.Credential.validate(); err != nil {
		return err
	}
	tunnel, relay := c.Node.usedCredentials()
	for _, entry := range []struct {
		name       string
		held, used bool
		users      string
	}{
		{"tunnel", !c.Credential.Tunnel.IsZero(), tunnel, "ingress.tunnel, egress, or link"},
		{"relay", !c.Credential.Relay.IsZero(), relay, "ingress.public, ingress.proxy, or option.relay on a node that connects to the hub"},
	} {
		switch {
		case entry.used && !entry.held:
			return fmt.Errorf("credential.%s is required by %s", entry.name, entry.users)
		case entry.held && !entry.used:
			return fmt.Errorf("credential.%s is not used by this node", entry.name)
		}
	}
	return c.Node.validate()
}

// validate keeps the trust domains apart, so relay users learn nothing that
// opens the tunnel: with one password they could use it, and with one path
// they could find it.
func (c Credentials) validate() error {
	if c.Tunnel.IsZero() || c.Relay.IsZero() {
		return nil
	}
	switch {
	case c.Tunnel.password() == c.Relay.password():
		return errors.New("tunnel and relay passwords must differ")
	case c.Tunnel.url.Path == c.Relay.url.Path:
		return errors.New("tunnel and relay paths must differ")
	}
	return nil
}

// validate gives each local listener its own port and points each forward at
// a local egress alias.
func (n Node) validate() error {
	ports := make(map[uint16]string)
	claim := func(port uint16, owner string) error {
		if previous, ok := ports[port]; ok {
			return fmt.Errorf("%s: local port %d is already used by %s", owner, port, previous)
		}
		ports[port] = owner
		return nil
	}
	for _, entry := range []struct {
		name string
		port uint16
	}{
		{"ingress.tunnel", n.Ingress.Tunnel},
		{"ingress.private", n.Ingress.Private},
		{"ingress.public", n.Ingress.Public},
		{"ingress.proxy", n.Ingress.Proxy},
	} {
		if entry.port != 0 {
			if err := claim(entry.port, entry.name); err != nil {
				return err
			}
		}
	}
	for alias, mappings := range n.Forward {
		if _, ok := n.Egress[alias]; !ok {
			return fmt.Errorf("forward.%s: no local egress alias", alias)
		}
		for _, mapping := range mappings {
			if err := claim(mapping.Listen, fmt.Sprintf("forward.%s", alias)); err != nil {
				return err
			}
		}
	}
	for node, aliases := range n.Link {
		for alias, mappings := range aliases {
			for _, mapping := range mappings {
				if err := claim(mapping.Listen, fmt.Sprintf("link.%s.%s", node, alias)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
