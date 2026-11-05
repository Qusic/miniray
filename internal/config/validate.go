package config

import (
	"errors"
	"fmt"
	"maps"
	"slices"
)

// validate checks the topology as a whole: its trust domains, every node's
// config, and that each forward names an egress alias of an existing node.
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
		for node, aliases := range t.Node[name].Forward {
			target, ok := t.Node[node]
			if !ok {
				return fmt.Errorf("node %s: forward.%s: no such node", name, node)
			}
			for alias := range aliases {
				if _, ok := target.Egress[alias]; !ok {
					return fmt.Errorf("node %s: forward.%s.%s: node %s has no egress alias %s", name, node, alias, node, alias)
				}
			}
		}
	}
	return nil
}

// validate checks that the config names its node, holds exactly the
// credentials the node uses, and has the egress alias of each forward to
// itself.
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
		{"tunnel", !c.Credential.Tunnel.IsZero(), tunnel, "ingress.tunnel, egress, or forward"},
		{"relay", !c.Credential.Relay.IsZero(), relay, "ingress.public, ingress.proxy, or option.relay on a node that connects to the hub"},
	} {
		switch {
		case entry.used && !entry.held:
			return fmt.Errorf("credential.%s is required by %s", entry.name, entry.users)
		case entry.held && !entry.used:
			return fmt.Errorf("credential.%s is not used by this node", entry.name)
		}
	}
	for alias := range c.Forward[c.Name] {
		if _, ok := c.Egress[alias]; !ok {
			return fmt.Errorf("forward.%s.%s: node %s has no egress alias %s", c.Name, alias, c.Name, alias)
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

// validate gives each local listener its own port.
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
	for node, aliases := range n.Forward {
		for alias, mappings := range aliases {
			for _, mapping := range mappings {
				if err := claim(mapping.Listen, fmt.Sprintf("forward.%s.%s", node, alias)); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
