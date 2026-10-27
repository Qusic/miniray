package config

import (
	"fmt"
	"io"
	"maps"
	"os"
	"slices"

	"github.com/Qusic/miniray/internal/netutil"
)

// Topology describes both shared credentials and every node by name. Only
// prepare reads it; each node runs its own Config.
type Topology struct {
	Credential Credentials            `json:"credential"`
	Node       map[netutil.Label]Node `json:"node"`
}

// LoadTopology reads and validates the topology.
func LoadTopology(path string) (Topology, error) {
	file, err := os.Open(path)
	if err != nil {
		return Topology{}, fmt.Errorf("topology %s: %w", path, err)
	}
	defer file.Close()
	t, err := decodeTopology(file)
	if err != nil {
		return Topology{}, fmt.Errorf("topology %s: %w", path, err)
	}
	return t, nil
}

func decodeTopology(r io.Reader) (Topology, error) {
	var t Topology
	if err := unmarshal(r, &t); err != nil {
		return Topology{}, err
	}
	if err := t.validate(); err != nil {
		return Topology{}, err
	}
	return t, nil
}

// Prepare cuts every node's config out of the topology, sorted by name.
func (t Topology) Prepare() []Config {
	var configs []Config
	for _, name := range slices.Sorted(maps.Keys(t.Node)) {
		configs = append(configs, t.config(name))
	}
	return configs
}

// config cuts out one node's config. It holds only the credentials that node
// uses, so a relay-only node never gets the tunnel password.
func (t Topology) config(name netutil.Label) Config {
	node := t.Node[name]
	cfg := Config{Name: name, Node: node}
	tunnel, relay := node.usedCredentials()
	if tunnel {
		cfg.Credential.Tunnel = t.Credential.Tunnel
	}
	if relay {
		cfg.Credential.Relay = t.Credential.Relay
	}
	return cfg
}
