package config

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/Qusic/miniray/internal/netutil"
)

const topology = `{
  "credential": {
    "tunnel": "wss://:tunnel-secret@hub.example.com/tunnel",
    "relay": "wss://:relay-secret@relay.example.com/relay"
  },
  "node": {
    "hub": {
      "ingress": { "tunnel": 8080, "proxy": 1080 },
      "egress": { "nas": "192.0.2.10" },
      "forward": { "nas": ["2202:22"] },
      "link": { "peer": { "git": ["2208:2209"] } }
    },
    "peer": {
      "egress": { "git": "192.0.2.11" },
      "option": { "relay": true }
    },
    "relay": { "ingress": { "public": 80 } },
    "laptop": {
      "link": { "hub": { "nas": ["1028:22"] }, "peer": { "git": ["2213:2214"] } },
      "option": { "local": true }
    }
  }
}`

func parseTopology(t *testing.T, data string) Topology {
	t.Helper()
	topo, err := decodeTopology(strings.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	return topo
}

func TestDecodeTopology(t *testing.T) {
	// The edited cases start from the valid topology above.
	for _, tt := range []struct {
		name, data string
		is         error
		at         string
		want       string
	}{
		{name: "empty", data: "", is: io.ErrUnexpectedEOF},
		{name: "no nodes", data: `{"credential": {}, "node": {}}`, want: "no nodes"},
		{name: "trailing value", data: topology + " {}", at: "syntax"},
		{name: "unknown member", data: strings.Replace(topology, `"node": {`, `"hub": { "peers": ["peer"] }, "node": {`, 1), is: json.ErrUnknownName},
		{name: "duplicate node", data: strings.Replace(topology, `    "peer": {`, `    "relay": {}, "peer": {`, 1), is: jsontext.ErrDuplicateName},
		{name: "invalid node", data: strings.Replace(topology, `"proxy": 1080`, `"proxy": 8080`, 1), want: "node hub: ingress.proxy"},
		// No node holds both credentials, yet relay users must not get the tunnel.
		{name: "identical passwords", data: `{
  "credential": {
    "tunnel": "wss://:shared-secret@hub.example.com/tunnel",
    "relay": "wss://:shared-secret@relay.example.com/relay"
  },
  "node": { "hub": { "ingress": { "tunnel": 80 } }, "relay": { "ingress": { "public": 80 } } }
}`, want: "passwords must differ"},
		{name: "missing credential", data: strings.Replace(topology, ",\n    \"relay\": \"wss://:relay-secret@relay.example.com/relay\"", "", 1), want: "node hub: credential.relay is required"},
		{name: "unknown linked alias", data: strings.Replace(topology, `"peer": { "git": ["2213:2214"] }`, `"peer": { "nope": ["2213:2214"] }`, 1), want: "node laptop: link.peer.nope: node peer has no egress alias nope"},
		// Even without aliases, a link must name an existing node.
		{name: "unknown linked node", data: strings.Replace(topology, `"peer": { "git": ["2213:2214"] }`, `"ghost": {}`, 1), want: "node laptop: link.ghost: no such node"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decodeTopology(strings.NewReader(tt.data))
			switch {
			case err == nil:
				t.Fatal("invalid topology accepted")
			case tt.is != nil && !errors.Is(err, tt.is):
				t.Fatalf("decodeTopology = %v, want %v", err, tt.is)
			case tt.at != "" && jsonError(err) != tt.at:
				t.Fatalf("decodeTopology = %v, want the JSON library to reject %s", err, tt.at)
			case !strings.Contains(err.Error(), tt.want):
				t.Fatalf("decodeTopology = %v, want message containing %q", err, tt.want)
			}
		})
	}
}

func TestLoadTopology(t *testing.T) {
	path := filepath.Join(t.TempDir(), "topology.json")
	if err := os.WriteFile(path, []byte(topology), 0600); err != nil {
		t.Fatal(err)
	}
	if topo, err := LoadTopology(path); err != nil || len(topo.Node) != 4 {
		t.Fatalf("LoadTopology = %d nodes, %v", len(topo.Node), err)
	}
	if _, err := LoadTopology(filepath.Join(t.TempDir(), "missing.json")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("missing topology = %v, want file not found", err)
	}
}

// A node holds the credentials of the endpoints it has or uses: the hub serves
// tunnel, other nodes with egress or link connect to it, optionally through
// relay, and public and proxy ingress use relay.
func TestUsedCredentials(t *testing.T) {
	egress := map[netutil.Label]netutil.Address{"git": mustAddress(t, "192.0.2.11")}
	link := map[netutil.Label]map[netutil.Label][]PortMapping{"hub": {"nas": {{Listen: 1028, Target: 22}}}}
	for _, tt := range []struct {
		name          string
		node          Node
		tunnel, relay bool
	}{
		{"hub", Node{Ingress: Ingress{Tunnel: 80}, Egress: egress, Link: link}, true, false},
		{"hub ignores option.relay", Node{Ingress: Ingress{Tunnel: 80}, Egress: egress, Option: Options{Relay: true}}, true, false},
		{"hub with relay proxy", Node{Ingress: Ingress{Tunnel: 80, Proxy: 1080}}, true, true},
		{"egress", Node{Egress: egress}, true, false},
		{"egress through relay", Node{Egress: egress, Option: Options{Relay: true}}, true, true},
		{"link through relay", Node{Link: link, Option: Options{Relay: true}}, true, true},
		{"public", Node{Ingress: Ingress{Public: 80}}, false, true},
		{"private", Node{Ingress: Ingress{Private: 2000}}, false, false},
		{"option.relay without hub connection", Node{Ingress: Ingress{Private: 2000}, Option: Options{Relay: true}}, false, false},
	} {
		if tunnel, relay := tt.node.usedCredentials(); tunnel != tt.tunnel || relay != tt.relay {
			t.Errorf("%s: tunnel=%t relay=%t, want %t %t", tt.name, tunnel, relay, tt.tunnel, tt.relay)
		}
	}
}

func TestPrepare(t *testing.T) {
	topo := parseTopology(t, topology)
	configs := topo.Prepare()
	// Configs come sorted by name, each with exactly the credentials it uses.
	want := []struct {
		name          string
		tunnel, relay bool
	}{
		{"hub", true, true},
		{"laptop", true, false},
		{"peer", true, true},
		{"relay", false, true},
	}
	if len(configs) != len(want) {
		t.Fatalf("prepared %d configs, want %d", len(configs), len(want))
	}
	for i, tt := range want {
		cfg := configs[i]
		t.Run(tt.name, func(t *testing.T) {
			if string(cfg.Name) != tt.name || !reflect.DeepEqual(cfg.Node, topo.Node[cfg.Name]) {
				t.Errorf("prepared %+v, want node %s unchanged", cfg, tt.name)
			}
			if tunnel, relay := !cfg.Credential.Tunnel.IsZero(), !cfg.Credential.Relay.IsZero(); tunnel != tt.tunnel || relay != tt.relay {
				t.Errorf("credentials tunnel=%t relay=%t, want %t %t", tunnel, relay, tt.tunnel, tt.relay)
			}

			// The saved file loads back unchanged and holds no other secret.
			path := filepath.Join(t.TempDir(), "node.json")
			if err := cfg.Save(path); err != nil {
				t.Fatal(err)
			}
			if back, err := Load(path); err != nil || !reflect.DeepEqual(back, cfg) {
				t.Errorf("reloaded %+v, %v; want %+v", back, err, cfg)
			}
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			for secret, want := range map[string]bool{"tunnel-secret": tt.tunnel, "relay-secret": tt.relay} {
				if got := strings.Contains(string(data), secret); got != want {
					t.Errorf("output contains %s = %t, want %t", secret, got, want)
				}
			}
		})
	}
}
