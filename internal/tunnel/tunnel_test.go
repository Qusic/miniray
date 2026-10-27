package tunnel

import (
	"strings"
	"testing"
)

func TestIsName(t *testing.T) {
	for name, want := range map[string]bool{
		"node.tunnel":        true,
		"alias.node.tunnel":  true,
		"ALIAS.NODE.TUNNEL.": true,
		// Malformed virtual names count too, so they are refused, not resolved.
		".tunnel":          true,
		"a.b.c.tunnel":     true,
		"bad_label.tunnel": true,
		// Only a final .tunnel label counts, not a name that merely contains it.
		"tunnel":             false,
		"notunnel":           false,
		"node.notunnel":      false,
		"tunnel.example.com": false,
		"":                   false,
	} {
		if got := IsName(name); got != want {
			t.Errorf("IsName(%q) = %t, want %t", name, got, want)
		}
	}
}

func TestParseTarget(t *testing.T) {
	for _, tt := range []struct {
		domain string
		port   uint16
		want   Target
	}{
		{"node.tunnel", 0, Target{Kind: Control, Node: "node"}},
		{"node.tunnel", 1, Target{Kind: Data, Node: "node", Port: 1}},
		{"node.tunnel", 65535, Target{Kind: Data, Node: "node", Port: 65535}},
		{"nas.node.tunnel", 22, Target{Kind: Route, Node: "node", Alias: "nas", Port: 22}},
		// Names are case-insensitive and may end with the root dot.
		{"Nas.NODE.Tunnel.", 22, Target{Kind: Route, Node: "node", Alias: "nas", Port: 22}},
		{"NODE.TUNNEL.", 0, Target{Kind: Control, Node: "node"}},
	} {
		got, err := ParseTarget(tt.domain, tt.port)
		if err != nil || got != tt.want {
			t.Errorf("ParseTarget(%q, %d) = %+v, %v; want %+v", tt.domain, tt.port, got, err, tt.want)
			continue
		}
		// Requests built from a target parse back to it.
		if back, err := ParseTarget(got.Domain(), got.Port); err != nil || back != got {
			t.Errorf("%+v: Domain %q parses to %+v, %v", got, got.Domain(), back, err)
		}
	}

	for _, tt := range []struct {
		domain string
		port   uint16
		want   string
	}{
		{"ordinary.example.com", 22, "neither"},
		{".tunnel", 0, "lowercase DNS label"},
		{"tunnel", 0, "neither"},
		{"a.b.c.tunnel", 22, "neither"},
		{"nas..tunnel", 22, "lowercase DNS label"},
		{"bad_label.tunnel", 0, "lowercase DNS label"},
		// A route needs the port to reach.
		{"nas.node.tunnel", 0, "has no port"},
	} {
		if got, err := ParseTarget(tt.domain, tt.port); err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("ParseTarget(%q, %d) = %+v, %v; want an error containing %q", tt.domain, tt.port, got, err, tt.want)
		}
	}
}
