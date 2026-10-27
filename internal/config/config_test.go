package config

import (
	"bytes"
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/Qusic/miniray/internal/netutil"
)

func mustAddress(t testing.TB, text string) netutil.Address {
	t.Helper()
	address, err := netutil.ParseAddress(text)
	if err != nil {
		t.Fatal(err)
	}
	return address
}

// jsonError reports where the JSON library rejected the input, without relying
// on its wording: the JSON pointer of a semantic error, or "syntax".
func jsonError(err error) string {
	var semantic *json.SemanticError
	var syntactic *jsontext.SyntacticError
	switch {
	case errors.As(err, &semantic):
		return string(semantic.JSONPointer)
	case errors.As(err, &syntactic):
		return "syntax"
	}
	return ""
}

const fullConfig = `{
  "name": "hub",
  "credential": {
    "tunnel": "wss://:tunnel-secret@hub.example.com/tunnel",
    "relay": "wss://:relay%2Fsecret@relay.example.com/relay"
  },
  "ingress": {"tunnel": 80, "proxy": 1080},
  "egress": {"app": "localhost", "git": "192.0.2.10"},
  "forward": {"git": ["2202:22"]},
  "link": {"peer": {"remote": ["2208:2209", "1028:65535"]}},
  "option": {"local": true, "relay": true}
}`

func TestDecodeConfig(t *testing.T) {
	got, err := decode(strings.NewReader(fullConfig))
	if err != nil {
		t.Fatal(err)
	}
	want := Config{
		Name: "hub",
		Credential: Credentials{
			Tunnel: Credential{url: url.URL{Scheme: "wss", User: url.UserPassword("", "tunnel-secret"), Host: "hub.example.com", Path: "/tunnel"}},
			// Standard escapes let a password hold any character.
			Relay: Credential{url: url.URL{Scheme: "wss", User: url.UserPassword("", "relay/secret"), Host: "relay.example.com", Path: "/relay"}},
		},
		Node: Node{
			Ingress: Ingress{Tunnel: 80, Proxy: 1080},
			Egress:  map[netutil.Label]netutil.Address{"app": mustAddress(t, "localhost"), "git": mustAddress(t, "192.0.2.10")},
			Forward: map[netutil.Label][]PortMapping{"git": {{Listen: 2202, Target: 22}}},
			Link:    map[netutil.Label]map[netutil.Label][]PortMapping{"peer": {"remote": {{Listen: 2208, Target: 2209}, {Listen: 1028, Target: 65535}}}},
			Option:  Options{Local: true, Relay: true},
		},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("decoded config = %+v, want %+v", got, want)
	}
}

func TestDecodeAcceptsValidConfig(t *testing.T) {
	for _, tt := range []struct {
		name, data string
	}{
		{"IPv6 credential host with port", `{
  "name": "peer",
  "credential": {"tunnel": "wss://:tunnel-secret@[2001:db8::1]:8443/tunnel"},
  "egress": {"git": "192.0.2.11"}
}`},
		{"ws credential with port", `{
  "name": "peer",
  "credential": {"tunnel": "ws://:tunnel-secret@127.0.0.1:65535/tunnel"},
  "egress": {"git": "192.0.2.11"}
}`},
		// Port 0 disables an ingress, so the disabled proxy needs no relay
		// credential.
		{"zero port disables ingress", `{
  "name": "hub",
  "credential": {"tunnel": "wss://:tunnel-secret@hub.example.com/tunnel"},
  "ingress": {"tunnel": 80, "proxy": 0}
}`},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := decode(strings.NewReader(tt.data)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestDecodeRejectsInvalidConfig(t *testing.T) {
	// base is an open object so cases can append members before closing it.
	const tunnel = `"wss://:tunnel-secret@hub.example.com/tunnel"`
	const relay = `"wss://:relay-secret@relay.example.com/relay"`
	const base = `{"name": "hub", "credential": {"tunnel": ` + tunnel + `}, "ingress": {"tunnel": 8080}`
	const node = base + "}"
	const withRelay = tunnel + `, "relay": ` + relay + `}`
	public := strings.Replace(node, `"tunnel": 8080}`, `"tunnel": 8080, "public": 8081}`, 1)
	publicRelay := strings.Replace(public, tunnel+"}", withRelay, 1)
	// credential replaces the tunnel credential of node.
	credential := func(text string) string { return strings.Replace(node, tunnel, text, 1) }
	// Every invalid case is one edit away from a valid node config.
	for _, valid := range []string{node, publicRelay} {
		if _, err := decode(strings.NewReader(valid)); err != nil {
			t.Fatalf("base config rejected: %v", err)
		}
	}
	for _, tt := range []struct {
		name, data string
		is         error  // sentinel for errors the JSON library defines
		at         string // where the JSON library rejects the input; see jsonError
		want       string // message for errors this package defines
	}{
		{name: "empty", data: "", is: io.ErrUnexpectedEOF},
		{name: "trailing value", data: node + " {}", at: "syntax"},
		{name: "duplicate name", data: base + `, "name": "peer"}`, is: jsontext.ErrDuplicateName},
		{name: "wrong-case name", data: strings.Replace(node, `"name"`, `"Name"`, 1), is: json.ErrUnknownName},
		{name: "unknown member", data: base + `, "hub": {"peers": ["peer"]}}`, is: json.ErrUnknownName},
		{name: "unknown credential", data: strings.Replace(node, `{"tunnel": "`, `{"extra": "`, 1), is: json.ErrUnknownName},
		{name: "unknown option", data: base + `, "option": {"locak": true}}`, is: json.ErrUnknownName},
		{name: "high ingress port", data: strings.Replace(node, "8080", "65536", 1), at: "/ingress/tunnel"},
		{name: "string ingress port", data: strings.Replace(node, "8080", `"8080"`, 1), at: "/ingress/tunnel"},
		{name: "missing name", data: strings.Replace(node, `"name": "hub", `, "", 1), want: "name is required"},
		{name: "bad name", data: strings.Replace(node, `"hub"`, `"hub.node"`, 1), want: `"hub.node" is not a lowercase DNS label`},
		{name: "old array form", data: credential(`["hub.example.com", "/tunnel", "tunnel-secret"]`), at: "/credential/tunnel"},
		{name: "no scheme", data: credential(`"//:tunnel-secret@hub.example.com/tunnel"`), want: "credential must be"},
		{name: "other scheme", data: credential(`"https://:tunnel-secret@hub.example.com/tunnel"`), want: "credential must be"},
		{name: "query", data: credential(`"wss://:tunnel-secret@hub.example.com/tunnel?ed=2048"`), want: "query or fragment"},
		{name: "fragment", data: credential(`"wss://:tunnel-secret@hub.example.com/tunnel#x"`), want: "query or fragment"},
		{name: "parse error hides password", data: credential(`"wss://:tunnel-secret@hub example.com/tunnel"`), want: "credential must be"},
		{name: "password as user name", data: credential(`"wss://tunnel-secret@hub.example.com/tunnel"`), want: "needs a password"},
		{name: "empty password", data: credential(`"wss://:@hub.example.com/tunnel"`), want: "needs a password"},
		{name: "missing path", data: credential(`"wss://:tunnel-secret@hub.example.com"`), want: "needs a path"},
		{name: "port zero", data: credential(`"wss://:tunnel-secret@hub.example.com:0/tunnel"`), want: "port must be between"},
		{name: "port too high", data: credential(`"wss://:tunnel-secret@hub.example.com:65536/tunnel"`), want: "port must be between"},
		// An explicit empty credential is invalid, not the same as an omitted one.
		{name: "empty credential", data: credential(`""`), want: "credential must be"},
		{name: "missing tunnel credential", data: strings.Replace(node, `"credential": {"tunnel": `+tunnel+`}, `, "", 1), want: "credential.tunnel is required"},
		{name: "unused relay credential", data: strings.Replace(node, tunnel+"}", withRelay, 1), want: "credential.relay is not used"},
		{name: "unused tunnel credential", data: `{"name": "relay", "credential": {"tunnel": ` + tunnel + `, "relay": ` + relay + `}, "ingress": {"public": 80}}`, want: "credential.tunnel is not used"},
		{name: "public without relay", data: public, want: "credential.relay is required"},
		{name: "proxy without relay", data: strings.Replace(public, `"public"`, `"proxy"`, 1), want: "credential.relay is required"},
		{name: "relay hop without relay", data: `{"name": "peer", "credential": {"tunnel": ` + tunnel + `}, "egress": {"git": "192.0.2.11"}, "option": {"relay": true}}`, want: "credential.relay is required"},
		{name: "link without tunnel", data: `{"name": "laptop", "link": {"peer": {"remote": ["9000:22"]}}}`, want: "credential.tunnel is required"},
		{name: "identical trust passwords", data: strings.Replace(publicRelay, "relay-secret@", "tunnel-secret@", 1), want: "passwords must differ"},
		{name: "identical trust paths", data: strings.Replace(publicRelay, "relay.example.com/relay", "relay.example.com/tunnel", 1), want: "paths must differ"},
		// The handler compares decoded paths, so escaping does not make them differ.
		{name: "identical escaped trust paths", data: strings.Replace(publicRelay, "relay.example.com/relay", "relay.example.com/%74unnel", 1), want: "paths must differ"},
		{name: "duplicate local port", data: base + `, "link": {"peer": {"remote": ["8080:22"]}}}`, want: "already used"},
		{name: "missing local alias", data: base + `, "forward": {"git": ["2202:22"]}}`, want: "no local egress alias"},
		{name: "bad egress alias", data: base + `, "egress": {"Git": "192.0.2.10"}}`, want: `"Git" is not a lowercase DNS label`},
		{name: "bad remote alias", data: base + `, "link": {"peer": {"remote.alias": ["2202:22"]}}}`, want: `"remote.alias" is not a lowercase DNS label`},
		{name: "empty egress host", data: base + `, "egress": {"app": ""}}`, want: "domain length 0"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := decode(strings.NewReader(tt.data))
			switch {
			case err == nil:
				t.Fatal("invalid config accepted")
			case tt.is != nil && !errors.Is(err, tt.is):
				t.Fatalf("decode error = %v, want %v", err, tt.is)
			case tt.at != "" && jsonError(err) != tt.at:
				t.Fatalf("decode error = %v, want the JSON library to reject %s", err, tt.at)
			case !strings.Contains(err.Error(), tt.want):
				t.Fatalf("decode error = %v, want message containing %q", err, tt.want)
			}
			if strings.Contains(err.Error(), "-secret") {
				t.Fatal("error disclosed a password")
			}
		})
	}
}

func TestLoad(t *testing.T) {
	path := filepath.Join(t.TempDir(), "node.json")
	if err := os.WriteFile(path, []byte(fullConfig), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil || cfg.Name != "hub" {
		t.Fatalf("Load = %q, %v", cfg.Name, err)
	}
	// Save writes a config that loads back the same, escaped password included.
	if err := cfg.Save(path); err != nil {
		t.Fatal(err)
	}
	if back, err := Load(path); err != nil || !reflect.DeepEqual(back, cfg) {
		t.Errorf("reloaded %+v, %v; want %+v", back, err, cfg)
	}
	if _, err := Load(filepath.Join(t.TempDir(), "missing.json")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("missing config = %v, want file not found", err)
	}
}

// Printing or logging a credential, or printing a config that holds one, must
// not leak the password; only MarshalText, which saves configs, writes it.
func TestCredentialHidesPassword(t *testing.T) {
	const text = "wss://:tunnel-secret@hub.example.com/tunnel"
	var c Credential
	if err := c.UnmarshalText([]byte(text)); err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	slog.New(slog.NewTextHandler(&logs, nil)).Info("text", "credential", c)
	slog.New(slog.NewJSONHandler(&logs, nil)).Info("json", "credential", &c)
	cfg := Config{Credential: Credentials{Tunnel: c}}
	for _, out := range []string{fmt.Sprint(c), fmt.Sprintf("%+v", &c), fmt.Sprintf("%+v", cfg), logs.String()} {
		if strings.Contains(out, "tunnel-secret") {
			t.Errorf("output leaks the password: %s", out)
		}
	}
	if got, err := c.MarshalText(); err != nil || string(got) != text {
		t.Errorf("MarshalText = %q, %v; want %q", got, err, text)
	}
}

func TestPortMapping(t *testing.T) {
	for text, want := range map[string]PortMapping{
		"1:1":         {Listen: 1, Target: 1},
		"2202:22":     {Listen: 2202, Target: 22},
		"65535:65535": {Listen: 65535, Target: 65535},
	} {
		var got PortMapping
		if err := got.UnmarshalText([]byte(text)); err != nil || got != want {
			t.Errorf("PortMapping %q = %+v, %v; want %+v", text, got, err, want)
		}
		if back, err := want.MarshalText(); err != nil || string(back) != text {
			t.Errorf("PortMapping %+v marshals to %q, %v; want %q", want, back, err, text)
		}
	}
	for _, text := range []string{"", "1025", "1025:", ":22", "0:22", "22:0", "65536:22", "22:65536", "1:2:3", " 1:2", "+1:2", "a:b"} {
		var got PortMapping
		if err := got.UnmarshalText([]byte(text)); err == nil {
			t.Errorf("PortMapping %q accepted as %+v", text, got)
		}
	}
}
