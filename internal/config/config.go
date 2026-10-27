// Package config reads the hand-maintained topology and the node configs
// prepared from it. Decoding checks each value on its own; validation checks
// how the values fit together.
package config

import (
	"encoding/json/jsontext"
	"encoding/json/v2"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/Qusic/miniray/internal/netutil"
)

// Config is a node config, what one node runs: its name, its Node section of
// the topology, and exactly the credentials that section uses.
type Config struct {
	Name       netutil.Label `json:"name"`
	Credential Credentials   `json:"credential,omitzero"`
	Node       `json:",inline"`
}

// Load reads and validates a node config.
func Load(path string) (Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return Config{}, fmt.Errorf("config %s: %w", path, err)
	}
	defer file.Close()
	cfg, err := decode(file)
	if err != nil {
		return Config{}, fmt.Errorf("config %s: %w", path, err)
	}
	return cfg, nil
}

func decode(r io.Reader) (Config, error) {
	var cfg Config
	if err := unmarshal(r, &cfg); err != nil {
		return Config{}, err
	}
	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// unmarshal accepts exactly one JSON value and rejects unknown or duplicate
// names.
func unmarshal(r io.Reader, v any) error {
	return json.UnmarshalRead(r, v, json.RejectUnknownMembers(true))
}

// Save writes c as indented JSON that Load accepts. It creates new files with
// mode 0600, since they hold passwords.
func (c Config) Save(path string) error {
	data, err := json.Marshal(c, json.Deterministic(true), jsontext.WithIndent("  "))
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(data, '\n'), 0o600)
}

// Credentials are two separate trust domains. Tunnel secures the private
// channel through the hub: its ingress, egress registration, and links. Relay
// secures the public ingress, which reaches only ordinary hosts, so it can be
// shared without opening the tunnel.
type Credentials struct {
	Tunnel Credential `json:"tunnel,omitzero"`
	Relay  Credential `json:"relay,omitzero"`
}

// Credential is a URL like "wss://:password@host/path", the same on both ends;
// reserved characters are percent-escaped, such as a / in the password as %2F.
// Clients dial it, wss through a TLS front proxy or ws for tests and trusted
// networks; listeners serve plain WebSocket and check only the path and the
// password. The zero Credential means none is configured.
type Credential struct{ url url.URL }

// URL returns the credential in full, password included.
func (c Credential) URL() url.URL {
	return c.url
}

func (c Credential) password() string {
	password, _ := c.url.User.Password()
	return password
}

// IsZero reports whether no credential is configured. Save omits it.
func (c Credential) IsZero() bool {
	return c.url == url.URL{}
}

// String hides the password, so printing a credential cannot leak it.
func (c Credential) String() string {
	return c.url.Redacted()
}

// LogValue hides the password from slog, which would otherwise log MarshalText.
func (c Credential) LogValue() slog.Value {
	return slog.StringValue(c.String())
}

func (c Credential) MarshalText() ([]byte, error) {
	return []byte(c.url.String()), nil
}

// UnmarshalText leaves URL syntax to url.Parse and checks only what a
// credential adds. Parse errors quote the input, which holds the password, so
// only fixed messages are returned.
func (c *Credential) UnmarshalText(text []byte) error {
	u, err := url.Parse(string(text))
	if err != nil || (u.Scheme != "ws" && u.Scheme != "wss") || u.Host == "" {
		return errors.New(`credential must be a URL like "wss://:password@host/path", with scheme wss or ws`)
	}
	password, _ := u.User.Password()
	switch {
	case u.User.Username() != "" || password == "":
		return errors.New("credential needs a password and no user name")
	case !validPort(u.Port()):
		return errors.New("credential port must be between 1 and 65535")
	case u.Path == "":
		return errors.New("credential needs a path")
	case u.RawQuery != "" || u.Fragment != "":
		// Neither would be used; a bare ? or # carries nothing and passes.
		return errors.New("credential must not have a query or fragment")
	}
	c.url = *u
	return nil
}

// validPort reports whether a URL port is absent, meaning the scheme's default,
// or between 1 and 65535. url.Parse only checks that it has digits.
func validPort(port string) bool {
	if port == "" {
		return true
	}
	n, err := strconv.ParseUint(port, 10, 16)
	return err == nil && n != 0
}

// Node is one node's section of the topology.
type Node struct {
	Ingress Ingress                                           `json:"ingress,omitzero"`
	Egress  map[netutil.Label]netutil.Address                 `json:"egress,omitempty"`
	Forward map[netutil.Label][]PortMapping                   `json:"forward,omitempty"`
	Link    map[netutil.Label]map[netutil.Label][]PortMapping `json:"link,omitempty"`
	Option  Options                                           `json:"option,omitzero"`
}

// usedCredentials reports which credentials the node uses: those of the
// endpoints it has and of those it uses. A node with tunnel ingress has the
// hub, and every other node with egress or links uses it to register its
// aliases or reach its links. A node with public ingress has a relay; the
// proxy ingress uses one, and so does the connection to the hub with
// option.relay.
func (n Node) usedCredentials() (tunnel, relay bool) {
	hasHub := n.Ingress.Tunnel != 0
	usesHub := !hasHub && (len(n.Egress) != 0 || len(n.Link) != 0)
	hasRelay := n.Ingress.Public != 0
	usesRelay := n.Ingress.Proxy != 0 || (n.Option.Relay && usesHub)
	return hasHub || usesHub, hasRelay || usesRelay
}

// Ingress holds the local port of each ingress; 0 means disabled. Tunnel and
// public serve Trojan over WebSocket with the tunnel and relay credentials.
// Private and proxy are HTTP proxies without authentication, for the local
// machine or a private network; private dials directly and proxy through the
// relay, and neither reaches .tunnel names, which links do.
type Ingress struct {
	Tunnel  uint16 `json:"tunnel,omitzero"`
	Private uint16 `json:"private,omitzero"`
	Public  uint16 `json:"public,omitzero"`
	Proxy   uint16 `json:"proxy,omitzero"`
}

// PortMapping is "listen:target" in JSON. Forward targets a local egress alias;
// Link's target alias belongs to the remote node.
type PortMapping struct {
	Listen uint16
	Target uint16
}

func (p PortMapping) MarshalText() ([]byte, error) {
	return fmt.Appendf(nil, "%d:%d", p.Listen, p.Target), nil
}

func (p *PortMapping) UnmarshalText(text []byte) error {
	listen, target, _ := strings.Cut(string(text), ":")
	local, err1 := strconv.ParseUint(listen, 10, 16)
	remote, err2 := strconv.ParseUint(target, 10, 16)
	if err1 != nil || err2 != nil || local == 0 || remote == 0 {
		return fmt.Errorf("port mapping %q: expected listen:target ports between 1 and 65535", text)
	}
	*p = PortMapping{Listen: uint16(local), Target: uint16(remote)}
	return nil
}

// Options adjust how the node listens and connects: Local binds every listener
// to 127.0.0.1, and Relay reaches the hub through relay (the hub ignores it).
type Options struct {
	Local bool `json:"local,omitzero"`
	Relay bool `json:"relay,omitzero"`
}
