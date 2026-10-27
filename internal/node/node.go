// Package node runs a miniray node from its config: it serves the node's local
// ports and dials the targets of the streams that arrive there. The hub also
// routes to the egress aliases of registered spokes, and every other node with
// egress aliases registers with the hub as a spoke.
//
// Node is the only package that puts the others together; they are building
// blocks that depend on nothing but netutil. Here servers bind the ports,
// handlers accept streams, routes decide where a stream goes, and dialers reach
// targets, only along explicit ways: proxy environment variables are never
// read. Handlers log each request; routes and dialers only return errors.
// Mechanism code uses Go's words, such as handler and dial; topology words,
// such as ingress and egress, appear only in the config, in assemble, and in
// logs.
package node

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"slices"
	"strconv"
	"time"

	"github.com/Qusic/miniray/internal/config"
	"github.com/Qusic/miniray/internal/netutil"
	"github.com/Qusic/miniray/internal/tunnel"
)

// setupTimeout bounds each step of setting up a stream: reading the HTTP
// request header, reading the proxy request, and dialing the target.
// Established streams have no idle timeout.
const setupTimeout = 10 * time.Second

// Run serves the node until one of its servers stops, or forever on a node
// that only runs a spoke.
func Run(cfg config.Config) error {
	servers, spoke := assemble(cfg)
	if len(servers) == 0 && spoke == nil {
		return fmt.Errorf("node %s: nothing to run", cfg.Name)
	}
	stopped := make(chan error, 2)
	if spoke != nil {
		go func() { stopped <- spoke.Run(context.Background()) }()
	}
	if len(servers) != 0 {
		go func() { stopped <- listenAndServe(servers) }()
	}
	return <-stopped
}

// assemble translates the node config into the node's runtime parts: a server
// for each active ingress, the router behind them, and the spoke of a node
// that registers with the hub. It is the only code that reads the config.
func assemble(cfg config.Config) (servers []server, spoke *tunnel.Spoke) {
	// relay reaches targets through the relay's public ingress.
	var relay dialFunc
	if !cfg.Credential.Relay.IsZero() {
		relay = newTrojanClient(cfg.Credential.Relay.URL(), dialDirect).dial
	}
	r := router{self: cfg.Name, aliases: aliases(cfg.Egress)}
	isHub := cfg.Ingress.Tunnel != 0
	if isHub {
		r.hub = tunnel.NewHub(cfg.Name, slog.With("server", "ingress.tunnel"))
	} else if !cfg.Credential.Tunnel.IsZero() {
		via := dialFunc(dialDirect)
		if cfg.Option.Relay {
			via = relay
		}
		r.toHub = newTrojanClient(cfg.Credential.Tunnel.URL(), via).dial
	}

	bind := ""
	if cfg.Option.Local {
		bind = "127.0.0.1"
	}
	add := func(name string, port uint16, serve func(net.Listener) error) {
		servers = append(servers, server{name: name, addr: net.JoinHostPort(bind, strconv.Itoa(int(port))), serve: serve})
	}
	if cfg.Ingress.Tunnel != 0 {
		add("ingress.tunnel", cfg.Ingress.Tunnel, newTrojanHandler(
			cfg.Credential.Tunnel.URL(),
			r.routeTunnel,
			slog.With("server", "ingress.tunnel"),
		).serve)
	}
	if cfg.Ingress.Private != 0 {
		add("ingress.private", cfg.Ingress.Private, newHTTPProxyHandler(
			dialDirect,
			slog.With("server", "ingress.private"),
		).serve)
	}
	if cfg.Ingress.Public != 0 {
		add("ingress.public", cfg.Ingress.Public, newTrojanHandler(
			cfg.Credential.Relay.URL(),
			routeDirect,
			slog.With("server", "ingress.public"),
		).serve)
	}
	if cfg.Ingress.Proxy != 0 {
		add("ingress.proxy", cfg.Ingress.Proxy, newHTTPProxyHandler(
			relay,
			slog.With("server", "ingress.proxy"),
		).serve)
	}
	// listen serves the local port of each mapping, joining it with host at the
	// mapping's target port, as dial reaches it.
	listen := func(name string, host netutil.Address, mappings []config.PortMapping, dial dialFunc) {
		log := slog.With("server", name)
		for _, m := range mappings {
			add(name, m.Listen, tcpHandler{host: host, port: m.Target, dial: dial, log: log}.serve)
		}
	}
	for _, alias := range slices.Sorted(maps.Keys(cfg.Forward)) {
		listen("forward."+string(alias), cfg.Egress[alias], cfg.Forward[alias], dialDirect)
	}
	for _, node := range slices.Sorted(maps.Keys(cfg.Link)) {
		for _, alias := range slices.Sorted(maps.Keys(cfg.Link[node])) {
			// Two labels always make a valid domain.
			virtual, _ := netutil.DomainAddress(tunnel.Target{Kind: tunnel.Route, Node: node, Alias: alias}.Domain())
			listen("link."+string(node)+"."+string(alias), virtual, cfg.Link[node][alias], r.dialTunnel)
		}
	}

	if !isHub && len(cfg.Egress) != 0 {
		spoke = &tunnel.Spoke{
			Name:      cfg.Name,
			DialHub:   r.toHub,
			DialAlias: r.aliases.dial,
			Log:       slog.Default(),
		}
	}
	return servers, spoke
}
