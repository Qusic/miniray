package node

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/synctest"

	"github.com/Qusic/miniray/internal/netutil"
	"github.com/Qusic/miniray/internal/protocol"
	"github.com/Qusic/miniray/internal/tunnel"
)

// routeTunnel checks the target before dialing. The context is already
// canceled, so a target that does reach dialing fails with context.Canceled,
// and no case touches the network.
func TestRouteTunnelChecksTargets(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	r := router{self: "hub", hub: tunnel.NewHub("hub", quiet)}
	for _, tt := range []struct {
		name string
		host netutil.Address
		want string // rejection message, or "" when the target reaches dialing
	}{
		{"ordinary host", ipAddress(t, "127.0.0.1"), ""},
		{"unregistered node", domainAddress(t, "web.lab.tunnel"), "spoke lab is not registered"},
		{"malformed name", domainAddress(t, "a.b.c.tunnel"), "neither"},
		// Spoke streams go to the hub, which expects no such connection.
		{"stray data stream", domainAddress(t, "lab.tunnel"), "spoke lab has no pending connection"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conn, peer := net.Pipe()
			defer peer.Close()
			defer conn.Close()
			err := r.routeTunnel(ctx, conn, tt.host, 1)
			if tt.want == "" {
				if !errors.Is(err, context.Canceled) {
					t.Errorf("got %v, want the canceled dial", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Errorf("got %v, want %q before dialing", err, tt.want)
			}
		})
	}
}

// Through the hub's tunnel ingress, spokes register and deliver connections,
// and clients reach the spokes' aliases.
func TestRouteTunnelReachesSpokes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		p := protocol.NewTrojan("secret")
		hub := tunnel.NewHub("hub", quiet)
		srv := httptest.NewTestServer(t, trojanHandler{
			path:   "/",
			trojan: p,
			route:  router{self: "hub", hub: hub}.routeTunnel,
			log:    quiet,
		})
		client := newTrojanClient(mustURL(t, "ws://:secret@example.com/"), memoryDial(srv))
		dialed := make(chan string, 1)
		spoke := tunnel.Spoke{
			Name:    "spoke",
			DialHub: client.dial,
			DialAlias: func(_ context.Context, alias netutil.Label, port uint16) (net.Conn, error) {
				dialed <- fmt.Sprintf("%s:%d", alias, port)
				return echo(), nil
			},
			Log: quiet,
		}
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		go spoke.Run(ctx)
		synctest.Wait()

		conn, err := client.dial(t.Context(), domainAddress(t, "nas.spoke.tunnel"), 22)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		write(t, conn, []byte("hello"))
		if got := <-dialed; got != "nas:22" {
			t.Errorf("spoke dialed %s, want nas:22", got)
		}
		read(t, conn, "hello")
	})
}

// dialTunnel reaches the node's own aliases directly and other nodes' through
// the hub: on the hub by asking the spoke, elsewhere by way of the hub's tunnel
// ingress. The context is already canceled, so a target that does reach
// dialing fails with context.Canceled, and no case touches the network.
func TestDialTunnel(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	own := aliases{"web": ipAddress(t, "127.0.0.1")}
	hub := router{self: "hub", aliases: own, hub: tunnel.NewHub("hub", quiet)}
	lab := router{self: "lab", aliases: own, toHub: func(_ context.Context, host netutil.Address, port uint16) (net.Conn, error) {
		return nil, fmt.Errorf("asked the hub for %s:%d", host, port)
	}}
	for _, tt := range []struct {
		name string
		r    router
		host string
		want string // the error, or "" when the alias's host is dialed
	}{
		{"own alias on the hub", hub, "web.hub.tunnel", ""},
		{"missing own alias", hub, "mail.hub.tunnel", "no egress alias mail"},
		{"spoke's alias on the hub", hub, "web.lab.tunnel", "spoke lab is not registered"},
		{"own alias elsewhere", lab, "web.lab.tunnel", ""},
		{"other alias elsewhere", lab, "web.hub.tunnel", "asked the hub for web.hub.tunnel:1"},
		{"no alias", hub, "lab.tunnel", "names no alias"},
		{"malformed name", lab, "a.b.c.tunnel", "neither"},
	} {
		conn, err := tt.r.dialTunnel(ctx, domainAddress(t, tt.host), 1)
		if conn != nil {
			conn.Close()
			t.Fatalf("%s: opened a connection", tt.name)
		}
		if tt.want == "" {
			if !errors.Is(err, context.Canceled) {
				t.Errorf("%s: got %v, want the canceled dial", tt.name, err)
			}
		} else if err == nil || !strings.Contains(err.Error(), tt.want) {
			t.Errorf("%s: got %v, want %q", tt.name, err, tt.want)
		}
	}
}
