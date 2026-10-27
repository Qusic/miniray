package node

import (
	"fmt"
	"log/slog"
	"net"
)

// server is one local address and how the connections accepted there are
// served. Its name, such as ingress.tunnel or link.lab.nas, labels its
// errors and logs.
type server struct {
	name  string
	addr  string
	serve func(net.Listener) error
}

// listenAndServe binds every server before serving any, so a failed bind
// starts nothing. Once one server stops, closing the listeners stops the rest.
func listenAndServe(servers []server) error {
	var listeners []net.Listener
	defer func() {
		for _, listener := range listeners {
			listener.Close()
		}
	}()
	for _, s := range servers {
		listener, err := net.Listen("tcp", s.addr)
		if err != nil {
			return fmt.Errorf("%s: listen on %s: %w", s.name, s.addr, err)
		}
		listeners = append(listeners, listener)
	}
	stopped := make(chan error, len(servers))
	for i, s := range servers {
		slog.Info("listening", "server", s.name, "address", listeners[i].Addr().String())
		go func() { stopped <- fmt.Errorf("%s: %w", s.name, s.serve(listeners[i])) }()
	}
	return <-stopped
}
