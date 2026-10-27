package node

import (
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

// A port that fails to bind must stop the whole start: nothing is served, the
// error names the server, and the ports already bound are released.
func TestListenAndServeStartsNothingOnBindFailure(t *testing.T) {
	taken, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer taken.Close()
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	free := probe.Addr().String()
	probe.Close()

	serve := func(net.Listener) error {
		t.Error("served although a bind failed")
		return nil
	}
	err = listenAndServe([]server{
		{"ingress.a", free, serve},
		{"ingress.b", taken.Addr().String(), serve},
	})
	if want := "ingress.b: listen on " + taken.Addr().String() + ": "; err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Fatalf("listenAndServe = %v, want an error starting with %q", err, want)
	}
	again, err := net.Listen("tcp", free)
	if err != nil {
		t.Fatalf("ingress.a still holds its port: %v", err)
	}
	again.Close()
}

// Once one server stops, they all stop: listenAndServe returns that server's
// error, named after it, and closes the other listeners, which ends their
// serving too.
func TestListenAndServeStopsTogether(t *testing.T) {
	failed := errors.New("failed")
	accepted := make(chan error, 1)
	servers := []server{
		{"ingress.a", "127.0.0.1:0", func(l net.Listener) error {
			_, err := l.Accept()
			accepted <- err
			return err
		}},
		{"ingress.b", "127.0.0.1:0", func(net.Listener) error { return failed }},
	}
	stopped := make(chan error, 1)
	go func() { stopped <- listenAndServe(servers) }()
	select {
	case err := <-stopped:
		if !errors.Is(err, failed) || !strings.HasPrefix(err.Error(), "ingress.b: ") {
			t.Fatalf("listenAndServe = %v, want ingress.b's failure", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("listenAndServe kept serving after ingress.b stopped")
	}
	select {
	case err := <-accepted:
		if !errors.Is(err, net.ErrClosed) {
			t.Errorf("ingress.a: Accept = %v, want its listener closed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ingress.a kept serving")
	}
}
