package tunnel

import (
	"context"
	"errors"
	"net"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/Qusic/miniray/internal/netutil"
)

// The delay before each new attempt doubles up to maxBackoff, whether the hub
// was unreachable or rejected the registration by closing the stream, and
// starts over once the hub has acknowledged a registration. Run ends with the
// cause of its context.
func TestSpokeReconnects(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		var attempts []time.Duration
		ctx, cancel := context.WithCancelCause(t.Context())
		stop := errors.New("stop")
		spoke := Spoke{
			Name: "lab",
			Log:  quiet,
			DialHub: func(_ context.Context, host netutil.Address, port uint16) (net.Conn, error) {
				if host.Domain() != "lab.tunnel" || port != 0 {
					t.Errorf("registered at %s:%d, want lab.tunnel:0", host, port)
				}
				attempts = append(attempts, time.Since(start))
				conn, hub := net.Pipe()
				switch len(attempts) {
				case 1, 3, 5, 7:
					return nil, errors.New("hub unreachable")
				case 8:
					// Accepted: the hub acknowledges, then the stream drops.
					go func() {
						hub.Write([]byte(`{"type":"ping"}` + "\n"))
						hub.Close()
					}()
					return conn, nil
				case 10:
					cancel(stop)
				}
				hub.Close() // rejected
				return conn, nil
			},
		}
		if err := spoke.Run(ctx); err != stop {
			t.Errorf("Run = %v, want the cause %v", err, stop)
		}
		s := time.Second
		want := []time.Duration{0, 1 * s, 3 * s, 7 * s, 15 * s, 31 * s, 61 * s, 91 * s, 92 * s, 94 * s}
		if !slices.Equal(attempts, want) {
			t.Errorf("attempts at %v, want %v", attempts, want)
		}
	})
}

// Connections already open outlive the control stream, while the hub stops
// routing to the spoke at once.
func TestSpokeKeepsOpenConnections(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		hub := NewHub("hub", quiet)
		spoke, _ := spokeServing(t, hub, "lab")
		ctx, cancel := context.WithCancel(t.Context())
		stopped := make(chan error)
		go func() { stopped <- spoke.Run(ctx) }()
		synctest.Wait()
		conn, err := hub.Dial(t.Context(), "lab", "nas", 22)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		cancel()
		<-stopped
		synctest.Wait()
		if _, err := hub.Dial(t.Context(), "lab", "nas", 22); err == nil || !strings.Contains(err.Error(), "not registered") {
			t.Errorf("Dial to a spoke that left = %v, want not registered", err)
		}
		exchange(t, conn, "still open")
	})
}
