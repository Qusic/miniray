// Package netutil provides small network building blocks: host addresses, DNS
// labels, and stream joining.
package netutil

import (
	"fmt"
	"io"
	"sync"
	"time"
)

// Join's idle limits, once one direction has finished:
//   - lingerTimeout, after the target has. Closing is how the client learns
//     that the response is complete, but clients such as V2Ray drop what they
//     have not yet delivered when the WebSocket closes. Shadowrocket is closed
//     source and may do the same, so every client gets a moment first, as
//     long as V2Ray and Xray servers give.
//   - drainTimeout, after the client has, while the target's response may
//     still be coming. It bounds resources; it is no proof of delivery.
const (
	lingerTimeout = time.Second
	drainTimeout  = 30 * time.Second
)

var errDrainTimeout = fmt.Errorf(
	"netutil: target sent nothing for %v after the client finished sending",
	drainTimeout,
)

// Join copies between a client's stream and its target's in both directions,
// and takes ownership of both. Their Close methods must eventually unblock
// pending I/O, though bounded protocol handshakes are allowed; CloseWrite, if
// any, must not wait for the peer.
//
// A stream with CloseWrite can half-close, so its EOF only ends its sending.
// If the other stream can half-close too, Join passes the EOF on, and the
// other direction goes on as long as it lasts. If not, the other stream never
// learns of the EOF, so its direction goes on only until it has been idle for
// lingerTimeout after the target's EOF, or drainTimeout after the client's.
// Idleness counts from the EOF at the earliest; a connection that is merely
// idle stays open. A stream without CloseWrite is gone after its EOF, and Join
// ends at once.
//
// Join returns the first copy or half-close error, or an error if the drain
// runs out; the linger running out is a normal end. It closes both streams
// concurrently and waits for all copying to stop before returning.
func Join(client, target io.ReadWriteCloser) error {
	var wg sync.WaitGroup
	defer func() {
		wg.Go(func() { client.Close() })
		wg.Go(func() { target.Close() })
		wg.Wait()
	}()

	// A direction from a stream that can half-close is never waited on, so it
	// skips tracking idleness, and io.Copy can still splice between TCP
	// streams.
	_, clientHalfCloses := client.(closeWriter)
	_, targetHalfCloses := target.(closeWriter)
	request := &direction{from: client, to: target, trackIdle: !clientHalfCloses}
	response := &direction{from: target, to: client, trackIdle: !targetHalfCloses}
	requestDone, responseDone := make(chan error, 1), make(chan error, 1)
	wg.Go(func() { requestDone <- request.copy() })
	wg.Go(func() { responseDone <- response.copy() })

	select {
	case err := <-responseDone:
		if err == nil && targetHalfCloses {
			if clientHalfCloses {
				return <-requestDone
			}
			return request.wait(requestDone, lingerTimeout, nil)
		}
		return err
	case err := <-requestDone:
		if err == nil && clientHalfCloses {
			if targetHalfCloses {
				return <-responseDone
			}
			return response.wait(responseDone, drainTimeout, errDrainTimeout)
		}
		return err
	}
}

// direction copies one way through a Join. With trackIdle, it records when
// from last yielded data.
type direction struct {
	from      io.Reader
	to        io.Writer
	trackIdle bool

	mu       sync.Mutex
	lastRead time.Time
}

func (d *direction) copy() error {
	var src io.Reader = d.from
	if d.trackIdle {
		src = d
	}
	if _, err := io.Copy(d.to, src); err != nil {
		return err
	}
	if c, ok := d.to.(closeWriter); ok {
		return c.CloseWrite()
	}
	return nil
}

func (d *direction) Read(p []byte) (int, error) {
	n, err := d.from.Read(p)
	if n > 0 {
		d.mu.Lock()
		d.lastRead = time.Now()
		d.mu.Unlock()
	}
	return n, err
}

// wait returns what done reports, or idleErr once d has moved no data for
// limit, counting from the call at the earliest.
func (d *direction) wait(done <-chan error, limit time.Duration, idleErr error) error {
	timer := time.NewTimer(limit)
	defer timer.Stop()
	for {
		select {
		case err := <-done:
			return err
		case <-timer.C:
			d.mu.Lock()
			idle := time.Since(d.lastRead)
			d.mu.Unlock()
			if idle >= limit {
				return idleErr
			}
			timer.Reset(limit - idle)
		}
	}
}

type closeWriter interface {
	CloseWrite() error
}
