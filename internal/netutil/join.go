// Package netutil provides small network building blocks: host addresses, DNS
// labels, and stream joining.
package netutil

import (
	"errors"
	"io"
	"sync"
	"time"
)

// drainTimeout bounds how long Join waits for a stream that cannot half-close,
// such as WebSocket, to finish after the other side has. Its client is
// typically still consuming the response, which clients such as V2Ray drop if
// the WebSocket closes first. This is a resource bound, not proof of delivery.
const drainTimeout = 30 * time.Second

var errDrainTimeout = errors.New("netutil: timed out waiting for the other direction")

type closeWriter interface {
	CloseWrite() error
}

// Join copies in both directions and takes ownership of a and b. Their Close
// methods must eventually unblock pending I/O; bounded protocol handshakes are
// allowed. CloseWrite, if supported, must return without waiting for the peer.
//
// When one direction reaches EOF, Join follows what the streams support:
//   - A stream with CloseWrite half-closes, so its EOF only ends its sending.
//     Join passes the EOF on and keeps the other direction open as long as it
//     lasts, or for at most 30 seconds if the other stream cannot half-close
//     and so cannot learn of the EOF.
//   - A stream without CloseWrite has no half-close, so its EOF means it is
//     finished and nothing more can reach it. Join ends at once.
//
// The drain timer starts at that first EOF, not at connection establishment or
// while a stream is idle. A copy or half-close error, or the drain timeout,
// stops both directions. Join closes both streams concurrently and waits for
// all copy tasks to exit; the time spent closing is additional to the drain.
func Join(a, b io.ReadWriteCloser) error {
	var wg sync.WaitGroup
	defer func() {
		wg.Go(func() { a.Close() })
		wg.Go(func() { b.Close() })
		wg.Wait()
	}()

	type result struct {
		from, to io.ReadWriteCloser
		err      error
	}
	done := make(chan result, 2)
	wg.Go(func() { done <- result{from: b, to: a, err: forward(a, b)} })
	wg.Go(func() { done <- result{from: a, to: b, err: forward(b, a)} })

	first := <-done
	// The direction to → from is still running, but only worth it while
	// from is listening: a stream without half-close is gone after its EOF.
	if first.err != nil || !halfCloses(first.from) {
		return first.err
	}
	// to finishes on its own once it learns of the EOF, which needs
	// half-close; otherwise it gets drainTimeout to finish.
	var timeout <-chan time.Time // nil waits as long as the other direction lasts.
	if !halfCloses(first.to) {
		timeout = time.After(drainTimeout)
	}
	select {
	case second := <-done:
		return second.err
	case <-timeout:
		return errDrainTimeout
	}
}

func halfCloses(s io.ReadWriteCloser) bool {
	_, ok := s.(closeWriter)
	return ok
}

func forward(dst io.Writer, src io.Reader) error {
	if _, err := io.Copy(dst, src); err != nil {
		return err
	}
	if c, ok := dst.(closeWriter); ok {
		return c.CloseWrite()
	}
	return nil
}
