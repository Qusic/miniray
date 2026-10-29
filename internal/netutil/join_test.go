package netutil

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"testing/synctest"
	"time"
)

// Two one-way pipes model a stream with real half-close, like TCP. A net.Pipe
// has no half-close, like WebSocket.
type halfPipe struct {
	in  *io.PipeReader
	out *io.PipeWriter
}

func (p *halfPipe) Read(b []byte) (int, error)  { return p.in.Read(b) }
func (p *halfPipe) Write(b []byte) (int, error) { return p.out.Write(b) }
func (p *halfPipe) CloseWrite() error           { return p.out.Close() }
func (p *halfPipe) Close() error                { return errors.Join(p.in.Close(), p.out.Close()) }

func pipePair(t *testing.T) (*halfPipe, *halfPipe) {
	t.Helper()
	r1, w1 := io.Pipe()
	r2, w2 := io.Pipe()
	a, b := &halfPipe{r1, w2}, &halfPipe{r2, w1}
	t.Cleanup(func() { a.Close(); b.Close() })
	return a, b
}

func start(a, b io.ReadWriteCloser) <-chan error {
	done := make(chan error, 1)
	go func() { done <- Join(a, b) }()
	return done
}

func send(w io.Writer, data []byte) <-chan error {
	done := make(chan error, 1)
	go func() {
		n, err := w.Write(data)
		if err == nil && n != len(data) {
			err = io.ErrShortWrite
		}
		done <- err
	}()
	return done
}

func read(t *testing.T, r io.Reader, want []byte) {
	t.Helper()
	got := make([]byte, len(want))
	if n, err := io.ReadFull(r, got); err != nil {
		t.Fatalf("read %d/%d bytes: %v", n, len(want), err)
	}
	for i, b := range want {
		if got[i] != b {
			t.Fatalf("byte %d = %#x, want %#x", i, got[i], b)
		}
	}
}

func eof(t *testing.T, r io.Reader) {
	t.Helper()
	if n, err := r.Read(make([]byte, 1)); n != 0 || err != io.EOF {
		t.Fatalf("read = %d, %v; want EOF", n, err)
	}
}

// Between two half-closing streams, the EOF of the side that finishes first is
// passed on, and the other direction may take arbitrarily long: no linger or
// drain bound applies.
func TestJoinWaitsBetweenHalfClosingStreams(t *testing.T) {
	for _, first := range []string{"client", "target"} {
		t.Run(first+" finishes first", func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client, a := pipePair(t)
				b, target := pipePair(t)
				done := start(a, b)
				finished, other := client, target
				if first == "target" {
					finished, other = target, client
				}
				finished.CloseWrite()
				eof(t, other)

				synctest.Sleep(time.Hour)
				written := send(other, []byte("late"))
				read(t, finished, []byte("late"))
				if err := <-written; err != nil {
					t.Fatal(err)
				}
				other.CloseWrite()
				eof(t, finished)
				if err := <-done; err != nil {
					t.Fatal(err)
				}
			})
		})
	}
}

func TestJoinBackpressure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, a := pipePair(t)
		b, target := pipePair(t)
		start(a, b)
		payload := bytes.Repeat([]byte("x"), 1<<20)
		written := send(client, payload)
		synctest.Wait()
		select {
		case err := <-written:
			t.Fatalf("write completed without a receiver: %v", err)
		default:
		}
		read(t, target, payload)
		if err := <-written; err != nil {
			t.Fatal(err)
		}
	})
}

// A stream without half-close is gone after its EOF and nothing more can reach
// it, so Join closes the other stream at once instead of leaving it to finish.
func TestJoinEndsWhenStreamWithoutHalfCloseEnds(t *testing.T) {
	for _, gone := range []string{"client", "target"} {
		t.Run(gone+" ends", func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				// The stream that ends cannot half-close; the other can.
				var client, a, b, target io.ReadWriteCloser
				if gone == "client" {
					client, a = net.Pipe()
					b, target = pipePair(t)
				} else {
					client, a = pipePair(t)
					b, target = net.Pipe()
				}
				ends, other := client, target
				if gone == "target" {
					ends, other = target, client
				}
				done := start(a, b)
				start := time.Now()
				ends.Close()
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				if elapsed := time.Since(start); elapsed != 0 {
					t.Errorf("Join took %v after the %s ended", elapsed, gone)
				}
				if _, err := other.Write([]byte("late")); err == nil {
					t.Errorf("the other stream is still open after the %s ended", gone)
				}
			})
		})
	}
}

// A client whose stream cannot half-close learns that the target finished only
// when Join closes the stream. Join keeps it open while the client still sends,
// and closes it once the client has been idle for lingerTimeout; the close is a
// normal end.
func TestJoinLinger(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, a := net.Pipe()
		b, target := pipePair(t)
		defer client.Close()
		done := start(a, b)

		// An idle connection is not a finished direction.
		synctest.Sleep(time.Hour)
		select {
		case err := <-done:
			t.Fatalf("idle connection ended: %v", err)
		default:
		}
		target.CloseWrite()
		// Sending keeps the client's direction going past lingerTimeout.
		for range 3 {
			synctest.Sleep(lingerTimeout * 3 / 5)
			written := send(client, []byte("late"))
			read(t, target, []byte("late"))
			if err := <-written; err != nil {
				t.Fatal(err)
			}
		}
		last := time.Now()
		if err := <-done; err != nil {
			t.Fatalf("Join = %v, want nil", err)
		}
		if idle := time.Since(last); idle != lingerTimeout {
			t.Errorf("Join ended after %v idle, want %v", idle, lingerTimeout)
		}
		eof(t, client)
	})
}

// A target whose stream cannot half-close never learns that the client finished
// sending. Its response still reaches the client, and Join keeps going while it
// comes; a target idle for drainTimeout is an error.
func TestJoinDrainTimeout(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, a := pipePair(t)
		b, target := net.Pipe()
		defer target.Close()
		done := start(a, b)
		client.CloseWrite()
		// The response keeps the target's direction going past drainTimeout.
		for range 3 {
			synctest.Sleep(drainTimeout * 3 / 5)
			written := send(target, []byte("response"))
			read(t, client, []byte("response"))
			if err := <-written; err != nil {
				t.Fatal(err)
			}
		}
		last := time.Now()
		if err := <-done; !errors.Is(err, errDrainTimeout) {
			t.Fatalf("Join = %v, want errDrainTimeout", err)
		}
		if idle := time.Since(last); idle != drainTimeout {
			t.Errorf("Join ended after %v idle, want %v", idle, drainTimeout)
		}
		eof(t, target)
	})
}

// While Join waits on a direction, it ends as soon as that direction ends,
// without waiting out the idle limit.
func TestJoinEndsWhenWaitedDirectionEnds(t *testing.T) {
	for _, limit := range []string{"linger", "drain"} {
		t.Run(limit, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var a, b io.ReadWriteCloser
				var finished *halfPipe // half-closes first
				var waited net.Conn    // cannot half-close, so Join waits on it
				if limit == "linger" {
					waited, a = net.Pipe()
					b, finished = pipePair(t)
				} else {
					finished, a = pipePair(t)
					b, waited = net.Pipe()
				}
				done := start(a, b)
				start := time.Now()
				finished.CloseWrite()
				synctest.Wait()
				select {
				case err := <-done:
					t.Fatalf("Join returned while waiting: %v", err)
				default:
				}
				waited.Close()
				if err := <-done; err != nil {
					t.Fatal(err)
				}
				if elapsed := time.Since(start); elapsed != 0 {
					t.Errorf("Join took %v after both directions ended", elapsed)
				}
			})
		})
	}
}

func TestJoinError(t *testing.T) {
	for _, phase := range []string{"first", "during linger"} {
		t.Run(phase, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client, a := net.Pipe()
				b, target := pipePair(t)
				defer client.Close()
				done := start(a, b)
				start := time.Now()
				if phase == "during linger" {
					target.CloseWrite()
					synctest.Wait()
				}
				if err := a.SetReadDeadline(time.Now()); err != nil {
					t.Fatal(err)
				}
				if err := <-done; !errors.Is(err, os.ErrDeadlineExceeded) {
					t.Errorf("Join = %v, want deadline exceeded", err)
				}
				if elapsed := time.Since(start); elapsed != 0 {
					t.Errorf("error took %v to stop Join", elapsed)
				}
				eof(t, client)
			})
		})
	}
}

var errCloseWrite = errors.New("half-close failed")

type closeWriteFailure struct{ *halfPipe }

func (closeWriteFailure) CloseWrite() error { return errCloseWrite }

func TestJoinCloseWriteError(t *testing.T) {
	for _, failing := range []string{"target", "client"} {
		t.Run(failing+" fails to half-close", func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				// One side finishes, and passing its EOF on to the other
				// fails.
				client, a := pipePair(t)
				b, target := pipePair(t)
				finished := client
				var joinClient, joinTarget io.ReadWriteCloser = a, closeWriteFailure{b}
				if failing == "client" {
					finished = target
					joinClient, joinTarget = closeWriteFailure{a}, b
				}
				finished.CloseWrite()
				if err := Join(joinClient, joinTarget); !errors.Is(err, errCloseWrite) {
					t.Fatalf("Join = %v, want half-close error", err)
				}
				eof(t, finished)
			})
		})
	}
}

type closeTogether struct {
	net.Conn
	entered chan struct{}
	other   <-chan struct{}
}

func (c closeTogether) Close() error {
	close(c.entered)
	<-c.other
	return c.Conn.Close()
}

func TestJoinClosesBothConcurrently(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, a := net.Pipe()
		b, target := net.Pipe()
		defer client.Close()
		defer target.Close()
		aClosed, bClosed := make(chan struct{}), make(chan struct{})
		done := start(closeTogether{a, aClosed, bClosed}, closeTogether{b, bClosed, aClosed})
		if err := a.SetReadDeadline(time.Now()); err != nil {
			t.Fatal(err)
		}
		if err := <-done; !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Fatalf("Join = %v, want deadline exceeded", err)
		}
		eof(t, client)
		eof(t, target)
	})
}
