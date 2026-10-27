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

func TestJoin(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, a := pipePair(t)
		b, target := pipePair(t)
		done := start(a, b)

		for _, direction := range []struct{ from, to *halfPipe }{{client, target}, {target, client}} {
			written := send(direction.from, []byte("hello"))
			read(t, direction.to, []byte("hello"))
			if err := <-written; err != nil {
				t.Fatal(err)
			}
		}
		client.CloseWrite()
		eof(t, target)
		target.CloseWrite()
		eof(t, client)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}

func TestJoinWaitsForHalfClosedResponse(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, a := pipePair(t)
		b, target := pipePair(t)
		done := start(a, b)
		client.CloseWrite()
		eof(t, target)

		// The request is finished, but its response may take arbitrarily long:
		// between two half-closing streams no drain bound applies.
		synctest.Sleep(time.Hour)
		written := send(target, []byte("response"))
		read(t, client, []byte("response"))
		if err := <-written; err != nil {
			t.Fatal(err)
		}
		target.CloseWrite()
		eof(t, client)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}

func TestJoinBackpressure(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, a := pipePair(t)
		b, target := pipePair(t)
		done := start(a, b)
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
		client.CloseWrite()
		eof(t, target)
		target.CloseWrite()
		eof(t, client)
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	})
}

func TestJoinEndsWhenStreamWithoutHalfCloseEnds(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, a := net.Pipe()
		b, target := pipePair(t)
		done := start(a, b)
		start := time.Now()
		// The client is gone and nothing more can reach it, so the target is
		// closed at once instead of being left to finish.
		client.Close()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(start); elapsed != 0 {
			t.Errorf("Join took %v after the client closed", elapsed)
		}
		if _, err := target.Write([]byte("late")); err == nil {
			t.Error("target still open after the client closed")
		}
	})
}

func TestJoinDrainTimeout(t *testing.T) {
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
		// The client cannot learn that the target finished, so it gets
		// drainTimeout to finish on its own.
		start := time.Now()
		target.CloseWrite()
		if err := <-done; !errors.Is(err, errDrainTimeout) {
			t.Fatalf("Join = %v, want errDrainTimeout", err)
		}
		if elapsed := time.Since(start); elapsed != drainTimeout {
			t.Errorf("drain took %v, want %v", elapsed, drainTimeout)
		}
		eof(t, client)
	})
}

func TestJoinDrainEndsWhenClientFinishes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, a := net.Pipe()
		b, target := pipePair(t)
		done := start(a, b)
		start := time.Now()
		target.CloseWrite()
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("Join returned before the client finished: %v", err)
		default:
		}
		client.Close()
		if err := <-done; err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(start); elapsed != 0 {
			t.Errorf("Join took %v after both directions ended", elapsed)
		}
	})
}

func TestJoinError(t *testing.T) {
	for _, phase := range []string{"first", "during drain"} {
		t.Run(phase, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client, a := net.Pipe()
				b, target := pipePair(t)
				defer client.Close()
				done := start(a, b)
				start := time.Now()
				if phase == "during drain" {
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
	synctest.Test(t, func(t *testing.T) {
		client, a := pipePair(t)
		b, _ := pipePair(t)
		client.CloseWrite()
		if err := Join(a, closeWriteFailure{b}); !errors.Is(err, errCloseWrite) {
			t.Fatalf("Join = %v, want half-close error", err)
		}
		eof(t, client)
	})
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
			t.Fatal(err)
		}
		eof(t, client)
		eof(t, target)
	})
}
