package tunnel

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"slices"
	"strings"
	"testing"
	"testing/synctest"
	"time"
	"unicode/utf8"
)

// reading returns a control whose peer sends input and then closes.
func reading(t *testing.T, input string) *control {
	conn, peer := net.Pipe()
	t.Cleanup(func() { conn.Close() })
	go func() {
		peer.Write([]byte(input))
		peer.Close()
	}()
	return newControl(conn)
}

// The wire format is one JSON object per line, as written here by hand.
func TestControlMessages(t *testing.T) {
	for _, tt := range []struct {
		line string
		m    message
	}{
		{`{"type":"open","id":7,"alias":"nas","port":22}`, message{Type: typeOpen, ID: 7, Alias: "nas", Port: 22}},
		{`{"type":"fail","id":7,"error":"connection refused"}`, message{Type: typeFail, ID: 7, Error: "connection refused"}},
		{`{"type":"ping"}`, message{Type: typePing}},
	} {
		conn, peer := net.Pipe()
		go func() {
			newControl(conn).send(tt.m)
			conn.Close()
		}()
		if got, err := io.ReadAll(peer); err != nil || string(got) != tt.line+"\n" {
			t.Errorf("send(%+v) wrote %q, %v; want %q", tt.m, got, err, tt.line+"\n")
		}
		if got, err := reading(t, tt.line+"\n").receive(); err != nil || got != tt.m {
			t.Errorf("receive(%s) = %+v, %v; want %+v", tt.line, got, err, tt.m)
		}
	}
}

// run passes every message on in order, and ends with the cause of ctx,
// closing the stream.
func TestControlRun(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn, peer := net.Pipe()
		ctx, cancel := context.WithCancelCause(t.Context())
		stop := errors.New("stop")
		var got []message
		done := make(chan error)
		go func() {
			done <- newControl(conn).run(ctx, func(m message) {
				got = append(got, m)
				if len(got) == 3 {
					cancel(stop)
				}
			})
		}()
		peer.Write([]byte(`{"type":"open","id":1,"alias":"nas","port":22}` + "\n" + `{"type":"later"}` + "\n" + `{"type":"ping"}` + "\n"))
		if err := <-done; err != stop {
			t.Errorf("run = %v, want the cause %v", err, stop)
		}
		want := []message{{Type: typeOpen, ID: 1, Alias: "nas", Port: 22}, {Type: "later"}, {Type: typePing}}
		if !slices.Equal(got, want) {
			t.Errorf("handled %+v, want %+v", got, want)
		}
		if n, err := peer.Read(make([]byte, 1)); n != 0 || err != io.EOF {
			t.Errorf("peer read %d, %v after run; want the stream closed", n, err)
		}
	})
}

// Each side pings every pingInterval, and a stream that stays silent for
// silenceTimeout is lost; every message from the peer restarts that timer.
func TestControlHeartbeat(t *testing.T) {
	for _, tt := range []struct {
		name  string
		sends []time.Duration // when the peer sends a message
		end   time.Duration
	}{
		{"silent peer", nil, silenceTimeout},
		{"messages restart the timer", []time.Duration{3 * time.Second, 8 * time.Second}, 8*time.Second + silenceTimeout},
	} {
		t.Run(tt.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				conn, peer := net.Pipe()
				start := time.Now()
				pings := make(chan time.Duration, 32)
				go func() {
					defer close(pings)
					lines := bufio.NewReader(peer)
					for {
						line, err := lines.ReadString('\n')
						if err != nil {
							return
						}
						if line == `{"type":"ping"}`+"\n" {
							pings <- time.Since(start)
						}
					}
				}()
				go func() {
					for _, at := range tt.sends {
						time.Sleep(at - time.Since(start))
						peer.Write([]byte(`{"type":"ping"}` + "\n"))
					}
				}()

				err := newControl(conn).run(t.Context(), func(message) {})
				if !errors.Is(err, errSilent) || time.Since(start) != tt.end {
					t.Errorf("run = %v after %v, want %v after %v", err, time.Since(start), errSilent, tt.end)
				}
				peer.Close()
				var got, want []time.Duration
				for at := range pings {
					// A ping due at the very end may or may not go out.
					if at < tt.end {
						got = append(got, at)
					}
				}
				for at := pingInterval; at < tt.end; at += pingInterval {
					want = append(want, at)
				}
				if !slices.Equal(got, want) {
					t.Errorf("pings at %v, want %v", got, want)
				}
			})
		})
	}
}

// A message too long for the peer to accept is never sent, and fail messages
// trim their error text to fit.
func TestControlSendFitsMessages(t *testing.T) {
	conn, peer := net.Pipe()
	defer peer.Close()
	c := newControl(conn)
	long := errors.New(strings.Repeat("é", maxMessage))
	if err := c.send(message{Type: typeFail, Error: long.Error()}); !errors.Is(err, errLongMessage) {
		t.Fatalf("send of an oversized message = %v, want %v", err, errLongMessage)
	}
	m := failMessage(7, long)
	if len(m.Error) > maxErrorText || !utf8.ValidString(m.Error) || !strings.HasPrefix(long.Error(), m.Error) {
		t.Fatalf("failMessage kept %d bytes of error text, valid UTF-8 %t", len(m.Error), utf8.ValidString(m.Error))
	}
	go c.send(m)
	if got, err := newControl(peer).receive(); err != nil || got != m {
		t.Errorf("trimmed fail message arrived as %+v, %v", got, err)
	}
}

// A peer that stops reading is lost too: a write gives up after silenceTimeout.
func TestControlSendGivesUp(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conn, peer := net.Pipe()
		defer peer.Close()
		start := time.Now()
		if err := newControl(conn).send(message{Type: typePing}); !errors.Is(err, os.ErrDeadlineExceeded) || time.Since(start) != silenceTimeout {
			t.Errorf("send to a stalled peer = %v after %v, want a deadline error after %v", err, time.Since(start), silenceTimeout)
		}
	})
}

// Receivers ignore unknown fields, and pass unknown types on for the session
// to ignore, so later versions can add both.
func TestControlReceiveAcceptsUnknown(t *testing.T) {
	c := reading(t, `{"type":"open","id":7,"alias":"nas","port":22,"later":[1,{}]}`+"\n"+`{"type":"later","id":1}`+"\n")
	for _, want := range []message{
		{Type: typeOpen, ID: 7, Alias: "nas", Port: 22},
		{Type: "later", ID: 1},
	} {
		if got, err := c.receive(); err != nil || got != want {
			t.Errorf("receive = %+v, %v; want %+v", got, err, want)
		}
	}
}

func TestControlReceiveRejects(t *testing.T) {
	// A line of exactly maxMessage bytes, newline included, is accepted.
	padded := `{"type":"ping","pad":"` + strings.Repeat("x", maxMessage-len(`{"type":"ping","pad":""}`)-1) + `"}` + "\n"
	if got, err := reading(t, padded).receive(); err != nil || got.Type != typePing {
		t.Fatalf("receive of a %d-byte line = %+v, %v", len(padded), got, err)
	}
	for _, tt := range []struct {
		name, input string
		want        error
	}{
		{"empty", "", io.EOF},
		{"truncated", `{"type":"ping"}`, io.ErrUnexpectedEOF},
		{"too long", strings.Repeat(" ", maxMessage) + `{"type":"ping"}` + "\n", errLongMessage},
		{"not JSON", "ping\n", nil},
		{"not an object", "[]\n", nil},
	} {
		got, err := reading(t, tt.input).receive()
		switch {
		case err == nil:
			t.Errorf("%s: receive = %+v, want an error", tt.name, got)
		case tt.want != nil && !errors.Is(err, tt.want):
			t.Errorf("%s: receive error = %v, want %v", tt.name, err, tt.want)
		case tt.want == nil && !strings.Contains(err.Error(), "malformed control message"):
			t.Errorf("%s: receive error = %v, want a malformed message", tt.name, err)
		}
	}
}
