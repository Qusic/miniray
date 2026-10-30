package tunnel

import (
	"context"
	"errors"
	"io"
	"net"
	"runtime"
	"slices"
	"strings"
	"testing"
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
	bubble(t, func(t *testing.T) {
		for _, tt := range []struct {
			line string
			m    message
		}{
			{`{"type":"ready"}`, message{Type: typeReady}},
			{`{"type":"open","id":7,"alias":"nas","port":22}`, message{Type: typeOpen, ID: 7, Alias: "nas", Port: 22}},
			{`{"type":"fail","id":7,"error":"connection refused"}`, message{Type: typeFail, ID: 7, Error: "connection refused"}},
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
	})
}

// run passes messages on in order until ctx ends, and none after, even one it
// has already read. It then returns the cause of ctx and closes the stream.
func TestControlRun(t *testing.T) {
	lines := `{"type":"open","id":1,"alias":"nas","port":22}` + "\n" + `{"type":"later"}` + "\n" + `{"type":"ready"}` + "\n"
	sent := []message{{Type: typeOpen, ID: 1, Alias: "nas", Port: 22}, {Type: "later"}, {Type: typeReady}}
	for _, tt := range []struct {
		name string
		end  int // how many messages are handled before ctx ends
	}{
		{"while waiting", 3},
		{"with a message read", 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			bubble(t, func(t *testing.T) {
				conn, peer := net.Pipe()
				ctx, cancel := context.WithCancelCause(t.Context())
				stop := errors.New("stop")
				var got []message
				done := make(chan error)
				go func() {
					done <- newControl(conn).run(ctx, func(m message) {
						got = append(got, m)
						if len(got) == tt.end {
							cancel(stop)
						}
					})
				}()
				// One write, so that run has read every line when ctx ends early.
				peer.Write([]byte(lines))
				if err := <-done; err != stop {
					t.Errorf("run = %v, want the cause %v", err, stop)
				}
				if want := sent[:tt.end]; !slices.Equal(got, want) {
					t.Errorf("handled %+v, want %+v", got, want)
				}
				if n, err := peer.Read(make([]byte, 1)); n != 0 || err != io.EOF {
					t.Errorf("peer read %d, %v after run; want the stream closed", n, err)
				}
			})
		})
	}
}

// A stream that fails ends run with the error, and run closes it.
func TestControlRunFails(t *testing.T) {
	bubble(t, func(t *testing.T) {
		conn, peer := net.Pipe()
		go peer.Write([]byte("open\n"))
		err := newControl(conn).run(t.Context(), func(m message) {
			t.Errorf("handled %+v from a malformed line", m)
		})
		if err == nil || !strings.Contains(err.Error(), "malformed control message") {
			t.Errorf("run = %v, want a malformed message", err)
		}
		if n, err := peer.Read(make([]byte, 1)); n != 0 || err != io.EOF {
			t.Errorf("peer read %d, %v after run; want the stream closed", n, err)
		}
	})
}

// The longest line the peer accepts is sent, one byte more is not, and fail
// messages trim their error text to fit.
func TestControlSendFitsMessages(t *testing.T) {
	bubble(t, func(t *testing.T) {
		conn, peer := net.Pipe()
		defer peer.Close()
		c, p := newControl(conn), newControl(peer)
		longest := message{Type: typeFail, Error: strings.Repeat("x", maxMessage-len(`{"type":"fail","error":""}`+"\n"))}
		received := make(chan message, 1)
		go func() {
			m, _ := p.receive()
			received <- m
		}()
		if err := c.send(longest); err != nil {
			t.Fatalf("send of a %d-byte line = %v", maxMessage, err)
		}
		if got := <-received; got != longest {
			t.Fatalf("a %d-byte line arrived with %d bytes of error text", maxMessage, len(got.Error))
		}
		longest.Error += "x"
		if err := c.send(longest); !errors.Is(err, errLongMessage) {
			t.Fatalf("send of a line one byte longer = %v, want %v", err, errLongMessage)
		}

		// The cut falls inside a character, whose remains are dropped.
		long := errors.New("x" + strings.Repeat("é", maxMessage))
		m := failMessage(7, long)
		if want := long.Error()[:maxErrorText-1]; m.Error != want {
			t.Fatalf("failMessage kept %d bytes of error text, want the first %d", len(m.Error), len(want))
		}
		go c.send(m)
		if got, err := p.receive(); err != nil || got != m {
			t.Errorf("trimmed fail message arrived as %+v, %v", got, err)
		}
	})
}

// byteConn writes a byte at a time, yielding after each, so that writes from
// different goroutines interleave unless something serializes them. It does
// not sleep instead, since fake time stops while a goroutine waits for a mutex.
type byteConn struct{ net.Conn }

func (c byteConn) Write(p []byte) (int, error) {
	for i := range p {
		if _, err := c.Conn.Write(p[i : i+1]); err != nil {
			return i, err
		}
		runtime.Gosched()
	}
	return len(p), nil
}

// Sends from different goroutines arrive whole, even on a connection that
// lets concurrent writes interleave.
func TestControlSendsArriveWhole(t *testing.T) {
	bubble(t, func(t *testing.T) {
		conn, peer := net.Pipe()
		defer peer.Close()
		c, p := newControl(byteConn{conn}), newControl(peer)
		const n = 3
		sent := make(map[message]bool)
		for id := range uint16(n) {
			m := message{Type: typeOpen, ID: id + 1, Alias: "nas", Port: 22}
			sent[m] = true
			go c.send(m)
		}
		for range n {
			m, err := p.receive()
			if err != nil || !sent[m] {
				t.Fatalf("received %+v, %v; want each message sent, once", m, err)
			}
			delete(sent, m)
		}
	})
}

// Receivers ignore unknown fields, and pass unknown types on for the session
// to ignore, so later versions can add both.
func TestControlReceiveAcceptsUnknown(t *testing.T) {
	bubble(t, func(t *testing.T) {
		c := reading(t, `{"type":"open","id":7,"alias":"nas","port":22,"later":[1,{}]}`+"\n"+`{"type":"later","id":1}`+"\n")
		for _, want := range []message{
			{Type: typeOpen, ID: 7, Alias: "nas", Port: 22},
			{Type: "later", ID: 1},
		} {
			if got, err := c.receive(); err != nil || got != want {
				t.Errorf("receive = %+v, %v; want %+v", got, err, want)
			}
		}
	})
}

func TestControlReceiveRejects(t *testing.T) {
	bubble(t, func(t *testing.T) {
		// A line of exactly maxMessage bytes, newline included, is accepted.
		padded := `{"type":"open","pad":"` + strings.Repeat("x", maxMessage-len(`{"type":"open","pad":""}`)-1) + `"}` + "\n"
		if got, err := reading(t, padded).receive(); err != nil || got.Type != typeOpen {
			t.Fatalf("receive of a %d-byte line = %+v, %v", len(padded), got, err)
		}
		for _, tt := range []struct {
			name, input string
			want        error
		}{
			{"empty", "", io.EOF},
			{"truncated", `{"type":"open"}`, io.ErrUnexpectedEOF},
			{"too long", strings.Repeat(" ", maxMessage) + `{"type":"open"}` + "\n", errLongMessage},
			{"not JSON", "open\n", nil},
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
	})
}
