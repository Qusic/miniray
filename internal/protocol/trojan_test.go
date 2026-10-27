package protocol

import (
	"bytes"
	"errors"
	"io"
	"net/netip"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/Qusic/miniray/internal/netutil"
)

func mustIP(t testing.TB, text string) netutil.Address {
	t.Helper()
	address, err := netutil.IPAddress(netip.MustParseAddr(text))
	if err != nil {
		t.Fatal(err)
	}
	return address
}

func mustDomain(t testing.TB, text string) netutil.Address {
	t.Helper()
	address, err := netutil.DomainAddress(text)
	if err != nil {
		t.Fatal(err)
	}
	return address
}

// Lowercase hex SHA-224("secret"), checked independently of the codec.
const testTrojanKey = "95c7fbca92ac5083afda62a564a3d014fc3b72c9140e3cb99ea6bf12"

func TestTrojanRequest(t *testing.T) {
	p := NewTrojan("secret")
	// Bytes after the key, written out independently of the encoder.
	tests := []struct {
		name string
		host netutil.Address
		port uint16
		wire string
	}{
		{"ipv4", mustIP(t, "127.0.0.1"), 22, "\r\n\x01\x01\x7f\x00\x00\x01\x00\x16\r\n"},
		{"ipv6", mustIP(t, "2001:db8::1"), 443, "\r\n\x01\x04\x20\x01\x0d\xb8" + strings.Repeat("\x00", 11) + "\x01\x01\xbb\r\n"},
		{"mapped ipv6", mustIP(t, "::ffff:192.0.2.1"), 443, "\r\n\x01\x04" + strings.Repeat("\x00", 10) + "\xff\xff\xc0\x00\x02\x01\x01\xbb\r\n"},
		{"domain", mustDomain(t, "host.example.com"), 1024, "\r\n\x01\x03\x10host.example.com\x04\x00\r\n"},
		{"ipv4-shaped domain", mustDomain(t, "127.0.0.1"), 22, "\r\n\x01\x03\x09127.0.0.1\x00\x16\r\n"},
		{"ipv6-shaped domain", mustDomain(t, "2001:db8::1"), 443, "\r\n\x01\x03\x0b2001:db8::1\x01\xbb\r\n"},
		{"zero port", mustDomain(t, "example.com"), 0, "\r\n\x01\x03\x0bexample.com\x00\x00\r\n"},
		{"max port", mustDomain(t, "example.com"), 65535, "\r\n\x01\x03\x0bexample.com\xff\xff\r\n"},
		{"max domain", mustDomain(t, strings.Repeat("a", 255)), 443, "\r\n\x01\x03\xff" + strings.Repeat("a", 255) + "\x01\xbb\r\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := TrojanRequest{Command: TrojanConnect, Host: tt.host, Port: tt.port}
			wire := testTrojanKey + tt.wire

			var buf bytes.Buffer
			if err := p.WriteRequest(&buf, req); err != nil {
				t.Fatal(err)
			}
			if buf.String() != wire {
				t.Errorf("WriteRequest = %q, want %q", buf.String(), wire)
			}

			// Exercise both coalesced input and one-byte reads without losing payload.
			for _, oneByte := range []bool{false, true} {
				r := strings.NewReader(wire + "payload")
				var reader io.Reader = r
				if oneByte {
					reader = iotest.OneByteReader(r)
				}
				got, err := p.ReadRequest(reader)
				if err != nil || got != req {
					t.Errorf("ReadRequest(oneByte=%t) = %+v, %v; want %+v", oneByte, got, err, req)
				}
				if rest, err := io.ReadAll(r); err != nil || string(rest) != "payload" {
					t.Errorf("ReadRequest(oneByte=%t) left %q, %v; want payload", oneByte, rest, err)
				}
			}
		})
	}
}

func TestTrojanReadRequestKeepsCommands(t *testing.T) {
	p := NewTrojan("secret")
	for _, command := range []TrojanCommand{TrojanConnect, TrojanUDPAssociate, 0xff} {
		var wire bytes.Buffer
		want := TrojanRequest{Command: command, Host: mustDomain(t, "example.com"), Port: 443}
		if err := p.WriteRequest(&wire, want); err != nil {
			t.Fatal(err)
		}
		wire.WriteString("payload")
		if got, err := p.ReadRequest(&wire); err != nil || got != want {
			t.Errorf("ReadRequest = %+v, %v; want %+v", got, err, want)
		}
		if wire.String() != "payload" {
			t.Errorf("ReadRequest consumed payload for command %#x", command)
		}
	}
}

func TestTrojanReadRejects(t *testing.T) {
	p := NewTrojan("secret")
	tests := []struct {
		name string
		wire string
		want error
	}{
		{"wrong password", strings.Repeat("0", len(testTrojanKey)) + "\r\n\x01\x01\x7f\x00\x00\x01\x00\x16\r\n", errTrojanAuth},
		{"no CRLF after key", testTrojanKey + "\n\r\x01\x01\x7f\x00\x00\x01\x00\x16\r\n", errTrojanMalformed},
		{"unknown address type", testTrojanKey + "\r\n\x01\x02\x7f\x00\x00\x01\x00\x16\r\n", errTrojanMalformed},
		{"empty domain", testTrojanKey + "\r\n\x01\x03\x00\x00\x16\r\n", errTrojanMalformed},
		{"no CRLF after port", testTrojanKey + "\r\n\x01\x01\x7f\x00\x00\x01\x00\x16\n\r", errTrojanMalformed},
		{"empty", "", io.EOF},
	}
	for _, tt := range tests {
		if _, err := p.ReadRequest(strings.NewReader(tt.wire)); !errors.Is(err, tt.want) {
			t.Errorf("%s: err = %v, want %v", tt.name, err, tt.want)
		}
	}
}

func TestTrojanReadTruncated(t *testing.T) {
	p := NewTrojan("secret")
	for _, req := range []TrojanRequest{
		{Command: TrojanConnect, Host: mustIP(t, "127.0.0.1"), Port: 22},
		{Command: TrojanConnect, Host: mustIP(t, "2001:db8::1"), Port: 443},
		{Command: TrojanConnect, Host: mustDomain(t, "host.example.com"), Port: 1024},
	} {
		var wire bytes.Buffer
		if err := p.WriteRequest(&wire, req); err != nil {
			t.Fatal(err)
		}
		for n := 0; n < wire.Len(); n++ {
			if _, err := p.ReadRequest(bytes.NewReader(wire.Bytes()[:n])); !errors.Is(err, io.EOF) && !errors.Is(err, io.ErrUnexpectedEOF) {
				t.Fatalf("truncated %+v request at byte %d: %v, want EOF", req, n, err)
			}
		}
	}
}

func FuzzTrojanReadRequest(f *testing.F) {
	p := NewTrojan("secret")
	for _, req := range []TrojanRequest{
		{Command: TrojanConnect, Host: mustIP(f, "127.0.0.1"), Port: 22},
		{Command: TrojanConnect, Host: mustIP(f, "2001:db8::1"), Port: 443},
		{Command: TrojanConnect, Host: mustDomain(f, "host.example.com"), Port: 1024},
		{Command: TrojanConnect, Host: mustDomain(f, "127.0.0.1"), Port: 80},
	} {
		var wire bytes.Buffer
		if err := p.WriteRequest(&wire, req); err != nil {
			f.Fatal(err)
		}
		f.Add(wire.Bytes()[len(testTrojanKey):])
	}
	f.Fuzz(func(t *testing.T, body []byte) {
		// Preserve authentication so mutations exercise the entire parser.
		wire := append(append([]byte(nil), testTrojanKey...), body...)
		r := bytes.NewReader(wire)
		req, err := p.ReadRequest(r)
		consumed := len(wire) - r.Len()
		if consumed > len(testTrojanKey)+2+1+1+1+255+2+2 {
			t.Fatalf("header consumed %d bytes", consumed)
		}
		if err != nil {
			return
		}
		var encoded bytes.Buffer
		if err := p.WriteRequest(&encoded, req); err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(encoded.Bytes(), wire[:consumed]) {
			t.Fatal("read/write changed the request's wire representation")
		}
	})
}

func TestTrojanWriteRejectsZeroAddress(t *testing.T) {
	var wire bytes.Buffer
	p := NewTrojan("secret")
	if err := p.WriteRequest(&wire, TrojanRequest{Command: TrojanConnect, Port: 80}); err == nil || wire.Len() != 0 {
		t.Fatalf("zero host: wrote %d bytes, err = %v", wire.Len(), err)
	}
}
