package netutil

import (
	"net/netip"
	"strings"
	"testing"
)

func mustIP(t testing.TB, text string) Address {
	t.Helper()
	address, err := IPAddress(netip.MustParseAddr(text))
	if err != nil {
		t.Fatal(err)
	}
	return address
}

func mustDomain(t testing.TB, text string) Address {
	t.Helper()
	address, err := DomainAddress(text)
	if err != nil {
		t.Fatal(err)
	}
	return address
}

func TestIPAddress(t *testing.T) {
	for _, text := range []string{"127.0.0.1", "2001:db8::1", "::ffff:192.0.2.1", "0.0.0.0", "::"} {
		t.Run(text, func(t *testing.T) {
			ip := netip.MustParseAddr(text)
			address, err := IPAddress(ip)
			if err != nil {
				t.Fatal(err)
			}
			if !address.IsValid() {
				t.Error("IsValid() = false, want true")
			}
			if got := address.IP(); got != ip {
				t.Errorf("IP() = %v, want %v", got, ip)
			}
			if got := address.Domain(); got != "" {
				t.Errorf("Domain() = %q, want empty", got)
			}
			if got := address.String(); got != ip.String() {
				t.Errorf("String() = %q, want %q", got, ip.String())
			}
		})
	}
}

func TestDomainAddress(t *testing.T) {
	for _, text := range []string{"example.com", "Host.Example.COM", "127.0.0.1", "2001:db8::1", strings.Repeat("a", 255), strings.Repeat("界", 85)} {
		t.Run(text, func(t *testing.T) {
			address, err := DomainAddress(text)
			if err != nil {
				t.Fatal(err)
			}
			if !address.IsValid() {
				t.Error("IsValid() = false, want true")
			}
			if got := address.IP(); got.IsValid() {
				t.Errorf("IP() = %v, want invalid IP", got)
			}
			if got := address.Domain(); got != text {
				t.Errorf("Domain() = %q, want %q", got, text)
			}
			if got := address.String(); got != text {
				t.Errorf("String() = %q, want %q", got, text)
			}
		})
	}
	// Identical host text must not erase the address category.
	if mustIP(t, "127.0.0.1") == mustDomain(t, "127.0.0.1") {
		t.Fatal("IP and numeric domain are indistinguishable")
	}
}

func TestAddressRejectsInvalidInput(t *testing.T) {
	for _, ip := range []netip.Addr{{}, netip.MustParseAddr("fe80::1%eth0")} {
		if _, err := IPAddress(ip); err == nil {
			t.Errorf("IPAddress(%q) accepted an invalid or scoped IP", ip)
		}
	}
	for _, text := range []string{"", strings.Repeat("a", 256), strings.Repeat("界", 86)} {
		if _, err := DomainAddress(text); err == nil {
			t.Errorf("DomainAddress(%q) accepted a name outside 1..255 bytes", text)
		}
	}
}

func TestParseAddress(t *testing.T) {
	// Text has no address type, so IP literals become IPs.
	for text, want := range map[string]Address{
		"192.0.2.1":     mustIP(t, "192.0.2.1"),
		"2001:db8::1":   mustIP(t, "2001:db8::1"),
		"nas.home.arpa": mustDomain(t, "nas.home.arpa"),
	} {
		if got, err := ParseAddress(text); err != nil || got != want {
			t.Errorf("ParseAddress(%q) = %v, %v; want %v", text, got, err, want)
		}
	}
	// Common slips when writing a host: a port, URL-style brackets, or a URL.
	for _, text := range []string{"", " localhost", "local host", "fe80::1%eth0",
		"192.0.2.10:22", "host.example.com:22", "[2001:db8::1]", "http://example.com", "example.com/"} {
		if _, err := ParseAddress(text); err == nil {
			t.Errorf("ParseAddress(%q) accepted invalid text", text)
		}
	}
}
