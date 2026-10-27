package netutil

import (
	"fmt"
	"net/netip"
	"strings"
	"unicode"
)

// Address is an immutable, comparable IP or domain name without a port, such
// as the host of a proxy request or of an egress alias. Domain names are kept
// verbatim, even when they look like IP literals, because protocol headers say
// which one they carry. The zero value is invalid. Use IPAddress,
// DomainAddress, or ParseAddress to construct one.
type Address struct {
	ip     netip.Addr
	domain string
}

// IPAddress constructs an address from a valid, unscoped IPv4 or IPv6 address.
// Header IP fields have no representation for an interface zone.
func IPAddress(ip netip.Addr) (Address, error) {
	if !ip.IsValid() || ip.Zone() != "" {
		return Address{}, fmt.Errorf("netutil: invalid unscoped IP address %q", ip)
	}
	return Address{ip: ip}, nil
}

// DomainAddress constructs a domain address with a 1..255-byte name.
// It does not resolve the name, parse it as an IP, or validate DNS syntax.
func DomainAddress(domain string) (Address, error) {
	if domain == "" || len(domain) > 255 {
		return Address{}, fmt.Errorf("netutil: domain length %d is outside 1..255 bytes", len(domain))
	}
	return Address{domain: domain}, nil
}

// ParseAddress parses hand-written text: an IP literal is an IP, and anything
// else a domain name. It rejects whitespace, colons, and slashes, which come
// from typos, ports, bracketed IPv6, or URLs.
func ParseAddress(text string) (Address, error) {
	if ip, err := netip.ParseAddr(text); err == nil {
		return IPAddress(ip)
	}
	if strings.ContainsFunc(text, unicode.IsSpace) {
		return Address{}, fmt.Errorf("netutil: address %q contains whitespace", text)
	}
	if strings.ContainsAny(text, ":/") {
		return Address{}, fmt.Errorf("netutil: address %q must be a bare host name or IP address, without port or URL syntax", text)
	}
	return DomainAddress(text)
}

// IsValid reports whether the address is an IP or a nonempty domain.
func (a Address) IsValid() bool { return a.ip.IsValid() || a.domain != "" }

// IP returns the IP, or an invalid netip.Addr for a domain or zero address.
func (a Address) IP() netip.Addr { return a.ip }

// Domain returns the domain name, or an empty string for an IP or zero address.
func (a Address) Domain() string { return a.domain }

// String returns the host text without a port or IPv6 brackets.
func (a Address) String() string {
	if a.ip.IsValid() {
		return a.ip.String()
	}
	return a.domain
}

// MarshalText returns String. Like any text, it does not keep the category of a
// domain name that looks like an IP literal.
func (a Address) MarshalText() ([]byte, error) {
	return []byte(a.String()), nil
}

// UnmarshalText parses text with ParseAddress.
func (a *Address) UnmarshalText(text []byte) error {
	address, err := ParseAddress(string(text))
	if err != nil {
		return err
	}
	*a = address
	return nil
}
