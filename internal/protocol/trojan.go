// Package protocol implements proxy protocol wire formats: byte-level encoding
// and authentication, without networking. Each protocol has its own concrete
// API; nothing here abstracts over protocols.
package protocol

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/netip"

	"github.com/Qusic/miniray/internal/netutil"
)

// TrojanCommand is the kind of Trojan request.
type TrojanCommand byte

const (
	TrojanConnect      TrojanCommand = 0x01
	TrojanUDPAssociate TrojanCommand = 0x03
)

const (
	addrIPv4   = 0x01
	addrDomain = 0x03
	addrIPv6   = 0x04
)

// TrojanRequest is the header a client sends before its payload.
type TrojanRequest struct {
	Command TrojanCommand
	Host    netutil.Address
	Port    uint16
}

// Trojan encodes and authenticates requests using a precomputed password hash.
// A request header is followed directly by its payload:
//
//	hex(SHA-224(password)) CRLF command address-type address port CRLF payload...
//
// The address uses SOCKS5 encoding and the port is big-endian.
type Trojan struct {
	key [sha256.Size224 * 2]byte
}

// NewTrojan returns a codec for password.
func NewTrojan(password string) Trojan {
	sum := sha256.Sum224([]byte(password))
	var p Trojan
	hex.Encode(p.key[:], sum[:])
	return p
}

var (
	errTrojanAuth      = errors.New("trojan: wrong password")
	errTrojanMalformed = errors.New("trojan: malformed request")
)

// ReadRequest reads and authenticates one request header, leaving the payload
// in r. Trojan servers send no reply. The caller decides which commands it can
// execute.
func (p Trojan) ReadRequest(r io.Reader) (TrojanRequest, error) {
	var got [sha256.Size224 * 2]byte
	if _, err := io.ReadFull(r, got[:]); err != nil {
		return TrojanRequest{}, err
	}
	if subtle.ConstantTimeCompare(got[:], p.key[:]) != 1 {
		return TrojanRequest{}, errTrojanAuth
	}

	// CRLF, command, address type
	var b [4]byte
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return TrojanRequest{}, err
	}
	if b[0] != '\r' || b[1] != '\n' {
		return TrojanRequest{}, errTrojanMalformed
	}
	command := TrojanCommand(b[2])
	host, err := readHost(r, b[3])
	if err != nil {
		return TrojanRequest{}, err
	}

	// port, CRLF
	if _, err := io.ReadFull(r, b[:]); err != nil {
		return TrojanRequest{}, err
	}
	if b[2] != '\r' || b[3] != '\n' {
		return TrojanRequest{}, errTrojanMalformed
	}
	return TrojanRequest{
		Command: command,
		Host:    host,
		Port:    binary.BigEndian.Uint16(b[:2]),
	}, nil
}

func readHost(r io.Reader, addrType byte) (netutil.Address, error) {
	switch addrType {
	case addrIPv4:
		var ip [4]byte
		if _, err := io.ReadFull(r, ip[:]); err != nil {
			return netutil.Address{}, err
		}
		return netutil.IPAddress(netip.AddrFrom4(ip))
	case addrIPv6:
		var ip [16]byte
		if _, err := io.ReadFull(r, ip[:]); err != nil {
			return netutil.Address{}, err
		}
		return netutil.IPAddress(netip.AddrFrom16(ip))
	case addrDomain:
		var n [1]byte
		if _, err := io.ReadFull(r, n[:]); err != nil {
			return netutil.Address{}, err
		}
		// An empty host would make the dialer connect to this machine.
		if n[0] == 0 {
			return netutil.Address{}, errTrojanMalformed
		}
		name := make([]byte, n[0])
		if _, err := io.ReadFull(r, name); err != nil {
			return netutil.Address{}, err
		}
		return netutil.DomainAddress(string(name))
	default:
		return netutil.Address{}, fmt.Errorf("%w: unknown address type %#x", errTrojanMalformed, addrType)
	}
}

// WriteRequest writes the header for req.
func (p Trojan) WriteRequest(w io.Writer, req TrojanRequest) error {
	if !req.Host.IsValid() {
		return errors.New("trojan: missing host")
	}

	ip, domain := req.Host.IP(), req.Host.Domain()
	b := append([]byte(nil), p.key[:]...)
	b = append(b, '\r', '\n', byte(req.Command))
	if ip.IsValid() {
		if ip.Is4() {
			b = append(b, addrIPv4)
		} else {
			b = append(b, addrIPv6)
		}
		b = append(b, ip.AsSlice()...)
	} else {
		b = append(b, addrDomain, byte(len(domain)))
		b = append(b, domain...)
	}
	b = binary.BigEndian.AppendUint16(b, req.Port)
	b = append(b, '\r', '\n')

	_, err := w.Write(b)
	return err
}
