// Package vtcp implements a full, RFC-compliant TCP protocol engine operating
// on raw TCP segments. It is IP-agnostic and Ethernet-agnostic — callers
// provide a SegmentWriter callback for sending and feed incoming segments
// via HandleSegment.
//
// Supported RFCs:
//   - RFC 793:  TCP state machine
//   - RFC 6298: RTO calculation (SRTT, RTTVAR, Karn's algorithm)
//   - RFC 5681: Congestion control (slow start, congestion avoidance, fast retransmit/recovery)
//   - RFC 7323: Window scaling, timestamps (RTTM, PAWS)
//   - RFC 2018: Selective acknowledgment (SACK)
//   - RFC 1122: Keepalive
package vtcp

import (
	"crypto/rand"
	"encoding/binary"
	"time"
)

// State represents a TCP connection state per RFC 793.
type State uint8

const (
	StateClosed      State = iota
	StateListen            // passive open, waiting for SYN
	StateSynSent           // active open, SYN sent
	StateSynReceived       // SYN received, SYN-ACK sent
	StateEstablished       // data transfer
	StateFinWait1          // FIN sent, waiting for ACK or FIN
	StateFinWait2          // our FIN ACKed, waiting for remote FIN
	StateCloseWait         // remote FIN received, app hasn't closed yet
	StateClosing           // simultaneous close (both FINs sent, waiting for ACK)
	StateLastAck           // close from CloseWait, FIN sent, waiting for ACK
	StateTimeWait          // both FINs ACKed, waiting before final close
)

func (s State) String() string {
	switch s {
	case StateClosed:
		return "CLOSED"
	case StateListen:
		return "LISTEN"
	case StateSynSent:
		return "SYN-SENT"
	case StateSynReceived:
		return "SYN-RECEIVED"
	case StateEstablished:
		return "ESTABLISHED"
	case StateFinWait1:
		return "FIN-WAIT-1"
	case StateFinWait2:
		return "FIN-WAIT-2"
	case StateCloseWait:
		return "CLOSE-WAIT"
	case StateClosing:
		return "CLOSING"
	case StateLastAck:
		return "LAST-ACK"
	case StateTimeWait:
		return "TIME-WAIT"
	default:
		return "UNKNOWN"
	}
}

// TCP header flag bits.
const (
	FlagFIN = 0x01
	FlagSYN = 0x02
	FlagRST = 0x04
	FlagPSH = 0x08
	FlagACK = 0x10
	FlagURG = 0x20
)

// Default values.
const (
	DefaultMSS        = 1460
	DefaultWindowSize = 65535
	DefaultSendBuf    = 1 << 20 // 1 MB
	DefaultRecvBuf    = 1 << 20 // 1 MB

	DefaultRTO = time.Second
	MinRTO     = 200 * time.Millisecond
	MaxRTO     = 60 * time.Second
	MaxRetries = 8

	TimeWaitDuration = 2 * time.Second // shortened for virtual environments

	DefaultKeepaliveIdle     = 300 * time.Second
	DefaultKeepaliveInterval = 15 * time.Second
	DefaultKeepaliveCount    = 3
)

// SegmentWriter is the callback for sending outgoing TCP segments.
// The data is a raw TCP segment (header + payload, no IP header).
// The caller is responsible for wrapping it in an IP packet and Ethernet frame
// and computing the TCP checksum (which requires the IP pseudo-header).
// The checksum field in the segment is left at zero.
type SegmentWriter func(seg []byte) error

// RandUint32Exported returns a cryptographically random uint32.
func RandUint32Exported() uint32 { return randUint32() }

// randUint32 returns a cryptographically random uint32.
func randUint32() uint32 {
	var b [4]byte
	if _, err := rand.Read(b[:]); err != nil {
		return uint32(time.Now().UnixNano())
	}
	return binary.BigEndian.Uint32(b[:])
}
