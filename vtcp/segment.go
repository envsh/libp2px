package vtcp

import (
	"encoding/binary"
	"errors"
)

// Segment represents a parsed TCP segment (header + payload).
type Segment struct {
	SrcPort  uint16
	DstPort  uint16
	Seq      uint32
	Ack      uint32
	Flags    uint8
	Window   uint16
	Checksum uint16
	Urgent   uint16
	Options  []Option
	Payload  []byte
}

// ParseSegment parses a raw TCP segment (no IP header).
func ParseSegment(raw []byte) (Segment, error) {
	if len(raw) < 20 {
		return Segment{}, errors.New("segment too short")
	}
	var s Segment
	s.SrcPort = binary.BigEndian.Uint16(raw[0:2])
	s.DstPort = binary.BigEndian.Uint16(raw[2:4])
	s.Seq = binary.BigEndian.Uint32(raw[4:8])
	s.Ack = binary.BigEndian.Uint32(raw[8:12])
	dataOff := int(raw[12]>>4) * 4
	if dataOff < 20 || dataOff > len(raw) {
		return Segment{}, errors.New("invalid data offset")
	}
	s.Flags = raw[13]
	s.Window = binary.BigEndian.Uint16(raw[14:16])
	s.Checksum = binary.BigEndian.Uint16(raw[16:18])
	s.Urgent = binary.BigEndian.Uint16(raw[18:20])

	if dataOff > 20 {
		s.Options = ParseOptions(raw[20:dataOff])
	}
	if dataOff < len(raw) {
		s.Payload = raw[dataOff:]
	}
	return s, nil
}

// Marshal serializes a Segment to wire format.
// The checksum field is set to zero — the caller must compute and fill it
// using IP pseudo-header information.
func (s *Segment) Marshal() []byte {
	hdr := s.marshalHeader()
	if len(s.Payload) == 0 {
		return hdr
	}
	pkt := make([]byte, len(hdr)+len(s.Payload))
	copy(pkt, hdr)
	copy(pkt[len(hdr):], s.Payload)
	return pkt
}

// marshalHeader builds the TCP header bytes (with options, padded).
func (s *Segment) marshalHeader() []byte {
	var optBytes []byte
	if len(s.Options) > 0 {
		optBytes = BuildOptions(s.Options)
	}
	hdrLen := 20 + len(optBytes)
	hdr := make([]byte, hdrLen)

	binary.BigEndian.PutUint16(hdr[0:2], s.SrcPort)
	binary.BigEndian.PutUint16(hdr[2:4], s.DstPort)
	binary.BigEndian.PutUint32(hdr[4:8], s.Seq)
	binary.BigEndian.PutUint32(hdr[8:12], s.Ack)
	hdr[12] = byte(hdrLen/4) << 4
	hdr[13] = s.Flags
	binary.BigEndian.PutUint16(hdr[14:16], s.Window)
	// checksum at [16:18] left zero
	binary.BigEndian.PutUint16(hdr[18:20], s.Urgent)

	if len(optBytes) > 0 {
		copy(hdr[20:], optBytes)
	}
	return hdr
}

// DataLen returns the payload length of the segment.
func (s *Segment) DataLen() uint32 {
	return uint32(len(s.Payload))
}

// SegLen returns the "segment length" consumed in sequence space:
// payload length + 1 for SYN + 1 for FIN.
func (s *Segment) SegLen() uint32 {
	n := uint32(len(s.Payload))
	if s.Flags&FlagSYN != 0 {
		n++
	}
	if s.Flags&FlagFIN != 0 {
		n++
	}
	return n
}

// HasFlag reports whether the segment has the given flag set.
func (s *Segment) HasFlag(flag uint8) bool {
	return s.Flags&flag != 0
}
