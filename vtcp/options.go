package vtcp

import "encoding/binary"

// TCP option kinds.
const (
	OptEnd       byte = 0
	OptNOP       byte = 1
	OptMSS       byte = 2
	OptWScale    byte = 3
	OptSACKPerm  byte = 4
	OptSACK      byte = 5
	OptTimestamp byte = 8
)

// Option represents a parsed TCP option.
type Option struct {
	Kind byte
	Data []byte // raw data (excluding kind and length bytes)
}

// SACKBlock represents a SACK block (left edge inclusive, right edge exclusive).
type SACKBlock struct {
	Left  uint32
	Right uint32
}

// ParseOptions parses the options portion of a TCP header.
func ParseOptions(raw []byte) []Option {
	var opts []Option
	for i := 0; i < len(raw); {
		kind := raw[i]
		if kind == OptEnd {
			break
		}
		if kind == OptNOP {
			opts = append(opts, Option{Kind: OptNOP})
			i++
			continue
		}
		if i+1 >= len(raw) {
			break
		}
		l := int(raw[i+1])
		if l < 2 || i+l > len(raw) {
			break
		}
		var data []byte
		if l > 2 {
			data = make([]byte, l-2)
			copy(data, raw[i+2:i+l])
		}
		opts = append(opts, Option{Kind: kind, Data: data})
		i += l
	}
	return opts
}

// BuildOptions serializes options into wire format, NOP-padded to 4-byte boundary.
func BuildOptions(opts []Option) []byte {
	var buf []byte
	for _, o := range opts {
		if o.Kind == OptNOP {
			buf = append(buf, OptNOP)
			continue
		}
		buf = append(buf, o.Kind, byte(2+len(o.Data)))
		buf = append(buf, o.Data...)
	}
	// Pad to 4-byte boundary with zero (RFC 793: padding after End-of-Option is zero)
	for len(buf)%4 != 0 {
		buf = append(buf, OptEnd)
	}
	return buf
}

// MSSOption creates an MSS option.
func MSSOption(mss uint16) Option {
	data := make([]byte, 2)
	binary.BigEndian.PutUint16(data, mss)
	return Option{Kind: OptMSS, Data: data}
}

// WScaleOption creates a window scale option.
func WScaleOption(shift uint8) Option {
	return Option{Kind: OptWScale, Data: []byte{shift}}
}

// SACKPermOption creates a SACK-Permitted option.
func SACKPermOption() Option {
	return Option{Kind: OptSACKPerm}
}

// SACKOption creates a SACK option with the given blocks.
func SACKOption(blocks []SACKBlock) Option {
	data := make([]byte, 8*len(blocks))
	for i, b := range blocks {
		binary.BigEndian.PutUint32(data[i*8:], b.Left)
		binary.BigEndian.PutUint32(data[i*8+4:], b.Right)
	}
	return Option{Kind: OptSACK, Data: data}
}

// TimestampOption creates a timestamps option.
func TimestampOption(tsVal, tsEcr uint32) Option {
	data := make([]byte, 8)
	binary.BigEndian.PutUint32(data[0:4], tsVal)
	binary.BigEndian.PutUint32(data[4:8], tsEcr)
	return Option{Kind: OptTimestamp, Data: data}
}

// GetMSS extracts the MSS value from options, returning 0 if not present.
func GetMSS(opts []Option) uint16 {
	for _, o := range opts {
		if o.Kind == OptMSS && len(o.Data) == 2 {
			return binary.BigEndian.Uint16(o.Data)
		}
	}
	return 0
}

// GetWScale extracts the window scale shift from options, returning -1 if not present.
func GetWScale(opts []Option) int {
	for _, o := range opts {
		if o.Kind == OptWScale && len(o.Data) == 1 {
			return int(o.Data[0])
		}
	}
	return -1
}

// GetTimestamp extracts TSval and TSecr from options.
// Returns ok=false if not present.
func GetTimestamp(opts []Option) (tsVal, tsEcr uint32, ok bool) {
	for _, o := range opts {
		if o.Kind == OptTimestamp && len(o.Data) == 8 {
			return binary.BigEndian.Uint32(o.Data[0:4]), binary.BigEndian.Uint32(o.Data[4:8]), true
		}
	}
	return 0, 0, false
}

// GetSACKBlocks extracts SACK blocks from options.
func GetSACKBlocks(opts []Option) []SACKBlock {
	for _, o := range opts {
		if o.Kind == OptSACK && len(o.Data) >= 8 {
			n := len(o.Data) / 8
			blocks := make([]SACKBlock, n)
			for i := range n {
				blocks[i].Left = binary.BigEndian.Uint32(o.Data[i*8:])
				blocks[i].Right = binary.BigEndian.Uint32(o.Data[i*8+4:])
			}
			return blocks
		}
	}
	return nil
}

// HasSACKPerm checks if SACK-Permitted is in the options.
func HasSACKPerm(opts []Option) bool {
	for _, o := range opts {
		if o.Kind == OptSACKPerm {
			return true
		}
	}
	return false
}
