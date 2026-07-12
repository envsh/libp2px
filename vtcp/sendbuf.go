package vtcp

// SendBuf tracks application data through the TCP send pipeline:
//
//	[acknowledged] [sent but unacked] [unsent / queued] [free space]
//	               ^                  ^                 ^
//	               una                nxt               tail
//
// The buffer is a simple slice-based implementation. Data is appended
// at the tail and removed from the front as it is acknowledged.
type SendBuf struct {
	buf []byte // all unacknowledged + unsent data
	cap int    // max buffer capacity

	una uint32 // SND.UNA: first unacked sequence
	nxt uint32 // SND.NXT: next sequence to send

	// SACK scoreboard: tracks sequence ranges the receiver has reported
	// as received out of order (RFC 2018). Used to avoid retransmitting
	// data the receiver already has.
	sacked []SACKBlock

	// buf[0] corresponds to sequence 'una'.
	// buf[0 .. nxt-una) is sent but unacked.
	// buf[nxt-una .. len(buf)) is unsent.
}

// NewSendBuf creates a send buffer with the given capacity and initial sequence number.
func NewSendBuf(capacity int, initialSeq uint32) *SendBuf {
	return &SendBuf{
		cap: capacity,
		una: initialSeq,
		nxt: initialSeq,
	}
}

// Write appends application data. Returns the number of bytes accepted.
// May return less than len(p) if the buffer is full.
func (s *SendBuf) Write(p []byte) int {
	avail := s.cap - len(s.buf)
	if avail <= 0 {
		return 0
	}
	n := len(p)
	if n > avail {
		n = avail
	}
	s.buf = append(s.buf, p[:n]...)
	return n
}

// Pending returns the number of bytes queued but not yet sent.
func (s *SendBuf) Pending() int {
	sent := int(s.nxt - s.una)
	return len(s.buf) - sent
}

// Unacked returns the number of sent-but-unacknowledged bytes.
func (s *SendBuf) Unacked() int {
	return int(s.nxt - s.una)
}

// PeekUnsent returns up to n bytes of unsent data without consuming them.
func (s *SendBuf) PeekUnsent(n int) []byte {
	offset := int(s.nxt - s.una)
	unsent := s.buf[offset:]
	if len(unsent) > n {
		unsent = unsent[:n]
	}
	return unsent
}

// AdvanceSent marks n bytes as sent (moves nxt forward).
func (s *SendBuf) AdvanceSent(n int) {
	s.nxt += uint32(n)
}

// Acknowledge advances una to ack, freeing buffer space.
// Returns the number of bytes newly acknowledged.
func (s *SendBuf) Acknowledge(ack uint32) uint32 {
	if !SeqAfter(ack, s.una) {
		return 0
	}
	if SeqAfter(ack, s.nxt) {
		// ACK beyond what we sent — clamp to nxt
		ack = s.nxt
	}
	n := ack - s.una
	if int(n) > len(s.buf) {
		n = uint32(len(s.buf))
	}
	s.buf = s.buf[n:]
	s.una = ack

	// Remove SACK blocks that are now below UNA
	s.pruneSACK()

	return n
}

// MarkSACKed records SACK blocks from the receiver. These indicate
// out-of-order data the receiver holds. Used to avoid retransmitting
// already-received segments.
func (s *SendBuf) MarkSACKed(blocks []SACKBlock) {
	if len(blocks) == 0 {
		return
	}
	// Replace the scoreboard with the latest SACK info from the receiver.
	// Each ACK with SACK provides a fresh view of the receiver's OOO state.
	s.sacked = make([]SACKBlock, 0, len(blocks))
	for _, b := range blocks {
		// Only keep blocks within our unacked range
		if SeqAfter(b.Right, s.una) && SeqBefore(b.Left, s.nxt) {
			s.sacked = append(s.sacked, b)
		}
	}
}

// pruneSACK removes SACK blocks that have been cumulatively acknowledged.
func (s *SendBuf) pruneSACK() {
	j := 0
	for _, b := range s.sacked {
		if SeqAfter(b.Right, s.una) {
			s.sacked[j] = b
			j++
		}
	}
	s.sacked = s.sacked[:j]
}

// IsSACKed reports whether the given sequence number has been selectively
// acknowledged by the receiver.
func (s *SendBuf) IsSACKed(seq uint32) bool {
	for _, b := range s.sacked {
		if SeqAfterEq(seq, b.Left) && SeqBefore(seq, b.Right) {
			return true
		}
	}
	return false
}

// RetransmitData returns the first n bytes of unacknowledged data (from una),
// skipping any SACK'd ranges to avoid redundant retransmissions.
func (s *SendBuf) RetransmitData(n int) []byte {
	unacked := int(s.nxt - s.una)
	if unacked > len(s.buf) {
		unacked = len(s.buf)
	}

	if len(s.sacked) == 0 {
		// No SACK info — retransmit from UNA
		data := s.buf[:unacked]
		if len(data) > n {
			data = data[:n]
		}
		return data
	}

	// Find the first unsacked byte starting from UNA by skipping SACK ranges.
	// This is O(m) where m = number of SACK blocks, not O(n*m).
	seq := s.una
	for changed := true; changed; {
		changed = false
		for _, b := range s.sacked {
			if SeqAfterEq(seq, b.Left) && SeqBefore(seq, b.Right) {
				seq = b.Right
				changed = true
			}
		}
	}
	if SeqAfterEq(seq, s.nxt) {
		return nil // everything is SACKed
	}

	offset := int(seq - s.una)
	if offset >= unacked {
		return nil
	}
	data := s.buf[offset:unacked]
	if len(data) > n {
		data = data[:n]
	}
	return data
}

// IsEmpty reports whether all data is acknowledged and nothing is queued.
func (s *SendBuf) IsEmpty() bool {
	return len(s.buf) == 0
}

// UNA returns SND.UNA.
func (s *SendBuf) UNA() uint32 { return s.una }

// NXT returns SND.NXT.
func (s *SendBuf) NXT() uint32 { return s.nxt }

// Available returns the number of bytes of free space in the buffer.
func (s *SendBuf) Available() int {
	return s.cap - len(s.buf)
}
