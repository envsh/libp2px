package vtcp

const maxOOOEntries = 128

// RecvBuf reassembles an incoming TCP byte stream, handling both in-order
// and out-of-order segments. It maintains a SACK scoreboard for reporting
// non-contiguous received blocks.
type RecvBuf struct {
	buf        []byte     // contiguous in-order data ready for Read
	nxt        uint32     // RCV.NXT: next expected sequence number
	ooo        []oooEntry // out-of-order segments, sorted by start seq
	windowSize int        // maximum receive window; 0 = unlimited
}

type oooEntry struct {
	seq  uint32
	data []byte
}

// NewRecvBuf creates a receive buffer with the given initial sequence number
// and maximum window size. Data beyond nxt+windowSize is rejected.
func NewRecvBuf(initialNxt uint32, windowSize int) *RecvBuf {
	return &RecvBuf{nxt: initialNxt, windowSize: windowSize}
}

// Window returns the current receive window (available space).
func (r *RecvBuf) Window() uint32 {
	if r.windowSize <= 0 {
		return 65535 // unlimited
	}
	used := len(r.buf)
	// Also count OOO data as consuming window space
	for _, e := range r.ooo {
		used += len(e.data)
	}
	avail := r.windowSize - used
	if avail < 0 {
		return 0
	}
	return uint32(avail)
}

// Insert adds a segment's payload at the given sequence number.
// Returns the number of new contiguous bytes added (available for Read).
// Out-of-order segments are buffered for later reassembly.
// Data beyond the receive window (nxt + windowSize) is trimmed.
func (r *RecvBuf) Insert(seq uint32, data []byte) int {
	if len(data) == 0 {
		return 0
	}

	endSeq := seq + uint32(len(data))

	// Trim data that's before nxt (already received / duplicate)
	if SeqBefore(seq, r.nxt) {
		overlap := r.nxt - seq
		if overlap >= uint32(len(data)) {
			return 0 // entirely duplicate
		}
		data = data[overlap:]
		seq = r.nxt
	}

	// Trim data beyond the receive window (RFC 9293 §3.10.7.4)
	if r.windowSize > 0 {
		rightEdge := r.nxt + uint32(r.windowSize)
		if SeqAfter(endSeq, rightEdge) {
			trim := endSeq - rightEdge
			if trim >= uint32(len(data)) {
				return 0 // entirely beyond window
			}
			data = data[:uint32(len(data))-trim]
			endSeq = rightEdge
		}
	}
	_ = endSeq

	if seq == r.nxt {
		// In-order: append to contiguous buffer
		r.buf = append(r.buf, data...)
		r.nxt = endSeq
		// Check if any OOO segments can now be merged
		r.mergeOOO()
		return len(data)
	}

	// Out-of-order: buffer for later (capped to prevent memory exhaustion)
	if len(r.ooo) < maxOOOEntries {
		r.insertOOO(seq, data)
	}
	return 0
}

// insertOOO inserts a segment into the out-of-order list, merging overlaps.
func (r *RecvBuf) insertOOO(seq uint32, data []byte) {
	endSeq := seq + uint32(len(data))

	// Find insertion point and merge with overlapping entries
	var merged []oooEntry
	inserted := false
	for _, e := range r.ooo {
		eEnd := e.seq + uint32(len(e.data))
		if SeqAfterEq(e.seq, endSeq) {
			// e is entirely after our segment
			if !inserted {
				merged = append(merged, oooEntry{seq: seq, data: append([]byte(nil), data...)})
				inserted = true
			}
			merged = append(merged, e)
		} else if SeqAfterEq(seq, eEnd) {
			// e is entirely before our segment
			merged = append(merged, e)
		} else {
			// Overlap: merge by extending our data range
			if SeqBefore(e.seq, seq) {
				// Extend left
				prefix := e.data[:seq-e.seq]
				data = append(append([]byte(nil), prefix...), data...)
				seq = e.seq
			}
			if SeqAfter(eEnd, endSeq) {
				// Extend right
				suffix := e.data[endSeq-e.seq:]
				data = append(data, suffix...)
				endSeq = eEnd
			}
		}
	}
	if !inserted {
		merged = append(merged, oooEntry{seq: seq, data: append([]byte(nil), data...)})
	}
	r.ooo = merged
}

// mergeOOO attempts to merge out-of-order entries that now connect to nxt.
func (r *RecvBuf) mergeOOO() {
	for {
		found := false
		remaining := r.ooo[:0]
		for _, e := range r.ooo {
			eEnd := e.seq + uint32(len(e.data))
			if SeqBeforeEq(e.seq, r.nxt) && SeqAfter(eEnd, r.nxt) {
				// This entry connects — take the new part
				offset := r.nxt - e.seq
				r.buf = append(r.buf, e.data[offset:]...)
				r.nxt = eEnd
				found = true
			} else if SeqAfter(e.seq, r.nxt) {
				remaining = append(remaining, e)
			}
			// else: entirely before nxt, discard
		}
		r.ooo = remaining
		if !found {
			break
		}
	}
}

// Read copies contiguous data into p and removes it from the buffer.
func (r *RecvBuf) Read(p []byte) int {
	n := copy(p, r.buf)
	r.buf = r.buf[n:]
	return n
}

// Readable returns the number of contiguous bytes available for Read.
func (r *RecvBuf) Readable() int {
	return len(r.buf)
}

// Nxt returns RCV.NXT (next expected sequence number).
func (r *RecvBuf) Nxt() uint32 {
	return r.nxt
}

// SACKBlocks returns up to 3 SACK blocks describing out-of-order data held.
func (r *RecvBuf) SACKBlocks() []SACKBlock {
	n := len(r.ooo)
	if n > 3 {
		n = 3
	}
	blocks := make([]SACKBlock, n)
	for i := range n {
		blocks[i] = SACKBlock{
			Left:  r.ooo[i].seq,
			Right: r.ooo[i].seq + uint32(len(r.ooo[i].data)),
		}
	}
	return blocks
}

// HasOOO reports whether there are out-of-order segments buffered.
func (r *RecvBuf) HasOOO() bool {
	return len(r.ooo) > 0
}
