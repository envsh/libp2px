package vtcp

// Sequence number arithmetic for TCP (RFC 793 Section 3.3).
// TCP sequence numbers are unsigned 32-bit integers that wrap around.
// Comparisons use signed 32-bit subtraction to handle wraparound correctly.

// SeqBefore reports whether sequence number a is before b.
func SeqBefore(a, b uint32) bool { return int32(a-b) < 0 }

// SeqAfter reports whether sequence number a is after b.
func SeqAfter(a, b uint32) bool { return int32(a-b) > 0 }

// SeqBeforeEq reports whether a is before or equal to b.
func SeqBeforeEq(a, b uint32) bool { return int32(a-b) <= 0 }

// SeqAfterEq reports whether a is after or equal to b.
func SeqAfterEq(a, b uint32) bool { return int32(a-b) >= 0 }

// SeqInRange reports whether seq is in [lo, hi) in sequence space.
func SeqInRange(seq, lo, hi uint32) bool {
	return SeqAfterEq(seq, lo) && SeqBefore(seq, hi)
}

// SeqInRangeInclusive reports whether seq is in [lo, hi] in sequence space.
func SeqInRangeInclusive(seq, lo, hi uint32) bool {
	return SeqAfterEq(seq, lo) && SeqBeforeEq(seq, hi)
}
