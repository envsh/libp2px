package vtcp

import "time"

// RTOCalculator implements RFC 6298 RTO computation.
type RTOCalculator struct {
	srtt     time.Duration
	rttvar   time.Duration
	rto      time.Duration
	measured bool // at least one RTT sample taken

	// Karn's algorithm state
	timing   bool
	timeSent time.Time
	timeSeq  uint32
}

// NewRTOCalculator creates an RTO calculator with the default initial RTO.
func NewRTOCalculator() *RTOCalculator {
	return &RTOCalculator{rto: DefaultRTO}
}

// Sample records an RTT measurement and recalculates RTO per RFC 6298.
func (r *RTOCalculator) Sample(rtt time.Duration) {
	if !r.measured {
		// First measurement (RFC 6298 Section 2.2)
		r.srtt = rtt
		r.rttvar = rtt / 2
		r.measured = true
	} else {
		// Subsequent measurements (RFC 6298 Section 2.3)
		// IMPORTANT: RTTVAR must be updated BEFORE SRTT
		diff := r.srtt - rtt
		if diff < 0 {
			diff = -diff
		}
		r.rttvar = (3*r.rttvar + diff) / 4
		r.srtt = (7*r.srtt + rtt) / 8
	}
	r.rto = r.srtt + 4*r.rttvar
	r.clamp()
}

// Backoff doubles the current RTO (exponential backoff on timeout).
func (r *RTOCalculator) Backoff() {
	r.rto *= 2
	r.clamp()
}

// RTO returns the current retransmission timeout.
func (r *RTOCalculator) RTO() time.Duration {
	return r.rto
}

// SRTT returns the smoothed RTT. Returns 0 if no measurement yet.
func (r *RTOCalculator) SRTT() time.Duration {
	return r.srtt
}

// StartTiming begins timing a segment for RTT measurement.
func (r *RTOCalculator) StartTiming(seq uint32) {
	if r.timing {
		return // already timing a segment
	}
	r.timing = true
	r.timeSent = time.Now()
	r.timeSeq = seq
}

// AckReceived checks if the ACK covers the timed segment.
// If so, records the sample and stops timing. Returns true if a sample was taken.
func (r *RTOCalculator) AckReceived(ack uint32) bool {
	if !r.timing {
		return false
	}
	if SeqAfter(ack, r.timeSeq) {
		r.Sample(time.Since(r.timeSent))
		r.timing = false
		return true
	}
	return false
}

// InvalidateTiming implements Karn's algorithm: discard the current
// timing on retransmission (don't sample retransmitted segments).
func (r *RTOCalculator) InvalidateTiming() {
	r.timing = false
}

func (r *RTOCalculator) clamp() {
	if r.rto < MinRTO {
		r.rto = MinRTO
	}
	if r.rto > MaxRTO {
		r.rto = MaxRTO
	}
}
