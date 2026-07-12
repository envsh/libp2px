package vtcp

import "math"

// CongestionController defines the interface for TCP congestion control algorithms.
type CongestionController interface {
	// OnACK is called when new bytes are acknowledged.
	OnACK(bytesAcked uint32)
	// OnDupACK is called on each duplicate ACK.
	// Returns true if fast retransmit should be triggered (3rd dup ACK).
	OnDupACK() bool
	// OnTimeout is called on RTO timeout (loss detected via timeout).
	OnTimeout()
	// OnFastRetransmit is called when entering fast retransmit/recovery.
	// sndNxt is the current SND.NXT, used as the recovery point.
	OnFastRetransmit(flightSize uint32, sndNxt uint32)
	// ExitRecovery is called when recovery is complete (all data acked past recovery point).
	ExitRecovery()
	// SendWindow returns the current congestion window in bytes.
	SendWindow() uint32
	// InRecovery reports whether the sender is in fast recovery.
	InRecovery() bool
	// RecoverySeq returns the sequence number that must be fully acknowledged
	// to exit fast recovery (SND.NXT at the time recovery was entered).
	RecoverySeq() uint32
}

// NewReno implements RFC 5681 TCP congestion control:
// slow start, congestion avoidance, fast retransmit, fast recovery.
type NewReno struct {
	cwnd        uint32 // congestion window (bytes)
	ssthresh    uint32 // slow start threshold (bytes)
	mss         uint32 // max segment size (bytes)
	dupAckCnt   int    // consecutive duplicate ACK count
	recovery    bool   // in fast recovery
	recoverySeq uint32 // SND.NXT at time recovery was entered
}

// NewNewReno creates a NewReno congestion controller.
// Initial cwnd is set to min(10*MSS, max(2*MSS, 14600)) per RFC 6928.
func NewNewReno(mss uint32) *NewReno {
	initialCWND := 10 * mss
	if alt := max(2*mss, 14600); alt < initialCWND {
		initialCWND = alt
	}
	return &NewReno{
		cwnd:     initialCWND,
		ssthresh: ^uint32(0), // infinity until first loss
		mss:      mss,
	}
}

// OnACK processes a new ACK. RFC 5681 Section 3.1.
func (nr *NewReno) OnACK(bytesAcked uint32) {
	nr.dupAckCnt = 0

	if nr.cwnd < nr.ssthresh {
		// Slow start: increase by min(bytesAcked, MSS) per ACK
		inc := bytesAcked
		if inc > nr.mss {
			inc = nr.mss
		}
		nr.cwnd += inc
	} else {
		// Congestion avoidance: increase by MSS^2/cwnd per ACK (approx +1 MSS per RTT)
		inc := nr.mss * nr.mss / nr.cwnd
		if inc == 0 {
			inc = 1
		}
		nr.cwnd += inc
	}
}

// OnDupACK processes a duplicate ACK. Returns true if this is the 3rd dup ACK
// (triggering fast retransmit). RFC 5681 Section 3.2.
func (nr *NewReno) OnDupACK() bool {
	nr.dupAckCnt++
	if nr.dupAckCnt == 3 && !nr.recovery {
		return true
	}
	// During fast recovery, inflate cwnd for each additional dup ACK
	if nr.recovery && nr.dupAckCnt > 3 {
		nr.cwnd += nr.mss
	}
	return false
}

// OnFastRetransmit enters fast recovery. RFC 5681 Section 3.2.
func (nr *NewReno) OnFastRetransmit(flightSize uint32, sndNxt uint32) {
	nr.ssthresh = max(flightSize/2, 2*nr.mss)
	nr.cwnd = nr.ssthresh + 3*nr.mss // inflate for the 3 dup ACKs
	nr.recovery = true
	nr.recoverySeq = sndNxt
}

// ExitRecovery leaves fast recovery, deflating cwnd. RFC 5681 Section 3.2.
func (nr *NewReno) ExitRecovery() {
	nr.cwnd = nr.ssthresh
	nr.recovery = false
	nr.dupAckCnt = 0
	nr.recoverySeq = 0
}

// OnTimeout handles RTO timeout. RFC 5681 Section 3.1.
func (nr *NewReno) OnTimeout() {
	nr.ssthresh = max(nr.cwnd/2, 2*nr.mss)
	nr.cwnd = nr.mss // reset to 1 MSS (slow start)
	nr.recovery = false
	nr.dupAckCnt = 0
	nr.recoverySeq = 0
}

// SendWindow returns the current congestion window.
func (nr *NewReno) SendWindow() uint32 {
	return nr.cwnd
}

// InRecovery reports whether we are in fast recovery.
func (nr *NewReno) InRecovery() bool {
	return nr.recovery
}

// RecoverySeq returns the recovery point (SND.NXT at time of entering recovery).
func (nr *NewReno) RecoverySeq() uint32 {
	return nr.recoverySeq
}

// SSThresh returns the current slow start threshold.
func (nr *NewReno) SSThresh() uint32 {
	return nr.ssthresh
}

// HighSpeed implements RFC 3649 HighSpeed TCP for large congestion windows.
// When cwnd <= Low_Window (38 segments), it behaves identically to NewReno.
// Above that threshold, it uses more aggressive increase/decrease functions
// that scale better on high-BDP networks.
type HighSpeed struct {
	cwnd        uint32
	ssthresh    uint32
	mss         uint32
	dupAckCnt   int
	recovery    bool
	recoverySeq uint32
}

// RFC 3649 parameters
const (
	hsLowWindow    = 38    // segments — below this, use standard TCP
	hsHighWindow   = 83000 // segments
	hsHighDecrease = 0.1
)

// NewHighSpeed creates a HighSpeed TCP congestion controller (RFC 3649).
func NewHighSpeed(mss uint32) *HighSpeed {
	initialCWND := 10 * mss
	if alt := max(2*mss, 14600); alt < initialCWND {
		initialCWND = alt
	}
	return &HighSpeed{
		cwnd:     initialCWND,
		ssthresh: ^uint32(0),
		mss:      mss,
	}
}

// hstcpA returns the increase parameter a(w) for HighSpeed TCP.
// w is in segments (cwnd/mss).
func hstcpA(w uint32) float64 {
	if w <= hsLowWindow {
		return 1.0
	}
	b := hstcpB(w)
	// p(w) = 0.078 / w^1.2
	p := 0.078 / math.Pow(float64(w), 1.2)
	// a(w) = w^2 * p * 2*b / (2-b)
	wf := float64(w)
	return wf * wf * p * 2.0 * b / (2.0 - b)
}

// hstcpB returns the decrease parameter b(w) for HighSpeed TCP.
// w is in segments.
func hstcpB(w uint32) float64 {
	if w <= hsLowWindow {
		return 0.5
	}
	// b(w) = (High_Decrease - 0.5) * (log(w) - log(Low_Window)) /
	//        (log(High_Window) - log(Low_Window)) + 0.5
	logW := math.Log(float64(w))
	logLow := math.Log(float64(hsLowWindow))
	logHigh := math.Log(float64(hsHighWindow))
	return (hsHighDecrease-0.5)*(logW-logLow)/(logHigh-logLow) + 0.5
}

func (hs *HighSpeed) OnACK(bytesAcked uint32) {
	hs.dupAckCnt = 0

	if hs.cwnd < hs.ssthresh {
		// Slow start: same as standard TCP
		inc := bytesAcked
		if inc > hs.mss {
			inc = hs.mss
		}
		hs.cwnd += inc
	} else {
		// Congestion avoidance: w += a(w)/w per ACK (in bytes)
		wSegs := hs.cwnd / hs.mss
		a := hstcpA(wSegs)
		// Convert: increase in bytes = a * MSS^2 / cwnd
		// (since a(w) is defined for w in segments, and we want bytes)
		inc := uint32(a * float64(hs.mss) * float64(hs.mss) / float64(hs.cwnd))
		if inc == 0 {
			inc = 1
		}
		hs.cwnd += inc
	}
}

func (hs *HighSpeed) OnDupACK() bool {
	hs.dupAckCnt++
	if hs.dupAckCnt == 3 && !hs.recovery {
		return true
	}
	if hs.recovery && hs.dupAckCnt > 3 {
		hs.cwnd += hs.mss
	}
	return false
}

func (hs *HighSpeed) OnFastRetransmit(flightSize uint32, sndNxt uint32) {
	wSegs := hs.cwnd / hs.mss
	b := hstcpB(wSegs)
	// ssthresh = (1 - b(w)) * cwnd
	hs.ssthresh = uint32(float64(hs.cwnd) * (1.0 - b))
	if hs.ssthresh < 2*hs.mss {
		hs.ssthresh = 2 * hs.mss
	}
	hs.cwnd = hs.ssthresh + 3*hs.mss
	hs.recovery = true
	hs.recoverySeq = sndNxt
}

func (hs *HighSpeed) ExitRecovery() {
	hs.cwnd = hs.ssthresh
	hs.recovery = false
	hs.dupAckCnt = 0
	hs.recoverySeq = 0
}

func (hs *HighSpeed) OnTimeout() {
	wSegs := hs.cwnd / hs.mss
	b := hstcpB(wSegs)
	hs.ssthresh = uint32(float64(hs.cwnd) * (1.0 - b))
	if hs.ssthresh < 2*hs.mss {
		hs.ssthresh = 2 * hs.mss
	}
	hs.cwnd = hs.mss
	hs.recovery = false
	hs.dupAckCnt = 0
	hs.recoverySeq = 0
}

func (hs *HighSpeed) SendWindow() uint32  { return hs.cwnd }
func (hs *HighSpeed) InRecovery() bool    { return hs.recovery }
func (hs *HighSpeed) RecoverySeq() uint32 { return hs.recoverySeq }
func (hs *HighSpeed) SSThresh() uint32    { return hs.ssthresh }
