package p2put

import (
	"context"
	"errors"
	"log"
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	pbv2 "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/pb"

	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	"github.com/multiformats/go-multiaddr"
)

const (
	probationMax    = 10
	poolHighWater   = 50
	poolLowWater    = 40
	demoteThreshold = 3
)

type errType int

const (
	errOK errType = iota
	errRateLimited
	errFailed
)

type WeightConfig struct {
	Success        float64
	Latency        float64
	DataLimit      float64
	DurationLimit  float64
	TTL            float64
	Uptime         float64
}

var defaultWeights = WeightConfig{
	Success:       0.35,
	Latency:       0.20,
	DataLimit:     0.15,
	DurationLimit: 0.15,
	TTL:           0.10,
	Uptime:        0.05,
}

// RelayItem represents a single relay peer in the pool.
// Each item lives in one of two tiers: probation (new, unproven) or main (proven via successful push).
// Probation uses FIFO eviction; main uses score-based eviction with protection rounds.
type RelayItem struct {
	PeerID             peer.ID
	Addr               multiaddr.Multiaddr

	// inMain is true after the first successful RecordResult (promotion from probation).
	// Set to false after demoteThreshold consecutive failures (demotion back to probation).
	inMain             bool
	successScore       float64
	avgRTT             time.Duration
	limitDuration      time.Duration
	limitData          int64
	rateLimitHits      int
	consecutiveFails   int
	circuitOpen        bool
	reservationExpires time.Time
	connectedSince     time.Time
	lastResult         time.Time
}

// RelayPool manages a two-tier pool of relay peers (probation + main).
// Capacity: total ≤ highWater (50). When exceeded, prune() halves the eviction
// between probation (FIFO) and main (score + protection rounds) down to lowWater (40).
// probationMax (10) soft-caps the probation tier; evictOneLocked always picks
// probation first when making room for a new Add.
type RelayPool struct {
	mu        sync.RWMutex
	items     map[peer.ID]*RelayItem
	config    WeightConfig
	protected map[peer.ID]bool
	managed   map[peer.ID]struct{}
	lowWater  int
	highWater int
}

type RelayPoolStats struct {
	Total        int
	Healthy      int
	CircuitOpen  int
	AvgScore     float64
	ProbationCnt int
	MainCnt      int
}

func NewRelayPool(cfg WeightConfig) *RelayPool {
	if cfg.Success == 0 && cfg.Latency == 0 && cfg.DataLimit == 0 &&
		cfg.DurationLimit == 0 && cfg.TTL == 0 && cfg.Uptime == 0 {
		cfg = defaultWeights
	}
	p := &RelayPool{
		items:     make(map[peer.ID]*RelayItem),
		config:    cfg,
		protected: make(map[peer.ID]bool),
		managed:   make(map[peer.ID]struct{}),
		lowWater:  poolLowWater,
		highWater: poolHighWater,
	}
	log.Printf("[relaypool] created probation=%d low=%d high=%d weights=%.2f/%.2f/%.2f/%.2f/%.2f/%.2f",
		probationMax, p.lowWater, p.highWater,
		cfg.Success, cfg.Latency, cfg.DataLimit, cfg.DurationLimit, cfg.TTL, cfg.Uptime)
	return p
}

func (p *RelayPool) Add(ai string) {
	m, err := multiaddr.NewMultiaddr(ai)
	if err != nil {
		log.Printf("[relaypool] add invalid addr: %s", err)
		return
	}
	pidStr, err := m.ValueForProtocol(multiaddr.P_P2P)
	if err != nil {
		log.Printf("[relaypool] add missing peer id: %s", ai)
		return
	}
	pid, err := peer.Decode(pidStr)
	if err != nil {
		log.Printf("[relaypool] add decode peer: %s", err)
		return
	}
	_, addr := multiaddr.SplitFunc(m, func(c multiaddr.Component) bool {
		return c.Protocol().Code == multiaddr.P_P2P
	})

	p.mu.Lock()
	if _, ok := p.items[pid]; ok {
		p.mu.Unlock()
		return
	}

	p.items[pid] = &RelayItem{
		PeerID:         pid,
		Addr:           addr,
		successScore:   0.70,
		connectedSince: time.Now(),
	}
	p.mu.Unlock()
	log.Printf("[relaypool] added %s (%s) probation", pid.ShortString(), addr.String())

	p.prune()
}

func (p *RelayPool) Remove(pid peer.ID) {
	p.mu.Lock()
	delete(p.items, pid)
	delete(p.protected, pid)
	delete(p.managed, pid)
	p.mu.Unlock()
	log.Printf("[relaypool] removed %s", pid.ShortString())
}

func (p *RelayPool) Protect(pid peer.ID) {
	p.mu.Lock()
	p.protected[pid] = true
	p.mu.Unlock()
	log.Printf("[relaypool] protect %s", pid.ShortString())
}

func (p *RelayPool) Unprotect(pid peer.ID) {
	p.mu.Lock()
	delete(p.protected, pid)
	p.mu.Unlock()
	log.Printf("[relaypool] unprotect %s", pid.ShortString())
}

func (p *RelayPool) AddManaged(pids ...peer.ID) {
	p.mu.Lock()
	for _, pid := range pids {
		p.managed[pid] = struct{}{}
	}
	p.mu.Unlock()
}

func (p *RelayPool) RemoveManaged(pids ...peer.ID) {
	p.mu.Lock()
	for _, pid := range pids {
		delete(p.managed, pid)
	}
	p.mu.Unlock()
}

func (p *RelayPool) ListManaged() []peer.ID {
	p.mu.RLock()
	out := make([]peer.ID, 0, len(p.managed))
	for pid := range p.managed {
		out = append(out, pid)
	}
	p.mu.RUnlock()
	return out
}

func (p *RelayPool) IsCircuitOpen(pid peer.ID) bool {
	p.mu.RLock()
	item, ok := p.items[pid]
	if !ok {
		p.mu.RUnlock()
		return false
	}
	open := item.circuitOpen
	p.mu.RUnlock()
	return open
}

// RecordResult updates a relay's score and tier based on the outcome.
//   errOK:           promotion (probation → main) if not already inMain
//   errRateLimited:  score decay with higher penalty (α=0.5)
//   errFailed:       demotion (main → probation) after demoteThreshold consecutive failures
//                     or circuit breaker at 5 consecutive failures
func (p *RelayPool) RecordResult(pid peer.ID, err error) {
	p.mu.Lock()
	item, ok := p.items[pid]
	if !ok {
		p.mu.Unlock()
		return
	}

	switch classifyError(err) {
	case errOK:
		item.successScore = ema(item.successScore, 1, 0.3)
		item.consecutiveFails = 0
		item.circuitOpen = false
		if !item.inMain {
			item.inMain = true
			log.Printf("[relaypool] promoted %s to main", pid.ShortString())
		}

	case errRateLimited:
		item.successScore = ema(item.successScore, 0, 0.5)
		item.rateLimitHits++

	case errFailed:
		item.successScore = ema(item.successScore, 0, 0.3)
		item.consecutiveFails++
		if item.inMain && item.consecutiveFails >= demoteThreshold {
			item.inMain = false
			item.successScore = 0.70
			log.Printf("[relaypool] demoted %s to probation", pid.ShortString())
		}
		if item.consecutiveFails >= 5 {
			item.circuitOpen = true
			log.Printf("[relaypool] circuit open %s", pid.ShortString())
		}
	}
	item.lastResult = time.Now()
	score := p.calcScore(item)
	p.mu.Unlock()

	p.prune()
	log.Printf("[relaypool] record %s err=%v score=%.3f inMain=%v",
		pid.ShortString(), err, score, item.inMain)
}

func (p *RelayPool) SetRelayLimits(pid peer.ID, dur time.Duration, data int64) {
	p.mu.Lock()
	item, ok := p.items[pid]
	if ok {
		item.limitDuration = dur
		item.limitData = data
	}
	p.mu.Unlock()
}

func (p *RelayPool) SetReservationTTL(pid peer.ID, expires time.Time) {
	p.mu.Lock()
	item, ok := p.items[pid]
	if ok {
		item.reservationExpires = expires
	}
	p.mu.Unlock()
}

func (p *RelayPool) Select() multiaddr.Multiaddr {
	p.mu.RLock()
	var entries []struct {
		pid   peer.ID
		score float64
		addr  multiaddr.Multiaddr
	}
	for _, item := range p.items {
		if item.circuitOpen {
			continue
		}
		score := p.calcScore(item)
		entries = append(entries, struct {
			pid   peer.ID
			score float64
			addr  multiaddr.Multiaddr
		}{item.PeerID, score, item.Addr})
	}
	p.mu.RUnlock()

	if len(entries) == 0 {
		return nil
	}

	if rand.Float64() < 0.1 {
		idx := rand.Intn(len(entries))
		log.Printf("[relaypool] select random %s score=%.3f",
			entries[idx].pid.ShortString(), entries[idx].score)
		return entries[idx].addr
	}

	total := 0.0
	for _, e := range entries {
		total += e.score
	}

	var selected multiaddr.Multiaddr
	if total == 0 {
		idx := rand.Intn(len(entries))
		selected = entries[idx].addr
	} else {
		r := rand.Float64() * total
		for _, e := range entries {
			r -= e.score
			if r <= 0 {
				selected = e.addr
				break
			}
		}
		if selected == nil {
			selected = entries[len(entries)-1].addr
		}
	}
	return selected
}

func (p *RelayPool) SelectN(n int) []peer.AddrInfo {
	p.mu.RLock()

	type scored struct {
		pid   peer.ID
		score float64
		addr  multiaddr.Multiaddr
	}
	var entries []scored
	for pid, item := range p.items {
		if item.circuitOpen {
			continue
		}
		entries = append(entries, scored{pid, p.calcScore(item), item.Addr})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].score > entries[j].score
	})
	p.mu.RUnlock()

	if n > len(entries) {
		n = len(entries)
	}
	infos := make([]peer.AddrInfo, n)
	for i := 0; i < n; i++ {
		infos[i] = peer.AddrInfo{ID: entries[i].pid, Addrs: []multiaddr.Multiaddr{entries[i].addr}}
	}
	if n > 0 {
		log.Printf("[relaypool] selectN %d/%d top=%s score=%.3f", n, len(entries), entries[0].pid.ShortString(), entries[0].score)
	}
	return infos
}

func (p *RelayPool) SetWeights(w WeightConfig) {
	p.mu.Lock()
	p.config = w
	p.mu.Unlock()
	log.Printf("[relaypool] weights updated %.2f/%.2f/%.2f/%.2f/%.2f/%.2f",
		w.Success, w.Latency, w.DataLimit, w.DurationLimit, w.TTL, w.Uptime)
}

func (p *RelayPool) SetWeight(factor int, value float64) {
	if value < 0 {
		value = 0
	}
	if value > 1 {
		value = 1
	}
	p.mu.Lock()
	switch factor {
	case 0:
		p.config.Success = value
	case 1:
		p.config.Latency = value
	case 2:
		p.config.DataLimit = value
	case 3:
		p.config.DurationLimit = value
	case 4:
		p.config.TTL = value
	case 5:
		p.config.Uptime = value
	}
	p.mu.Unlock()
}

func (p *RelayPool) StartHealthCheck(ctx context.Context, interval time.Duration) {
	if bootres == nil || bootres.Host == nil {
		log.Printf("[relaypool] health check skipped: host not ready")
		return
	}
	h := bootres.Host

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		log.Printf("[relaypool] health check started interval=%v", interval)
		for {
			select {
			case <-ticker.C:
				p.mu.RLock()
				var targets []peer.ID
				for _, item := range p.items {
					if item.circuitOpen {
						continue
					}
					if h.Network().Connectedness(item.PeerID) != network.Connected {
						continue
					}
					targets = append(targets, item.PeerID)
				}
				p.mu.RUnlock()

				for _, pid := range targets {
					pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
					start := time.Now()
					res := <-ping.Ping(pctx, h, pid)
					cancel()
					if res.Error == nil {
						p.mu.Lock()
						if item, ok := p.items[pid]; ok {
							item.avgRTT = emaRTT(item.avgRTT, time.Since(start), 0.3)
							item.successScore = ema(item.successScore, 1, 0.3)
							item.consecutiveFails = 0
						}
						p.mu.Unlock()
					} else {
						p.RecordResult(pid, res.Error)
					}
				}
			case <-ctx.Done():
				log.Printf("[relaypool] health check stopped")
				return
			}
		}
	}()
}

func (p *RelayPool) Stats() RelayPoolStats {
	p.mu.RLock()
	defer p.mu.RUnlock()

	var s RelayPoolStats
	s.Total = len(p.items)
	totalScore := 0.0
	for _, item := range p.items {
		if item.inMain {
			s.MainCnt++
		} else {
			s.ProbationCnt++
		}
		if item.circuitOpen {
			s.CircuitOpen++
		} else {
			s.Healthy++
		}
		totalScore += p.calcScore(item)
	}
	if s.Total > 0 {
		s.AvgScore = totalScore / float64(s.Total)
	}
	return s
}

func (p *RelayPool) calcScore(item *RelayItem) float64 {
	if item.circuitOpen {
		return 0
	}
	s := p.config.Success * item.successScore
	s += p.config.Latency * calcLatency(item.avgRTT)
	s += p.config.DataLimit * calcDataLimit(item.limitData)
	s += p.config.DurationLimit * calcDurationLimit(item.limitDuration)
	s += p.config.TTL * calcTTL(item.reservationExpires)
	s += p.config.Uptime * calcUptime(item.connectedSince)
	return s
}

// prune reduces the pool from highWater to lowWater when total exceeds highWater.
// Eviction is split: need/2 from probation (FIFO), need/2 from main (protection rounds + lowest score).
// If one tier has fewer items than its quota, the other tier makes up the difference.
// Protection rounds for main items (applied in order):
//   1. Recent activity: lastResult within 5 minutes → protected
//   2. Top-20% by score → protected
//   3. Top-10 by uptime (connectedSince) → protected
//   4. Remaining candidates evicted by ascending score until quota is met
func (p *RelayPool) prune() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.items) <= p.highWater {
		return
	}

	need := len(p.items) - p.lowWater

	var probationPids []peer.ID
	var mainPids []peer.ID
	for pid, item := range p.items {
		if item.inMain {
			mainPids = append(mainPids, pid)
		} else {
			probationPids = append(probationPids, pid)
		}
	}

	evictFromProbation := need / 2
	evictFromMain := need - evictFromProbation

	if evictFromProbation > len(probationPids) {
		evictFromMain += evictFromProbation - len(probationPids)
		evictFromProbation = len(probationPids)
	}
	if evictFromMain > len(mainPids) {
		evictFromProbation += evictFromMain - len(mainPids)
		evictFromMain = len(mainPids)
	}

	pruned := 0

	// probation tier: FIFO by connectedSince
	if evictFromProbation > 0 {
		sort.Slice(probationPids, func(i, j int) bool {
			return p.items[probationPids[i]].connectedSince.Before(
				p.items[probationPids[j]].connectedSince)
		})
		for _, pid := range probationPids {
			if evictFromProbation <= 0 {
				break
			}
			delete(p.items, pid)
			delete(p.protected, pid)
			evictFromProbation--
			pruned++
		}
	}

	// main tier: protection rounds then lowest-score eviction
	if evictFromMain > 0 {
		type kv struct {
			pid   peer.ID
			score float64
		}
		var candidates []kv
		for _, pid := range mainPids {
			if _, ok := p.items[pid]; !ok {
				continue
			}
			if p.protected[pid] {
				continue
			}
			item := p.items[pid]

			// round 1: protect recent activity (last 5 minutes)
			if time.Since(item.lastResult) < 5*time.Minute {
				continue
			}
			candidates = append(candidates, kv{pid, p.calcScore(item)})
		}

		// round 2: protect top-20% by score
		topK := int(math.Ceil(float64(len(candidates)) * 0.20))
		if topK > 0 && topK < len(candidates) {
			sort.Slice(candidates, func(i, j int) bool {
				return candidates[i].score > candidates[j].score
			})
			candidates = candidates[topK:]
		} else if topK >= len(candidates) {
			candidates = nil
		}

		// round 3: protect top-10 by uptime (connectedSince)
		if len(candidates) > 0 {
			uptimeK := 10
			if uptimeK > len(candidates) {
				uptimeK = len(candidates)
			}
			if uptimeK > 0 && uptimeK < len(candidates) {
				sort.Slice(candidates, func(i, j int) bool {
					return p.items[candidates[i].pid].connectedSince.Before(
						p.items[candidates[j].pid].connectedSince)
				})
				candidates = candidates[uptimeK:]
			} else if uptimeK >= len(candidates) {
				candidates = nil
			}
		}

		// final: evict lowest score among remaining candidates
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].score < candidates[j].score
		})
		for _, kv := range candidates {
			if evictFromMain <= 0 {
				break
			}
			delete(p.items, kv.pid)
			delete(p.protected, kv.pid)
			evictFromMain--
			pruned++
		}

		// spill remaining eviction quota to probation (FIFO)
		if evictFromMain > 0 {
			var remainingProbation []peer.ID
			for pid, item := range p.items {
				if !item.inMain {
					remainingProbation = append(remainingProbation, pid)
				}
			}
			sort.Slice(remainingProbation, func(i, j int) bool {
				return p.items[remainingProbation[i]].connectedSince.Before(
					p.items[remainingProbation[j]].connectedSince)
			})
			for _, pid := range remainingProbation {
				if evictFromMain <= 0 {
					break
				}
				delete(p.items, pid)
				delete(p.protected, pid)
				evictFromMain--
				pruned++
			}
		}
	}

	probationCnt := 0
	for _, item := range p.items {
		if !item.inMain {
			probationCnt++
		}
	}
	log.Printf("[relaypool] pruned %d items (total=%d, probation=%d, main=%d)",
		pruned, len(p.items), probationCnt, len(p.items)-probationCnt)
}

func classifyError(err error) errType {
	if err == nil {
		return errOK
	}
	if errors.Is(err, network.ErrReset) {
		return errRateLimited
	}
	var re *client.ReservationError
	if errors.As(err, &re) && re.Status == pbv2.Status_RESERVATION_REFUSED {
		return errRateLimited
	}
	return errFailed
}

func ema(old, newVal, alpha float64) float64 {
	return alpha*newVal + (1-alpha)*old
}

func emaRTT(old, newVal time.Duration, alpha float64) time.Duration {
	return time.Duration(alpha*float64(newVal) + (1-alpha)*float64(old))
}

func calcLatency(rtt time.Duration) float64 {
	if rtt == 0 {
		return 0.5
	}
	if rtt < 50*time.Millisecond {
		return 1.0
	}
	if rtt > 2000*time.Millisecond {
		return 0.1
	}
	return 1.0 - float64(rtt-50*time.Millisecond)/float64(1950*time.Millisecond)*0.9
}

func calcDataLimit(data int64) float64 {
	if data == 0 {
		return 0.5
	}
	return math.Min(float64(data)/float64(128*1024), 1.0)
}

func calcDurationLimit(dur time.Duration) float64 {
	if dur == 0 {
		return 0.5
	}
	return math.Min(float64(dur)/float64(2*time.Minute), 1.0)
}

func calcTTL(expires time.Time) float64 {
	remaining := time.Until(expires)
	if remaining < 0 {
		return 0
	}
	if remaining > 10*time.Minute {
		return 1.0
	}
	return float64(remaining) / float64(10*time.Minute)
}

func calcUptime(since time.Time) float64 {
	d := time.Since(since)
	if d > 5*time.Minute {
		return 1.0
	}
	if d < 10*time.Second {
		return 0.3
	}
	return 0.3 + float64(d-10*time.Second)/float64(290*time.Second)*0.7
}
