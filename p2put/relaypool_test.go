package p2put

import (
	"errors"
	"testing"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

var testPids []peer.ID

func init() {
	for i := 0; i < 200; i++ {
		priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, 0)
		if err != nil {
			panic(err)
		}
		pid, err := peer.IDFromPrivateKey(priv)
		if err != nil {
			panic(err)
		}
		testPids = append(testPids, pid)
	}
}

func newTestPid(t *testing.T, n int) peer.ID {
	t.Helper()
	if n < 1 || n > len(testPids) {
		t.Fatalf("test pid index %d out of range (1-%d)", n, len(testPids))
	}
	return testPids[n-1]
}

func testAddr(pid peer.ID) string {
	return "/ip4/127.0.0.1/tcp/4001/p2p/" + pid.String()
}

func assertLen(t *testing.T, rp *RelayPool, want int) {
	t.Helper()
	rp.mu.RLock()
	got := len(rp.items)
	rp.mu.RUnlock()
	if got != want {
		t.Errorf("len = %d, want %d", got, want)
	}
}

func assertStats(t *testing.T, rp *RelayPool, wantTotal, wantMain, wantProbation int) {
	t.Helper()
	s := rp.Stats()
	if s.Total != wantTotal {
		t.Errorf("Stats.Total = %d, want %d", s.Total, wantTotal)
	}
	if s.MainCnt != wantMain {
		t.Errorf("Stats.MainCnt = %d, want %d", s.MainCnt, wantMain)
	}
	if s.ProbationCnt != wantProbation {
		t.Errorf("Stats.ProbationCnt = %d, want %d", s.ProbationCnt, wantProbation)
	}
}

// ────────────────────────── 1. Add ──────────────────────────

func TestAdd_Normal(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	pid := newTestPid(t, 1)
	rp.Add(testAddr(pid))
	assertStats(t, rp, 1, 0, 1)
}

func TestAdd_Duplicate(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	pid := newTestPid(t, 1)
	rp.Add(testAddr(pid))
	rp.Add(testAddr(pid))
	assertStats(t, rp, 1, 0, 1)
}

func TestAdd_InvalidAddr(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	rp.Add("/not/a/multiaddr")
	assertLen(t, rp, 0)
}

func TestAdd_MissingPeerID(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	rp.Add("/ip4/127.0.0.1/tcp/4001")
	assertLen(t, rp, 0)
}

func TestAdd_ProbationFull(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	for i := 0; i < probationMax; i++ {
		pid := newTestPid(t, i+1)
		rp.Add(testAddr(pid))
	}
	// probation full at 10, adding more succeeds as long as total ≤ highWater
	assertStats(t, rp, probationMax, 0, probationMax)
	// total 11, still under highWater (50)
	pid := newTestPid(t, 99)
	rp.Add(testAddr(pid))
	assertStats(t, rp, probationMax+1, 0, probationMax+1)
}

func TestAdd_TotalFull_EvictsProbation(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	// fill to highWater
	for i := 0; i < poolHighWater; i++ {
		pid := newTestPid(t, i+1)
		rp.Add(testAddr(pid))
	}
	// add one more triggers prune: 51→need=11→evict 11 oldest probation→40
	pid99 := newTestPid(t, 99)
	rp.Add(testAddr(pid99))
	assertStats(t, rp, poolLowWater, 0, poolLowWater)
}

func TestAdd_TotalFull_AllProtected(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	// fill main with protected items
	for i := 0; i < 40; i++ {
		pid := newTestPid(t, i+1)
		rp.Add(testAddr(pid))
		rp.RecordResult(pid, nil) // promote to main
		rp.Protect(pid)
	}
	assertStats(t, rp, 40, 40, 0)
	// add items past highWater — prune evicts probation only,
	// protected main items survive
	for i := 40; i < poolHighWater+5; i++ {
		pid := newTestPid(t, i+1)
		rp.Add(testAddr(pid))
	}
	s := rp.Stats()
	if s.Total < 40 {
		t.Errorf("total should not drop below protected count: total=%d", s.Total)
	}
	// all protected main items should still be present
	for i := 0; i < 40; i++ {
		pid := newTestPid(t, i+1)
		rp.mu.RLock()
		_, ok := rp.items[pid]
		rp.mu.RUnlock()
		if !ok {
			t.Errorf("protected pid=%d should survive", i+1)
		}
	}
}

// ────────────────────────── 2. Promotion ──────────────────────────

func TestPromote_ProbationToMain(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	pid := newTestPid(t, 1)
	rp.Add(testAddr(pid))
	assertStats(t, rp, 1, 0, 1)

	rp.RecordResult(pid, nil) // errOK
	assertStats(t, rp, 1, 1, 0)
}

func TestPromote_MainFullPrunesFirst(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	// 40 main + 10 probation = 50 (highWater)
	for i := 0; i < 40; i++ {
		pid := newTestPid(t, i+1)
		rp.Add(testAddr(pid))
		rp.RecordResult(pid, nil)
	}
	for i := 40; i < 50; i++ {
		pid := newTestPid(t, i+1)
		rp.Add(testAddr(pid))
	}
	// backdate enough main items (20) to overcome protection rounds and hit lowWater
	for i := 0; i < 20; i++ {
		rp.mu.Lock()
		rp.items[newTestPid(t, i+1)].lastResult = time.Now().Add(-10 * time.Minute)
		rp.mu.Unlock()
	}
	// add past highWater triggers prune
	pid99 := newTestPid(t, 99)
	rp.Add(testAddr(pid99))
	rp.RecordResult(pid99, nil)
	// need=11, evictFromProb=5, evictFromMain=6
	// 20 pass round 1 → round 2: topK=4 → 16 remain → round 3: uptimeK=10 → 6 remain → evict 6
	// total = 51 - 5 - 6 = 40
	s := rp.Stats()
	if s.Total != poolLowWater {
		t.Errorf("after promotion+prune total=%d, want %d", s.Total, poolLowWater)
	}
}

func TestPromote_UnknownPID(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	pid := newTestPid(t, 1)
	rp.RecordResult(pid, nil) // no-op, unknown pid
	assertLen(t, rp, 0)
}

func TestPromote_MainAlreadyInMain(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	pid := newTestPid(t, 1)
	rp.Add(testAddr(pid))
	rp.RecordResult(pid, nil) // promotes
	rp.RecordResult(pid, nil) // stays in main, score goes up
	assertStats(t, rp, 1, 1, 0)
}

// ────────────────────────── 3. Demotion ──────────────────────────

func TestDemote_MainConsecutiveFails(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	pid := newTestPid(t, 1)
	rp.Add(testAddr(pid))
	rp.RecordResult(pid, nil) // promote to main
	assertStats(t, rp, 1, 1, 0)

	// 3 consecutive failures should demote
	for i := 0; i < 2; i++ {
		rp.RecordResult(pid, errFailedErr())
	}
	assertStats(t, rp, 1, 1, 0) // still main after 2 fails

	rp.RecordResult(pid, errFailedErr())
	assertStats(t, rp, 1, 0, 1) // demoted after 3rd fail
}

func TestDemote_MainCircuitOpen(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	pid := newTestPid(t, 1)
	rp.Add(testAddr(pid))
	rp.RecordResult(pid, nil)

	for i := 0; i < 5; i++ {
		rp.RecordResult(pid, errFailedErr())
	}

	rp.mu.RLock()
	item := rp.items[pid]
	rp.mu.RUnlock()
	if item == nil {
		t.Fatal("item should exist")
	}
	if !item.circuitOpen {
		t.Error("circuit should be open after 5 consecutive failures")
	}
}

func TestDemote_ProbationFailsStayInProbation(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	pid := newTestPid(t, 1)
	rp.Add(testAddr(pid))

	for i := 0; i < 5; i++ {
		rp.RecordResult(pid, errFailedErr())
	}
	// probation items don't get demoted further — they stay in probation
	assertStats(t, rp, 1, 0, 1)
}

func TestDemote_RateLimitedDecay(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	pid := newTestPid(t, 1)
	rp.Add(testAddr(pid))
	rp.RecordResult(pid, nil) // promote

	rp.RecordResult(pid, errRateLimitedErr())

	rp.mu.RLock()
	item := rp.items[pid]
	rp.mu.RUnlock()
	if item.rateLimitHits != 1 {
		t.Errorf("rateLimitHits = %d, want 1", item.rateLimitHits)
	}
	// should NOT demote from rate limiting
	assertStats(t, rp, 1, 1, 0)
}

func TestDemote_RepromoteAfterDemotion(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	pid := newTestPid(t, 1)
	rp.Add(testAddr(pid))
	rp.RecordResult(pid, nil) // promote

	for i := 0; i < 3; i++ {
		rp.RecordResult(pid, errFailedErr())
	}
	assertStats(t, rp, 1, 0, 1) // demoted

	rp.RecordResult(pid, nil) // promote again
	assertStats(t, rp, 1, 1, 0)
}

// ────────────────────────── 4. Select ──────────────────────────

func TestSelect_EmptyPool(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	if addr := rp.Select(); addr != nil {
		t.Error("Select on empty pool should return nil")
	}
}

func TestSelect_ReturnsAddr(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	pid := newTestPid(t, 1)
	rp.Add(testAddr(pid))
	rp.RecordResult(pid, nil)

	addr := rp.Select()
	if addr == nil {
		t.Fatal("Select should return an addr")
	}
}

func TestSelect_AllCircuitOpen(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	pid := newTestPid(t, 1)
	rp.Add(testAddr(pid))
	rp.RecordResult(pid, nil)

	// force circuit open
	for i := 0; i < 5; i++ {
		rp.RecordResult(pid, errFailedErr())
	}
	if addr := rp.Select(); addr != nil {
		t.Error("Select with all circuitOpen should return nil")
	}
}

// ────────────────────────── 5. Prune Capacity ──────────────────────────

func TestPrune_UnderHighWater(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	for i := 0; i < poolHighWater-5; i++ {
		pid := newTestPid(t, i+1)
		rp.Add(testAddr(pid))
	}
	rp.prune()
	assertStats(t, rp, poolHighWater-5, 0, poolHighWater-5)
}

func TestPrune_BetweenLowAndHigh(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	for i := 0; i < poolHighWater; i++ {
		pid := newTestPid(t, i+1)
		rp.Add(testAddr(pid))
	}
	rp.prune()
	// exactly at highWater, no prune needed
	assertStats(t, rp, poolHighWater, 0, poolHighWater)
}

func TestPrune_ProbationEvictsOldestFIFO(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	for i := 0; i < poolHighWater; i++ {
		pid := newTestPid(t, i+1)
		rp.Add(testAddr(pid))
	}
	// all probation at highWater, add one more → prune evicts 11 oldest
	pidOld := newTestPid(t, 1)
	pidNew := newTestPid(t, 99)
	rp.Add(testAddr(pidNew))
	s := rp.Stats()
	if s.Total != poolLowWater {
		t.Errorf("total = %d, want %d", s.Total, poolLowWater)
	}
	rp.mu.RLock()
	_, hasOld := rp.items[pidOld]
	_, hasNew := rp.items[pidNew]
	rp.mu.RUnlock()
	if hasOld {
		t.Error("oldest probation (pid=1) should have been evicted")
	}
	if !hasNew {
		t.Error("newest item (pid=99) should survive")
	}
}

func TestPrune_MainEvictsWhenNotProtected(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	// fill to highWater with main items
	for i := 0; i < poolHighWater; i++ {
		pid := newTestPid(t, i+1)
		rp.Add(testAddr(pid))
		rp.RecordResult(pid, nil)
	}
	// backdate ALL main items so they're evictable (no recent-activity protection)
	for i := 0; i < poolHighWater; i++ {
		rp.mu.Lock()
		rp.items[newTestPid(t, i+1)].lastResult = time.Now().Add(-10 * time.Minute)
		rp.mu.Unlock()
	}
	// add one more → prune evicts 5 probation + 6 main → total=40
	pid99 := newTestPid(t, 99)
	rp.Add(testAddr(pid99))
	s := rp.Stats()
	if s.Total != poolLowWater {
		t.Errorf("total = %d, want %d", s.Total, poolLowWater)
	}
}

func TestPrune_ProbationShortfallShiftsToMain(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	// 5 probation + 45 main = 50 (highWater). Probation has fewer than need/2 items.
	for i := 0; i < 45; i++ {
		pid := newTestPid(t, i+1)
		rp.Add(testAddr(pid))
		rp.RecordResult(pid, nil)
	}
	// backdate 20 main items so after protection rounds we can evict enough from main
	// need=11, evictFromProb=5(probation only has 5→all 5 evicted→evictFromMain=11-5=6)
	for i := 0; i < 20; i++ {
		rp.mu.Lock()
		rp.items[newTestPid(t, i+1)].lastResult = time.Now().Add(-10 * time.Minute)
		rp.mu.Unlock()
	}
	for i := 45; i < 50; i++ {
		pid := newTestPid(t, i+1)
		rp.Add(testAddr(pid))
	}
	// add one more → prune
	pid99 := newTestPid(t, 99)
	rp.Add(testAddr(pid99))
	if s := rp.Stats(); s.Total != poolLowWater {
		t.Errorf("total = %d, want %d", s.Total, poolLowWater)
	}
}

func TestPrune_MainShortfallShiftsToProbation(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	// 40 main + 10 probation = 50 (highWater). Main is all protected (recent activity).
	// Probation covers full need via spill-back.
	for i := 0; i < 40; i++ {
		pid := newTestPid(t, i+1)
		rp.Add(testAddr(pid))
		rp.RecordResult(pid, nil)
	}
	for i := 40; i < 50; i++ {
		pid := newTestPid(t, i+1)
		rp.Add(testAddr(pid))
	}
	// add one more → prune: evictFromProb=5, evictFromMain=6 (all protected→spill to prob)
	pid99 := newTestPid(t, 99)
	rp.Add(testAddr(pid99))
	s := rp.Stats()
	if s.Total != poolLowWater {
		t.Errorf("total = %d, want %d", s.Total, poolLowWater)
	}
}

// ────────────────────────── 6. Prune Protection ──────────────────────────

func TestPrune_ProtectRecentActivity(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	// 40 main items, all with recent lastResult
	for i := 0; i < 40; i++ {
		pid := newTestPid(t, i+1)
		rp.Add(testAddr(pid))
		rp.RecordResult(pid, nil)
	}
	// add 11 probation (total = 51 > highWater, triggers prune)
	for i := 40; i < 51; i++ {
		pid := newTestPid(t, i+1)
		rp.Add(testAddr(pid))
	}
	// Since all main items have recent lastResult, they're all protected.
	// Only probation items get evicted.
	s := rp.Stats()
	if s.Total < 40 {
		t.Errorf("protected items should not be evicted: total=%d", s.Total)
	}
}

func TestPrune_ProtectTopScore(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	// 40 main items, all with old lastResult (eligible for eviction)
	for i := 0; i < 40; i++ {
		pid := newTestPid(t, i+1)
		rp.Add(testAddr(pid))
		rp.RecordResult(pid, nil)
		// backdate lastResult to make them eligible for eviction
		rp.mu.Lock()
		rp.items[pid].lastResult = time.Now().Add(-10 * time.Minute)
		rp.mu.Unlock()
	}
	for i := 40; i < 52; i++ {
		pid := newTestPid(t, i+1)
		rp.Add(testAddr(pid))
	}
	rp.mu.Lock()
	rp.highWater = 30
	rp.mu.Unlock()
	// force prune
	rp.prune()
	// some top-scorers should survive
	s := rp.Stats()
	if s.Total == 0 {
		t.Error("top scorers should be protected")
	}
	rp.mu.Lock()
	rp.highWater = poolHighWater
	rp.mu.Unlock()
}

func TestPrune_ProtectProtectedMap(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	pid := newTestPid(t, 1)
	rp.Add(testAddr(pid))
	rp.RecordResult(pid, nil)
	rp.Protect(pid)

	// fill to exceed highWater
	for i := 1; i < poolHighWater+5; i++ {
		pid2 := newTestPid(t, i+100)
		rp.Add(testAddr(pid2))
		rp.RecordResult(pid2, nil)
	}

	// protected item should survive
	rp.mu.RLock()
	_, ok := rp.items[pid]
	rp.mu.RUnlock()
	if !ok {
		t.Error("protected item should not be evicted")
	}
}

func TestPrune_ProtectLongUptime(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	// create 1 main item with old lastResult (eligible) but long uptime
	// backdate connectedSince to simulate long uptime
	oldPid := newTestPid(t, 1)
	rp.Add(testAddr(oldPid))
	rp.RecordResult(oldPid, nil)
	rp.mu.Lock()
	rp.items[oldPid].lastResult = time.Now().Add(-10 * time.Minute)
	rp.items[oldPid].connectedSince = time.Now().Add(-10 * time.Minute)
	rp.mu.Unlock()

	// fill more main items (all evictable, recent) to exceed highWater
	for i := 0; i < poolHighWater+5; i++ {
		pid := newTestPid(t, i+10)
		rp.Add(testAddr(pid))
		rp.RecordResult(pid, nil)
	}
	// Backdate the lastResult of these new items too so they don't get round-1 protection.
	// We want to test uptime protection specifically.
	rp.mu.RLock()
	var pids []peer.ID
	for pid := range rp.items {
		pids = append(pids, pid)
	}
	rp.mu.RUnlock()
	for _, pid := range pids {
		if pid == oldPid {
			continue
		}
		rp.mu.Lock()
		if item, ok := rp.items[pid]; ok {
			item.lastResult = time.Now().Add(-10 * time.Minute)
		}
		rp.mu.Unlock()
	}
	rp.prune()
	// old item should survive via uptime protection (top-10 by connectedSince)
	rp.mu.RLock()
	_, ok := rp.items[oldPid]
	rp.mu.RUnlock()
	if !ok {
		t.Error("long uptime item should be protected from eviction")
	}
}

// ────────────────────────── 7. EvictOne (Add-time eviction) ──────────────────────────

func TestEvictOne_ProbationFirst(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	for i := 0; i < poolHighWater; i++ {
		pid := newTestPid(t, i+1)
		rp.Add(testAddr(pid))
	}
	// all probation at highWater: add more to trigger prune (FIFO eviction)
	pid99 := newTestPid(t, 99)
	rp.Add(testAddr(pid99))
	// prune should bring us to lowWater
	s := rp.Stats()
	if s.Total != poolLowWater {
		t.Errorf("total = %d, want %d", s.Total, poolLowWater)
	}
}

func TestEvictOne_MainLowestScore(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	// fill with main items (highWater=50, all probation → main)
	for i := 0; i < poolHighWater; i++ {
		pid := newTestPid(t, i+1)
		rp.Add(testAddr(pid))
		rp.RecordResult(pid, nil)
	}
	// Backdate all main items so they're evictable (no recent-activity protection)
	for i := 0; i < poolHighWater; i++ {
		rp.mu.Lock()
		rp.items[newTestPid(t, i+1)].lastResult = time.Now().Add(-10 * time.Minute)
		rp.mu.Unlock()
	}
	// add just one more to trigger prune to lowWater
	// total=51, need=11, evictFromProb=5, evictFromMain=6
	// 50 main candidates→round2 topK=10→40→round3 uptimeK=10→30→evict 6
	// wait: probation is pid100 (1 item). evictFromProb=5→shortfall→evictFromMain+=4=10
	// Probation evict 1, main evict 10. Total = 51-1-10=40
	// But actually probation is pid100 + no other. Probation shortfall means main covers.
	pid100 := newTestPid(t, 100)
	rp.Add(testAddr(pid100))
	rp.RecordResult(pid100, nil)
	s := rp.Stats()
	if s.Total != poolLowWater {
		t.Errorf("total = %d, want %d (1 added + 5 more to follow would be 44)", s.Total, poolLowWater)
	}
}

func TestEvictOne_AllProtected(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	// fill main with protected items (no probation)
	for i := 0; i < poolHighWater; i++ {
		pid := newTestPid(t, i+1)
		rp.Add(testAddr(pid))
		rp.RecordResult(pid, nil) // promote to main
		rp.Protect(pid)
	}
	// add more past highWater — each new item lands in probation,
	// prune evicts it immediately (probation FIFO), only protected main survives
	for i := 0; i < 5; i++ {
		pid := newTestPid(t, 100+i)
		rp.Add(testAddr(pid))
		rp.RecordResult(pid, nil)
	}
	// All protected main items survive; new items get evicted by prune
	s := rp.Stats()
	if s.Total != poolHighWater {
		t.Errorf("total=%d, want %d (protected main items survive, new items evicted)",
			s.Total, poolHighWater)
	}
	// verify all protected items still present
	for i := 0; i < poolHighWater; i++ {
		rp.mu.RLock()
		_, ok := rp.items[newTestPid(t, i+1)]
		rp.mu.RUnlock()
		if !ok {
			t.Errorf("protected pid=%d should survive", i+1)
		}
	}
}

// ────────────────────────── 8. Remove / Protect / Unprotect ──────────────────────────

func TestRemove(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	pid := newTestPid(t, 1)
	rp.Add(testAddr(pid))
	rp.Remove(pid)
	assertLen(t, rp, 0)
}

func TestProtectPreventsEvictionInMain(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	pid := newTestPid(t, 1)
	rp.Add(testAddr(pid))
	rp.RecordResult(pid, nil)
	rp.Protect(pid)
	// fill and prune
	for i := 0; i < poolHighWater; i++ {
		pid2 := newTestPid(t, i+2)
		rp.Add(testAddr(pid2))
		rp.RecordResult(pid2, nil)
	}
	// protected item should survive
	rp.mu.RLock()
	_, ok := rp.items[pid]
	rp.mu.RUnlock()
	if !ok {
		t.Error("protected item should survive prune")
	}
}

func TestUnprotect(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	pid := newTestPid(t, 1)
	rp.Add(testAddr(pid))
	rp.RecordResult(pid, nil)
	rp.Protect(pid)
	rp.Unprotect(pid)

	rp.mu.Lock()
	protected := rp.protected[pid]
	rp.mu.Unlock()
	if protected {
		t.Error("item should no longer be protected after Unprotect")
	}
}

// ────────────────────────── 9. Stats / Edge ──────────────────────────

func TestStats_Basic(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	s := rp.Stats()
	if s.Total != 0 || s.AvgScore != 0 {
		t.Errorf("empty stats: %+v", s)
	}

	pid := newTestPid(t, 1)
	rp.Add(testAddr(pid))
	s = rp.Stats()
	if s.Total != 1 || s.ProbationCnt != 1 || s.MainCnt != 0 {
		t.Errorf("after add: %+v", s)
	}

	rp.RecordResult(pid, nil)
	s = rp.Stats()
	if s.MainCnt != 1 || s.ProbationCnt != 0 {
		t.Errorf("after promote: %+v", s)
	}
}

func TestStats_CircuitOpen(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	pid := newTestPid(t, 1)
	rp.Add(testAddr(pid))
	// Stay in probation (never promote) — 5 consecutive failures opens the circuit
	// without demotion interference (demotion only applies when inMain=true).
	for i := 0; i < 5; i++ {
		rp.RecordResult(pid, errFailedErr())
	}
	s := rp.Stats()
	if s.CircuitOpen != 1 {
		t.Errorf("CircuitOpen = %d, want 1", s.CircuitOpen)
	}
	if s.Healthy != 0 {
		t.Errorf("Healthy = %d, want 0", s.Healthy)
	}
}

func TestSetWeight(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	rp.SetWeight(0, 0.5)
	rp.mu.RLock()
	v := rp.config.Success
	rp.mu.RUnlock()
	if v != 0.5 {
		t.Errorf("Success weight = %f, want 0.5", v)
	}
}

func TestSetWeights(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	w := WeightConfig{Success: 0.5, Latency: 0.3, Uptime: 0.2}
	rp.SetWeights(w)
	rp.mu.RLock()
	if rp.config.Success != 0.5 || rp.config.Latency != 0.3 || rp.config.Uptime != 0.2 {
		t.Errorf("weights not set correctly: %+v", rp.config)
	}
	rp.mu.RUnlock()
}

func TestDefaultWeights(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	if rp.config.Success != defaultWeights.Success {
		t.Error("zero config should fall back to defaults")
	}
}

func TestSetRelayLimits(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	pid := newTestPid(t, 1)
	rp.Add(testAddr(pid))
	rp.SetRelayLimits(pid, 5*time.Minute, 65536)
	rp.mu.RLock()
	item := rp.items[pid]
	rp.mu.RUnlock()
	if item.limitDuration != 5*time.Minute {
		t.Errorf("limitDuration = %v, want 5m", item.limitDuration)
	}
	if item.limitData != 65536 {
		t.Errorf("limitData = %d, want 65536", item.limitData)
	}
}

func TestSetRelayLimits_UnknownPID(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	pid := newTestPid(t, 1)
	rp.SetRelayLimits(pid, 5*time.Minute, 65536) // no-op, no panic
}

func TestSetReservationTTL(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	pid := newTestPid(t, 1)
	rp.Add(testAddr(pid))
	expires := time.Now().Add(10 * time.Minute)
	rp.SetReservationTTL(pid, expires)
	rp.mu.RLock()
	item := rp.items[pid]
	rp.mu.RUnlock()
	if !item.reservationExpires.Equal(expires) {
		t.Errorf("reservationExpires = %v, want %v", item.reservationExpires, expires)
	}
}

func TestSetReservationTTL_UnknownPID(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	rp.SetReservationTTL(newTestPid(t, 1), time.Now()) // no-op, no panic
}

func TestConcurrent_AddAndSelect(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			pid := newTestPid(t, (i%50)+1)
			rp.Add(testAddr(pid))
		}
		close(done)
	}()
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				rp.Select()
			}
		}
	}()
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				rp.Stats()
			}
		}
	}()
	<-done
}

func TestConcurrent_RecordResultRace(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	var pids []peer.ID
	for i := 0; i < 10; i++ {
		pid := newTestPid(t, i+1)
		pids = append(pids, pid)
		rp.Add(testAddr(pid))
	}
	done := make(chan struct{})
	go func() {
		for i := 0; i < 100; i++ {
			rp.RecordResult(pids[i%10], nil)
		}
		close(done)
	}()
	go func() {
		for i := 0; i < 100; i++ {
			rp.RecordResult(pids[i%10], errFailedErr())
		}
	}()
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				rp.Select()
			}
		}
	}()
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				rp.Stats()
			}
		}
	}()
	<-done
}

// ────────────────────────── 9. SelectN ──────────────────────────

func TestSelectN_Basic(t *testing.T) {
	rp := NewRelayPool(WeightConfig{Success: 1})
	p1, p2, p3, p4 := newTestPid(t, 1), newTestPid(t, 2), newTestPid(t, 3), newTestPid(t, 4)
	rp.Add(testAddr(p1))
	rp.Add(testAddr(p2))
	rp.Add(testAddr(p3))
	rp.Add(testAddr(p4))

	rp.RecordResult(p1, nil) // main, score=0.91 (ema from 0.70)
	rp.RecordResult(p2, nil)
	rp.RecordResult(p3, nil)
	rp.RecordResult(p4, errFailedErr()) // probation, circuitOpen

	infos := rp.SelectN(2)
	if len(infos) != 2 {
		t.Fatalf("SelectN(2) = %d, want 2", len(infos))
	}
	// p4 is circuitOpen, should not appear
	for _, ai := range infos {
		if ai.ID == p4 {
			t.Errorf("SelectN includes circuitOpen relay %s", p4.ShortString())
		}
	}
}

func TestSelectN_Zero(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	infos := rp.SelectN(2)
	if len(infos) != 0 {
		t.Errorf("SelectN on empty pool = %d, want empty", len(infos))
	}
}

func TestSelectN_OverCount(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	p1, p2 := newTestPid(t, 1), newTestPid(t, 2)
	rp.Add(testAddr(p1))
	rp.Add(testAddr(p2))

	infos := rp.SelectN(5)
	if len(infos) != 2 {
		t.Errorf("SelectN(5) = %d, want 2", len(infos))
	}
}

// ────────────────────────── 10. AddManaged ──────────────────────────

func TestAddManaged_Basic(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	p1, p2 := newTestPid(t, 1), newTestPid(t, 2)

	rp.AddManaged(p1)
	rp.AddManaged(p2)

	got := rp.ListManaged()
	if len(got) != 2 {
		t.Fatalf("ListManaged = %d, want 2", len(got))
	}

	rp.RemoveManaged(p1)
	got = rp.ListManaged()
	if len(got) != 1 {
		t.Fatalf("after remove = %d, want 1", len(got))
	}
	if got[0] != p2 {
		t.Errorf("remaining = %s, want %s", got[0].ShortString(), p2.ShortString())
	}
}

func TestAddManaged_RemoveUnknown(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	p1, p2 := newTestPid(t, 1), newTestPid(t, 2)
	rp.AddManaged(p1)
	rp.RemoveManaged(p2) // not added, should not panic
	got := rp.ListManaged()
	if len(got) != 1 {
		t.Errorf("after remove unknown = %d, want 1", len(got))
	}
}

// ────────────────────────── 11. IsCircuitOpen ──────────────────────────

func TestIsCircuitOpen(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	pid := newTestPid(t, 1)
	rp.Add(testAddr(pid))

	if rp.IsCircuitOpen(pid) {
		t.Error("expected false for new relay")
	}

	for i := 0; i < 5; i++ {
		rp.RecordResult(pid, errFailedErr())
	}

	if !rp.IsCircuitOpen(pid) {
		t.Error("expected true after 5 consecutive failures")
	}
}

func TestIsCircuitOpen_Unknown(t *testing.T) {
	rp := NewRelayPool(WeightConfig{})
	pid := newTestPid(t, 99)
	if rp.IsCircuitOpen(pid) {
		t.Error("expected false for unknown relay")
	}
}

// ────────────────────────── Helpers ──────────────────────────

func errFailedErr() error {
	return errors.New("test failure")
}

func errRateLimitedErr() error {
	return network.ErrReset
}
