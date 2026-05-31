package p2put

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
)

type OfflineState int

const (
	StateOffline OfflineState = iota
	StateOnline
)

func (s OfflineState) String() string {
	switch s {
	case StateOnline:
		return "online"
	default:
		return "offline"
	}
}

const (
	evaluateTick = 1500 * time.Millisecond
	stickCount   = 3
	confirmDur   = evaluateTick * stickCount
)

func IsOnline() bool {
	if bootres == nil || bootres.OfflineDetector == nil {
		return false
	}
	return bootres.OfflineDetector.isOnline()
}

func SetOnOfflineChange(fn func(state OfflineState, reason string)) {
	if bootres != nil && bootres.OfflineDetector != nil {
		bootres.OfflineDetector.OnChange = fn
	}
}

type OfflineDetector struct {
	h  host.Host
	rp *RelayPool

	mu sync.Mutex

	state              OfflineState
	lastTransitionTime time.Time
	goodSince          time.Time
	badSince           time.Time

	OnChange func(state OfflineState, reason string)
}

func NewOfflineDetector(h host.Host, rp *RelayPool) *OfflineDetector {
	now := time.Now()
	return &OfflineDetector{
		h:                  h,
		rp:                 rp,
		state:              StateOffline,
		lastTransitionTime: now,
	}
}

func (d *OfflineDetector) Run(ctx context.Context) {
	ticker := time.NewTicker(evaluateTick)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			d.evaluate()
		case <-ctx.Done():
			return
		}
	}
}

func (d *OfflineDetector) evaluate() {
	d.mu.Lock()
	defer d.mu.Unlock()

	active := 0
	for _, conn := range d.h.Network().Conns() {
		if conn.Stat().Direction == network.DirOutbound {
			active++
			break
		}
	}
	for _, conn := range d.h.Network().Conns() {
		if conn.Stat().Direction == network.DirInbound {
			active++
			break
		}
	}

	now := time.Now()
	switch {
	case active > 0:
		if d.goodSince.IsZero() {
			d.goodSince = now
		}
		d.badSince = time.Time{}
	default:
		if d.badSince.IsZero() {
			d.badSince = now
		}
		d.goodSince = time.Time{}
	}

	target := d.state
	if !d.goodSince.IsZero() && now.Sub(d.goodSince) >= confirmDur {
		target = StateOnline
	} else if !d.badSince.IsZero() && now.Sub(d.badSince) >= confirmDur {
		target = StateOffline
	}

	if target != d.state {
		dur := now.Sub(d.lastTransitionTime).Truncate(time.Second)
		oldState := d.state
		d.state = target
		d.lastTransitionTime = now
		log.Printf("[offline] %s→%s (lasted %v, active=%d)", oldState, target, dur, active)
		if d.OnChange != nil {
			d.OnChange(target, target.String())
		}
	}
}

func (d *OfflineDetector) isOnline() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.state == StateOnline
}
