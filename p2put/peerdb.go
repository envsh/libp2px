package p2put

import (
	"context"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

type PeerRecord struct {
	PeerID peer.ID
	Addrs  []multiaddr.Multiaddr
	SeenAt time.Time
}

type PeerDB struct {
	mu    sync.RWMutex
	peers map[peer.ID]*PeerRecord
	ttl   time.Duration
}

func NewPeerDB(ttl time.Duration) *PeerDB {
	return &PeerDB{
		peers: make(map[peer.ID]*PeerRecord),
		ttl:   ttl,
	}
}

func (db *PeerDB) Update(pid peer.ID, addrs []multiaddr.Multiaddr) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.peers[pid] = &PeerRecord{
		PeerID: pid,
		Addrs:  addrs,
		SeenAt: time.Now(),
	}
}

func (db *PeerDB) Get(pid peer.ID) (*PeerRecord, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	r, ok := db.peers[pid]
	if !ok {
		return nil, false
	}
	if time.Since(r.SeenAt) > db.ttl {
		return nil, false
	}
	return r, true
}

func (db *PeerDB) List() []PeerRecord {
	db.mu.RLock()
	defer db.mu.RUnlock()
	out := make([]PeerRecord, 0, len(db.peers))
	now := time.Now()
	for _, r := range db.peers {
		if now.Sub(r.SeenAt) > db.ttl {
			continue
		}
		out = append(out, *r)
	}
	return out
}

func (db *PeerDB) cleanup(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			db.mu.Lock()
			now := time.Now()
			for pid, r := range db.peers {
				if now.Sub(r.SeenAt) > db.ttl {
					delete(db.peers, pid)
				}
			}
			db.mu.Unlock()
		case <-ctx.Done():
			return
		}
	}
}
