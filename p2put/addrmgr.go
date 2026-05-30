package p2put

import (
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

type relayVouch struct {
	addrs      []multiaddr.Multiaddr
	expiration time.Time
}

type AddrManager struct {
	mu                sync.RWMutex
	localAddrs        []multiaddr.Multiaddr
	relayCircuitAddrs []multiaddr.Multiaddr
	relayVouches      map[peer.ID]*relayVouch
}

func NewAddrManager() *AddrManager {
	return &AddrManager{
		relayVouches: make(map[peer.ID]*relayVouch),
	}
}

func (m *AddrManager) SetLocal(addrs []multiaddr.Multiaddr) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.localAddrs = addrs
}

func (m *AddrManager) SetRelayCircuit(addrs []multiaddr.Multiaddr) {
	if len(addrs) == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.relayCircuitAddrs = addrs
}

func (m *AddrManager) SetRelayVouch(id peer.ID, addrs []multiaddr.Multiaddr, expiry time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.relayVouches[id] = &relayVouch{
		addrs:      addrs,
		expiration: expiry,
	}
}

func (m *AddrManager) GetAll() []multiaddr.Multiaddr {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := m.localAddrs

	var validCircuits []multiaddr.Multiaddr
	for _, a := range m.relayCircuitAddrs {
		relayID, err := extractRelayPeerID(a)
		if err != nil {
			validCircuits = append(validCircuits, a)
			continue
		}
		v, ok := m.relayVouches[relayID]
		if !ok || time.Now().Before(v.expiration) {
			validCircuits = append(validCircuits, a)
		}
	}
	m.relayCircuitAddrs = validCircuits
	out = mergeAddrs(out, m.relayCircuitAddrs)

	for id, v := range m.relayVouches {
		if time.Now().Before(v.expiration) {
			out = mergeAddrs(out, v.addrs)
		} else {
			delete(m.relayVouches, id)
		}
	}
	return out
}
