package p2put

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/net/swarm"
	"log"
	"time"
)

const baseProtocolPath = "/d2hub/"

var (
	regMu    sync.RWMutex
	registry = make(map[protocol.ID]network.StreamHandler)
)

func fullProtoID(name string) protocol.ID {
	name = strings.TrimPrefix(name, "/")
	return protocol.ID(baseProtocolPath + name)
}

func RegisterProtocol(name string, handler network.StreamHandler, wildcard ...bool) error {
	isWildcard := len(wildcard) > 0 && wildcard[0]
	pid := fullProtoID(name)
	if isWildcard {
		if idx := strings.LastIndex(name, "/"); idx > 0 {
			pid = fullProtoID(name[:idx+1])
		} else {
			return fmt.Errorf("wildcard protocol %q must contain '/' to determine base prefix", name)
		}
	}

	regMu.Lock()
	defer regMu.Unlock()
	if _, exists := registry[pid]; exists {
		return fmt.Errorf("protocol %q already registered", pid)
	}
	registry[pid] = handler

	if bootres.Host != nil {
		if isWildcard {
			pidStr := string(pid)
			bootres.Host.SetStreamHandlerMatch(pid, func(proto protocol.ID) bool {
				return strings.HasPrefix(string(proto), pidStr)
			}, handler)
		} else {
			bootres.Host.SetStreamHandler(pid, handler)
		}
	}
	return nil
}

func MustRegisterProtocol(name string, handler network.StreamHandler, wildcard ...bool) {
	if err := RegisterProtocol(name, handler, wildcard...); err != nil {
		panic(err)
	}
}

func UnregisterProtocol(name string) {
	pid := fullProtoID(name)
	regMu.Lock()
	defer regMu.Unlock()
	delete(registry, pid)
	if bootres.Host != nil {
		bootres.Host.RemoveStreamHandler(pid)
	}
}

func withBackoffBypass(ctx context.Context, h host.Host, p peer.ID) context.Context {
	if h.Network().Connectedness(p) == network.Connected {
		return ctx
	}
	if rem := swarmBackoffRemaining(h, p); rem > 0 {
		log.Printf("[bypass] %s backoff remaining %v, force direct dial", p.ShortString(), rem.Round(time.Millisecond))
		return network.WithForceDirectDial(ctx, "bypass-backoff")
	}
	return ctx
}

func swarmBackoffRemaining(h host.Host, p peer.ID) time.Duration {
	s, ok := h.Network().(*swarm.Swarm)
	if !ok {
		return 0
	}
	var maxRem time.Duration
	for _, addr := range h.Peerstore().Addrs(p) {
		if rem := s.Backoff().BackoffRemaining(p, addr); rem > maxRem {
			maxRem = rem
		}
	}
	return maxRem
}

func OpenStream(ctx context.Context, peerIDStr string, name string) (network.Stream, error) {
	p, err := peer.Decode(peerIDStr)
	if err != nil {
		return nil, fmt.Errorf("decode peer id: %w", err)
	}
	if bootres == nil || bootres.Host == nil {
		return nil, fmt.Errorf("host not ready")
	}
	ctx = network.WithAllowLimitedConn(ctx, name+"/force-relay")
	ctx = withBackoffBypass(ctx, bootres.Host, p)
	return bootres.Host.NewStream(ctx, p, fullProtoID(name))
}

func OpenStreamDirect(ctx context.Context, peerIDStr string, name string) (network.Stream, error) {
	p, err := peer.Decode(peerIDStr)
	if err != nil {
		return nil, fmt.Errorf("decode peer id: %w", err)
	}
	if bootres == nil || bootres.Host == nil {
		return nil, fmt.Errorf("host not ready")
	}
	ctx = withBackoffBypass(ctx, bootres.Host, p)
	return bootres.Host.NewStream(ctx, p, fullProtoID(name))
}

func replayProtocols(h host.Host) {
	regMu.RLock()
	defer regMu.RUnlock()
	for pid, handler := range registry {
		pidStr := string(pid)
		if strings.HasSuffix(pidStr, "/") {
			h.SetStreamHandlerMatch(pid, func(proto protocol.ID) bool {
				return strings.HasPrefix(string(proto), pidStr)
			}, handler)
		} else {
			h.SetStreamHandler(pid, handler)
		}
	}
}
