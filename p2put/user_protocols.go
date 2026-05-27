package p2put

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
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

func RegisterProtocol(name string, handler network.StreamHandler) error {
	pid := fullProtoID(name)
	regMu.Lock()
	defer regMu.Unlock()
	if _, exists := registry[pid]; exists {
		return fmt.Errorf("protocol %q already registered", pid)
	}
	registry[pid] = handler
	if bootres != nil && bootres.Host != nil {
		bootres.Host.SetStreamHandler(pid, handler)
	}
	return nil
}

func MustRegisterProtocol(name string, handler network.StreamHandler) {
	if err := RegisterProtocol(name, handler); err != nil {
		panic(err)
	}
}

func UnregisterProtocol(name string) {
	pid := fullProtoID(name)
	regMu.Lock()
	defer regMu.Unlock()
	delete(registry, pid)
	if bootres != nil && bootres.Host != nil {
		bootres.Host.RemoveStreamHandler(pid)
	}
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
	return bootres.Host.NewStream(ctx, p, fullProtoID(name))
}

func replayProtocols() {
	regMu.RLock()
	defer regMu.RUnlock()
	for pid, handler := range registry {
		bootres.Host.SetStreamHandler(pid, handler)
	}
}
