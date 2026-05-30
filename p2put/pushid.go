package p2put

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/protocol/identify"
	pb "github.com/libp2p/go-libp2p/p2p/protocol/identify/pb"
	"github.com/libp2p/go-msgio/pbio"
	"github.com/multiformats/go-multiaddr"
)

func pushMyAddrsToPeers(ctx context.Context, h host.Host) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			log.Printf("[pushAddrs] pushing to all connected peers")
			pushToAll(ctx, h)
		case <-ctx.Done():
			return
		}
	}
}

func pushToAll(ctx context.Context, h host.Host) {
	addrs := h.Addrs()
	seen := make(map[peer.ID]struct{})
	for _, conn := range h.Network().Conns() {
		pid := conn.RemotePeer()
		if _, ok := seen[pid]; ok {
			continue
		}
		seen[pid] = struct{}{}
		pushToConnected(ctx, h, pid, addrs)
	}
}

func pushToConnected(ctx context.Context, h host.Host, pid peer.ID, addrs []multiaddr.Multiaddr) {
	pubkey := h.Peerstore().PubKey(h.ID())
	if pubkey == nil {
		log.Printf("[pushAddrs] pubkey not found")
		return
	}
	pubkeyBytes, err := crypto.MarshalPublicKey(pubkey)
	if err != nil {
		log.Printf("[pushAddrs] marshal: %v", err)
		return
	}

	s, err := h.NewStream(ctx, pid, identify.IDPush)
	if err != nil {
		log.Printf("[pushAddrs] newstream %s: %v", pid.ShortString(), err)
		return
	}
	defer s.Close()

	rawAddrs := make([][]byte, len(addrs))
	for i, a := range addrs {
		rawAddrs[i] = a.Bytes()
	}

	mes := &pb.Identify{
		PublicKey:   pubkeyBytes,
		Protocols:   protocol.ConvertToStrings(h.Mux().Protocols()),
		ListenAddrs: rawAddrs,
	}
	wr := pbio.NewDelimitedWriter(s)
	if err := wr.WriteMsg(mes); err != nil {
		s.Reset()
		log.Printf("[pushAddrs] write %s: %v", pid.ShortString(), err)
		return
	}
	log.Printf("[pushAddrs] %v pushed to %s", len(addrs), pid.ShortString())
}

const LimitedPxProtocol = protocol.ID("/d2hub/push/1.0")

func queryObservedAddr(ctx context.Context, h host.Host, target peer.ID) ([]multiaddr.Multiaddr, error) {
	s, err := h.NewStream(ctx, target, identify.ID)
	if err != nil {
		return nil, fmt.Errorf("newstream: %w", err)
	}
	defer s.Close()

	var mes pb.Identify
	rd := pbio.NewDelimitedReader(s, 2048)
	if err := rd.ReadMsg(&mes); err != nil {
		s.Reset()
		return nil, fmt.Errorf("read: %w", err)
	}

	var addrs []multiaddr.Multiaddr
	if mes.ObservedAddr != nil {
		m, err := multiaddr.NewMultiaddrBytes(mes.ObservedAddr)
		if err == nil {
			addrs = append(addrs, m)
		}
	}
	log.Printf("[queryObservedAddr] from %s: %s", target.ShortString(), addrs)
	return addrs, nil
}
