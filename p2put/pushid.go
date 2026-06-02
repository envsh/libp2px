package p2put

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/protocol/identify"
	pb "github.com/libp2p/go-libp2p/p2p/protocol/identify/pb"
	"github.com/libp2p/go-msgio"
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

type PeerInfo struct {
	ID     string   `json:"id"`
	Addrs  []string `json:"addrs"`
	SeenAt int64    `json:"ts,omitempty"`
}

type PushMessage struct {
	Peers []PeerInfo `json:"peers"`
	TS    int64      `json:"ts"`
}

const (
	maxPushLen  = 1 * 1024 * 1024
	pushTimeout = 10 * time.Second
)

// listen /d2hub/push/1.0
// limited custom push handler
func HandlePushStream(s network.Stream) {
	defer s.Close()
	pid := s.Conn().RemotePeer()
	log.Printf("[push] incoming from %s", pid.ShortString())

	s.SetReadDeadline(time.Now().Add(pushTimeout))
	rd := msgio.NewVarintReaderSize(s, maxPushLen)
	raw, err := rd.ReadMsg()
	if err != nil {
		n, _ := rd.NextMsgLen()
		log.Printf("[push] read from %s (len=%d): %v", pid.ShortString(), n, err)
		return
	}
	var req PushMessage
	if err := json.Unmarshal(raw, &req); err != nil {
		log.Printf("[push] unmarshal from %s: %v", pid.ShortString(), err)
		return
	}

	for _, p := range req.Peers {
		rpid, err := peer.Decode(p.ID)
		if err != nil {
			continue
		}
		if rpid == bootres.Host.ID() {
			continue
		}
		addrs := make([]multiaddr.Multiaddr, 0, len(p.Addrs))
		for _, s := range p.Addrs {
			m, err := multiaddr.NewMultiaddr(s)
			if err != nil {
				continue
			}
			addrs = append(addrs, m)
		}
		bootres.PeerDB.Update(rpid, addrs, time.Now())
	}
	log.Println("[limitpx] addup peer curr/total", len(req.Peers), len(bootres.PeerDB.List()))

	records := bootres.PeerDB.List()
	resp := PushMessage{TS: time.Now().UnixMilli()}
	for _, r := range records {
		if r.PeerID == bootres.Host.ID() {
			continue
		}
		if r.PeerID == pid {
			continue
		}
		strs := make([]string, len(r.Addrs))
		for i, a := range r.Addrs {
			strs[i] = a.String()
		}
		resp.Peers = append(resp.Peers, PeerInfo{
			ID:     r.PeerID.String(),
			Addrs:  strs,
			SeenAt: r.SeenAt.UnixMilli(),
		})
	}

	out, err := json.Marshal(resp)
	if err != nil {
		log.Printf("[push] marshal: %v", err)
		return
	}
	s.SetWriteDeadline(time.Now().Add(pushTimeout))
	wr := msgio.NewVarintWriter(s)
	if err := wr.WriteMsg(out); err != nil {
		log.Printf("[push] write to %s (len=%d): %v", pid.ShortString(), len(out), err)
		return
	}
	s.CloseWrite()
	log.Printf("[push] %d peers sent to %s", len(resp.Peers), pid.ShortString())
}

// send /d2hub/push/1.0
func PushToPeer(ctx context.Context, pid peer.ID) error {
	addrs := bootres.AddrMgr.GetAll()
	req := PushMessage{
		Peers: []PeerInfo{{
			ID:    bootres.Host.ID().String(),
			Addrs: addrStrings(addrs),
		}},
		TS: time.Now().UnixMilli(),
	}

	ctx = network.WithAllowLimitedConn(ctx, "push/1.0")
	s, err := bootres.Host.NewStream(ctx, pid, LimitedPxProtocol)
	if err != nil {
		log.Printf("[push] newstream %s: %v", pid.ShortString(), err)
		return err
	}
	defer s.Close()

	out, err := json.Marshal(req)
	if err != nil {
		log.Printf("[push] marshal: %v", err)
		s.Reset()
		return err
	}
	wr := msgio.NewVarintWriter(s)
	if err := wr.WriteMsg(out); err != nil {
		log.Printf("[push] write to %s (len=%d): %v", pid.ShortString(), len(out), err)
		return err
	}
	s.CloseWrite()

	s.SetReadDeadline(time.Now().Add(pushTimeout))
	rd := msgio.NewVarintReaderSize(s, maxPushLen)
	raw, err := rd.ReadMsg()
	if err != nil {
		n, _ := rd.NextMsgLen()
		log.Printf("[push] read from %s (len=%d): %v", pid.ShortString(), n, err)
		return err
	}
	var resp PushMessage
	if err := json.Unmarshal(raw, &resp); err != nil {
		log.Printf("[push] unmarshal from %s: %v", pid.ShortString(), err)
		return err
	}

	ids := []string{}
	for _, p := range resp.Peers {
		rpid, err := peer.Decode(p.ID)
		_ = err
		ids = append(ids, strings.Split(rpid.ShortString(), " ")[1])
	}
	log.Printf("[push] got %d peers from %s, ids %v", len(resp.Peers), pid.ShortString(), ids)

	for _, p := range resp.Peers {
		rpid, err := peer.Decode(p.ID)
		if err != nil {
			continue
		}
		if rpid == bootres.Host.ID() {
			continue
		}
		addrs := make([]multiaddr.Multiaddr, 0, len(p.Addrs))
		for _, s := range p.Addrs {
			m, err := multiaddr.NewMultiaddr(s)
			if err != nil {
				continue
			}
			addrs = append(addrs, m)
		}
		seenAt := time.Now()
		if p.SeenAt > 0 {
			seenAt = time.UnixMilli(p.SeenAt)
		}
		bootres.PeerDB.Update(rpid, addrs, seenAt)
	}

	return nil
}

func addrStrings(addrs []multiaddr.Multiaddr) []string {
	s := make([]string, len(addrs))
	for i, a := range addrs {
		s[i] = a.String()
	}
	return s
}

func parseAddrs(strs []string) []multiaddr.Multiaddr {
	addrs := make([]multiaddr.Multiaddr, 0, len(strs))
	for _, s := range strs {
		m, err := multiaddr.NewMultiaddr(s)
		if err == nil {
			addrs = append(addrs, m)
		}
	}
	return addrs
}

func init() {
	MustRegisterProtocol("push/1.0", HandlePushStream)
}

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
