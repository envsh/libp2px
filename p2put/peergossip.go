package p2put

import (
	"context"
	"encoding/json"
	"log"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/multiformats/go-multiaddr"
)

const publishInterval = 60 * time.Second

type PeerAnnounce struct {
	PeerID string   `json:"peer_id"`
	Addrs  []string `json:"addrs"`
	Seq    uint64   `json:"seq"`
	TS     int64    `json:"ts"`
}

type PeerGossip struct {
	host      host.Host
	ps        *pubsub.PubSub
	topic     *pubsub.Topic
	sub       *pubsub.Subscription
	db        *PeerDB
	topicName string

	seq   uint64
	// 存储 []multiaddr.Multiaddr，当前知晓的本机地址列表，由 onEvent 更新，供 publish 使用
	addrs atomic.Value
}

func NewPeerGossip(h host.Host, ps *pubsub.PubSub, db *PeerDB, topicName string) *PeerGossip {
	return &PeerGossip{
		host:      h,
		ps:        ps,
		db:        db,
		topicName: topicName,
		seq:       uint64(time.Now().UnixMilli()),
	}
}

func (g *PeerGossip) Start(ctx context.Context) {
	var err error
	g.topic, err = g.ps.Join(g.topicName)
	if err != nil {
		log.Printf("[gossip] join topic: %v", err)
		return
	}
	g.sub, err = g.topic.Subscribe()
	if err != nil {
		log.Printf("[gossip] subscribe: %v", err)
		return
	}

	OnEvent(g.onEvent)
	go g.subLoop(ctx)
	go g.pubLoop(ctx)
	go g.db.cleanup(ctx)

	log.Printf("[gossip] started on %s", g.topicName)
}

func (g *PeerGossip) subLoop(ctx context.Context) {
	for {
		msg, err := g.sub.Next(ctx)
		if err != nil {
			return
		}
		var a PeerAnnounce
		if err := json.Unmarshal(msg.Data, &a); err != nil {
			continue
		}
		pid, err := peer.Decode(a.PeerID)
		if err != nil {
			continue
		}
		if pid == g.host.ID() {
			continue
		}
		fromPeerID, _ := peer.IDFromBytes(msg.From)
		if fromPeerID == g.host.ID() {
			continue
		}
		log.Printf("[gossip] %v=%v got from %s data.len=%d data:%s", g.topicName, len(g.ps.ListPeers(g.topicName)), fromPeerID.ShortString(), len(msg.Data), string(msg.Data))
		var addrs []multiaddr.Multiaddr
		for _, s := range a.Addrs {
			m, err := multiaddr.NewMultiaddr(s)
			if err != nil {
				continue
			}
			addrs = append(addrs, m)
		}
		if rec, ok := g.db.Get(pid); ok {
			added, removed := addrListDiff(rec.Addrs, addrs)
			if len(added) > 0 || len(removed) > 0 {
				log.Printf("[gossip] peer %s addrs changed: +%d -%d", pid.ShortString(), len(added), len(removed))
			}
		}
		g.db.Update(pid, addrs)
	}
}

func (g *PeerGossip) pubLoop(ctx context.Context) {
	ticker := time.NewTicker(publishInterval)
	defer ticker.Stop()
	g.publish(ctx)
	for {
		select {
		case <-ticker.C:
			g.publish(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (g *PeerGossip) publish(ctx context.Context) {
	addrsAny := g.addrs.Load()
	if addrsAny == nil {
		return
	}
	addrs := addrsAny.([]multiaddr.Multiaddr)
	if len(addrs) == 0 {
		return
	}
	strs := make([]string, len(addrs))
	for i, a := range addrs {
		strs[i] = a.String()
	}
	a := PeerAnnounce{
		PeerID: g.host.ID().String(),
		Addrs:  strs,
		Seq:    atomic.AddUint64(&g.seq, 1),
		TS:     time.Now().UnixMilli(),
	}
	data, _ := json.Marshal(a)
	g.topic.Publish(ctx, data)
}

func addrListDiff(oldAddrs, newAddrs []multiaddr.Multiaddr) (added, removed []string) {
	oldSet := make(map[string]struct{}, len(oldAddrs))
	for _, a := range oldAddrs {
		oldSet[a.String()] = struct{}{}
	}
	for _, a := range newAddrs {
		s := a.String()
		if _, ok := oldSet[s]; !ok {
			added = append(added, s)
		}
		delete(oldSet, s)
	}
	for s := range oldSet {
		removed = append(removed, s)
	}
	return
}

func (g *PeerGossip) onEvent(raw any) {
	e, ok := raw.(event.EvtLocalAddressesUpdated)
	if !ok {
		return
	}
	addrs := make([]multiaddr.Multiaddr, 0, len(e.Current)+len(e.Removed))
	for _, u := range e.Current {
		addrs = append(addrs, u.Address)
	}
	for _, u := range e.Removed {
		addrs = append(addrs, u.Address)
	}
	g.addrs.Store(addrs)
}
