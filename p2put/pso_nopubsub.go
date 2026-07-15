//go:build nopubsub

package p2put

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-msgio"
)

const pubSubFwdProtocol = protocol.ID("/d2hub/pubsub/1.0")

type pubsubFwdMsg struct {
	From  []byte `json:"from,omitempty"`
	Data  []byte `json:"data,omitempty"`
	Seqno []byte `json:"seqno,omitempty"`
	Topic string `json:"topic,omitempty"`
}

var (
	seenMsgIDs   sync.Map
	dedupTTL     = 10 * time.Minute
	dedupMaxSize = 2000
	dedupCount   atomic.Int32
	pubsubfwLastTime = time.Now()
	stubTopics     sync.Map
)

func init() {
	MustRegisterProtocol("pubsub/1.0", handlePubSubFwdStream)
}

func isMsgSeen(msgID string) bool {
	_, loaded := seenMsgIDs.LoadOrStore(msgID, time.Now())
	if loaded {
		return true
	}
	cnt := dedupCount.Add(1)
	if cnt >= int32(dedupMaxSize) {
		dedupCount.Store(0)
		seenMsgIDs.Range(func(key, val any) bool {
			if time.Since(val.(time.Time)) > dedupTTL {
				seenMsgIDs.Delete(key)
			} else {
				dedupCount.Add(1)
			}
			return true
		})
	}
	return false
}

func fwdMsgID(msg *pubsubFwdMsg) string {
	return string(msg.From) + string(msg.Seqno)
}

func handlePubSubFwdStream(s network.Stream) {
	defer s.Close()
	pid := s.Conn().RemotePeer()

	s.SetReadDeadline(time.Now().Add(pushTimeout))
	rd := msgio.NewVarintReaderSize(s, 1<<20)
	raw, err := rd.ReadMsg()
	if err != nil {
		n, _ := rd.NextMsgLen()
		log.Printf("[psostub] read from %s (len=%d): %v", pid.ShortString(), n, err)
		return
	}

	var msg pubsubFwdMsg
	if err := json.Unmarshal(raw, &msg); err != nil {
		log.Printf("[psostub] unmarshal from %s: %v", pid.ShortString(), err)
		return
	}

	id := fwdMsgID(&msg)
	if isMsgSeen(id) {
		return
	}

	log.Printf("[psostub] fwd from=%s seq_time=%s topic=%s peer=%s",
		peer.ID(msg.From).ShortString(),
		time.Unix(0, int64(binary.BigEndian.Uint64(msg.Seqno))).Format("15:04:05.000000"),
		msg.Topic, pid.ShortString())

	rawChan <- Event{Type: "pubsub", Topic: msg.Topic, Value: pubsubEvent{
		From:         string(msg.From),
		Data:         string(msg.Data),
		Seqno:        base64.StdEncoding.EncodeToString(msg.Seqno),
		Topic:        msg.Topic,
		ReceivedFrom: pid.ShortString(),
	}}
}

func BuildGossipSub(_ context.Context, _ host.Host, _ []peer.AddrInfo) (any, error) {
	return nil, nil
}

func getPSOTopics() []string {
	var out []string
	stubTopics.Range(func(key, _ any) bool {
		out = append(out, key.(string))
		return true
	})
	return out
}

func getSubscribedTopicPeers() map[string][]peer.ID {
	records := bootres.PeerDB.List()
	allPeers := make([]peer.ID, 0, len(records))
	for _, r := range records {
		allPeers = append(allPeers, r.PeerID)
	}

	result := make(map[string][]peer.ID)
	stubTopics.Range(func(key, _ any) bool {
		result[key.(string)] = allPeers
		return true
	})
	return result
}

func getOrSubscribeTopic(topic string) (any, error) {
	stubTopics.Store(topic, struct{}{})
	return nil, nil
}

func PublishTopic(topic string, data []byte) error {
	if bootres == nil || bootres.Host == nil || bootres.PeerDB == nil {
		return fmt.Errorf("libp2p not ready")
	}
	if len(data) > maxPublishSize {
		return fmt.Errorf("payload too large: %d bytes, max %d", len(data), maxPublishSize)
	}

	pid := bootres.Host.ID()
	seqno := make([]byte, 8)
	binary.BigEndian.PutUint64(seqno, uint64(time.Now().UnixNano()))

	msg := &pubsubFwdMsg{
		From:  []byte(pid),
		Data:  data,
		Seqno: seqno,
		Topic: topic,
	}
	getOrSubscribeTopic(topic)
	isMsgSeen(fwdMsgID(msg))

	// 模拟 pubsub 本地回环：publish 后自己也应收到消息
	rawChan <- Event{
		Type:  "pubsub",
		Topic: topic,
		Value: pubsubEvent{
			From:         string(pid),
			Data:         string(data),
			Seqno:        base64.StdEncoding.EncodeToString(seqno),
			Topic:        topic,
			ReceivedFrom: pid.ShortString(),
		},
	}

	peers := bootres.PeerDB.List()
	if len(peers) == 0 {
		return fmt.Errorf("no peers 0")
	}

	out, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	var okN, failN int
	for _, r := range peers {
		ctx := network.WithAllowLimitedConn(context.Background(), "pubsub/1.0")
		s, err := bootres.Host.NewStream(ctx, r.PeerID, pubSubFwdProtocol)
		if err != nil {
			failN++
			continue
		}
		wr := msgio.NewVarintWriter(s)
		if err := wr.WriteMsg(out); err != nil {
			s.Reset()
			failN++
			continue
		}
		s.CloseWrite()
		s.Close()
		okN++
	}

	if okN == 0 && time.Since(pubsubfwLastTime) > 9*time.Second {
		log.Printf("[psostub] publish %q peers=%d ok=%d fail=%d", topic, len(peers), okN, failN)
		pubsubfwLastTime = time.Now()
	}
	if okN == 0 {
		return fmt.Errorf("no peers %d", len(peers))
	}
	return nil
}

func UnsubscribeTopic(topic string) error {
	return fmt.Errorf("pubsub disabled")
}

func IsPeerInTopic(pid any, topic string) bool {
	return false
}

func IsPeerInAnyTopic(pid any) bool {
	return false
}

func CollectTopics() []TopicEntry {
	seen := make(map[string]*TopicEntry)

	for _, t := range getPSOTopics() {
		seen[t] = &TopicEntry{Topic: t}
	}

	for t, peers := range getSubscribedTopicPeers() {
		entry, ok := seen[t]
		if !ok {
			entry = &TopicEntry{Topic: t}
			seen[t] = entry
		}
		entry.Subscribed = true
		for _, p := range peers {
			entry.Peers = append(entry.Peers, p.String())
		}
	}

	discoveryTags.Range(func(key, _ any) bool {
		t := key.(string)
		if _, ok := seen[t]; !ok {
			seen[t] = &TopicEntry{Topic: t}
		}
		seen[t].IsTag = true
		return true
	})

	out := make([]TopicEntry, 0, len(seen))
	for _, v := range seen {
		out = append(out, *v)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Topic < out[j].Topic
	})
	return out
}

func CollectDHT() (DHTResp, error) {
	if bootres == nil || bootres.Host == nil {
		return DHTResp{}, fmt.Errorf("libp2p not ready")
	}
	size, strs := dhtCollectDHT()
	log.Println("no topics (pubsub disabled)")

	return DHTResp{
		PeerCount: size,
		Peers:     strs,
		Topics:    nil,
	}, nil
}

func ListTopicPeers(topic string) []peer.ID {
	records := bootres.PeerDB.List()
	out := make([]peer.ID, 0, len(records))
	for _, r := range records {
		out = append(out, r.PeerID)
	}
	return out
}
