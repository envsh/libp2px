//go:build !nopubsub

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

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-msgio"
)

const PubSubFwdProtocol = protocol.ID("/d2hub/pubsub/1.0")

var (
	seenMsgIDs   sync.Map
	dedupTTL     = 10 * time.Minute
	dedupMaxSize = 2000
	dedupCount   atomic.Int32

	topicSubs         sync.Map
	pubtopicLastTime  = time.Now()
)

func init() {
	MustRegisterProtocol("pubsub/1.0", handlePubSubFwdStream)
}

func BuildGossipSub(ctx context.Context, h host.Host, staticRelays []peer.AddrInfo) (any, error) {
	return pubsub.NewGossipSub(ctx, h,
		pubsub.WithPeerExchange(true),
		pubsub.WithFloodPublish(true),
		pubsub.WithDirectPeers(staticRelays),
		pubsub.WithGossipSubParams(myGossipSubParams()),
		pubsub.WithPeerScore(
			&pubsub.PeerScoreParams{
				SkipAtomicValidation: true,
				AppSpecificScore:     func(peer.ID) float64 { return 0 },
				DecayInterval:        time.Second,
				DecayToZero:          0.01,
			},
			&pubsub.PeerScoreThresholds{
				SkipAtomicValidation: true,
			},
		),
		pubsub.WithPeerScoreInspect(
			func(scores map[peer.ID]float64) {
				for pid, s := range scores {
					log.Printf("[score] %s: %.2f", pid.ShortString(), s)
				}
			},
			30*time.Second,
		),
	)
}

func myGossipSubParams() pubsub.GossipSubParams {
	dft := pubsub.DefaultGossipSubParams()
	dft0 := dft
	which := 0
	if currConfig.IsMobile {
		which = 2
	}
	if which == 1 {
		dft.D = 3
		dft.Dlo = 2
		dft.Dhi = 4
		dft.Dlazy = 2
		dft.GossipFactor = 0.05
		dft.HeartbeatInterval = 5 * time.Second
		dft.HistoryLength = 3
		dft.HistoryGossip = 2
		dft.DirectConnectTicks = 600
	} else if which == 2 {
		dft.D = 3
		dft.Dlo = 2
		dft.Dhi = 6
		dft.Dlazy = 3
		dft.GossipFactor = 0.1
		dft.HeartbeatInterval = 2 * time.Second
		dft.HistoryLength = 5
		dft.HistoryGossip = 3
		dft.DirectConnectTicks = 600
	} else {
		dft.D = 3
		dft.Dlo = 2
		dft.Dhi = 6
		dft.Dlazy = 6
		dft.GossipFactor = 0.25
		dft.HeartbeatInterval = 10 * time.Second
		dft.HistoryLength = 5
		dft.HistoryGossip = 3
		dft.DirectConnectTicks = 600
		dft = dft0
	}
	return dft
}

func getPSO() *pubsub.PubSub {
	if bootres == nil {
		return nil
	}
	pso, _ := bootres.PSO.(*pubsub.PubSub)
	return pso
}

func getPSOTopics() []string {
	pso := getPSO()
	if pso == nil {
		return nil
	}
	return pso.GetTopics()
}

func getSubscribedTopicPeers() map[string][]peer.ID {
	result := make(map[string][]peer.ID)
	topicSubs.Range(func(key, val any) bool {
		t := key.(string)
		topic := val.(*pubsub.Topic)
		result[t] = topic.ListPeers()
		return true
	})
	return result
}

func getOrSubscribeTopic(topic string) (*pubsub.Topic, error) {
	pso := getPSO()
	if pso == nil {
		return nil, fmt.Errorf("pso not ready")
	}
	if val, ok := topicSubs.Load(topic); ok {
		return val.(*pubsub.Topic), nil
	}

	t, err := pso.Join(topic)
	if err != nil {
		if val, ok := topicSubs.Load(topic); ok {
			return val.(*pubsub.Topic), nil
		}
		return nil, err
	}

	topicSubs.Store(topic, t)

	sub, err := t.Subscribe()
	if err != nil {
		topicSubs.Delete(topic)
		return nil, err
	}
	go topicListener(sub, topic)
	return t, nil
}

func topicListener(sub *pubsub.Subscription, topic string) {
	ctx := context.Background()
	for {
		msg, err := sub.Next(ctx)
		if err != nil {
			return
		}
		if isMsgSeen(msg.ID) {
			continue
		}
		if msg.ReceivedFrom == bootres.PeerID {
			ForwardToLimitedPeers(*msg.Topic, msg.Data)
		}
		msg.ID = ""
		msg.Message.Signature = nil
		msg.Message.Key = nil
		evt := Event{EventID: time.Now().UnixNano(), Type: "pubsub", Topic: topic, Value: pubsubEvent{
			From:         string(msg.Message.From),
			Data:         string(msg.Message.Data),
			Seqno:        base64.StdEncoding.EncodeToString(msg.Message.Seqno),
			Topic:        *msg.Message.Topic,
			ReceivedFrom: msg.ReceivedFrom.ShortString(),
		}}
		clientsMu.RLock()
		for ch, topics := range clientTopics {
			if hasTopic(topics, topic) {
				select {
				case ch <- evt:
				default:
					log.Printf("[events] drop pubsub/%s to slow client", topic)
				}
			}
		}
		clientsMu.RUnlock()
		pushToBuf(evt)
		fireCallbacks(msg)
	}
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

func PublishTopic(topic string, data []byte) error {
	t, err := getOrSubscribeTopic(topic)
	if err != nil {
		return err
	}
	if len(data) > maxPublishSize {
		return fmt.Errorf("payload too large: %d bytes, max %d", len(data), maxPublishSize)
	}
	err = t.Publish(context.Background(), data)
	pso := getPSO()
	if err == nil && pso != nil && len(pso.ListPeers(topic)) == 0 {
		err = fmt.Errorf("no peers found for %v", topic)
		if time.Since(pubtopicLastTime) > 5*time.Second {
			pubtopicLastTime = time.Now()
			log.Printf("[pso] publish topic=%q, peers=%d", topic, len(pso.ListPeers(topic)))
		}
	}
	return err
}

func UnsubscribeTopic(topic string) error {
	if getPSO() == nil {
		return fmt.Errorf("pso not ready")
	}
	val, ok := topicSubs.Load(topic)
	if !ok {
		return fmt.Errorf("topic %s not subscribed", topic)
	}
	t := val.(*pubsub.Topic)
	if err := t.Close(); err != nil {
		return err
	}
	topicSubs.Delete(topic)
	log.Printf("[pso] unsubscribed: %s", topic)
	return nil
}

func IsPeerInTopic(pid any, topic string) bool {
	p, err := toPeerID(pid)
	if err != nil {
		return false
	}
	pso := getPSO()
	if pso == nil {
		return false
	}
	for _, q := range pso.ListPeers(topic) {
		if q == p {
			return true
		}
	}
	return false
}

func IsPeerInAnyTopic(pid any) bool {
	p, err := toPeerID(pid)
	if err != nil {
		return false
	}
	pso := getPSO()
	if pso == nil {
		return false
	}
	for _, topic := range pso.GetTopics() {
		for _, q := range pso.ListPeers(topic) {
			if q == p {
				return true
			}
		}
	}
	return false
}

func CollectTopics() []TopicEntry {
	if bootres.Host == nil {
		return []TopicEntry{}
	}
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
	topics := getPSOTopics()
	log.Println(topics)

	return DHTResp{
		PeerCount: size,
		Peers:     strs,
		Topics:    topics,
	}, nil
}

func ListTopicPeers(topic string) []peer.ID {
	pso := getPSO()
	if pso == nil {
		return nil
	}
	return pso.ListPeers(topic)
}

func ForwardPubSubToPeer(pid peer.ID, msg *pubsub.Message) error {
	if bootres.Host == nil {
		return fmt.Errorf("host not ready")
	}

	_, ok := bootres.PeerDB.Get(pid)
	if !ok {
		return fmt.Errorf("peer %s not in PeerDB", pid.ShortString())
	}

	protos, err2 := bootres.Host.Peerstore().GetProtocols(pid)
	supported, err := bootres.Host.Peerstore().SupportsProtocols(pid, PubSubFwdProtocol)
	if err == nil && len(supported) == 0 {
		if err2 == nil && len(protos) == 0 {
		} else {
			log.Printf("[pubsubfw] %s does NOT support %s, has %d protos: %v",
				pid.ShortString(), PubSubFwdProtocol, len(protos), protos)
			return fmt.Errorf("peer %s does not support %s", pid.ShortString(), PubSubFwdProtocol)
		}
	}

	savedID := msg.ID
	msg.ID = ""
	out, err := json.Marshal(msg)
	msg.ID = savedID
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	ctx := network.WithAllowLimitedConn(context.Background(), "pubsub/1.0")
	s, err := bootres.Host.NewStream(ctx, pid, PubSubFwdProtocol)
	if err != nil {
		return fmt.Errorf("newstream: %w", err)
	}
	defer s.Close()

	wr := msgio.NewVarintWriter(s)
	if err := wr.WriteMsg(out); err != nil {
		s.Reset()
		return fmt.Errorf("write (len=%d): %w", len(out), err)
	}
	s.CloseWrite()
	return nil
}

func handlePubSubFwdStream(s network.Stream) {
	defer s.Close()
	pid := s.Conn().RemotePeer()

	s.SetReadDeadline(time.Now().Add(pushTimeout))
	rd := msgio.NewVarintReaderSize(s, 1<<20)
	raw, err := rd.ReadMsg()
	if err != nil {
		n, _ := rd.NextMsgLen()
		log.Printf("[pubsubfw] read from %s (len=%d): %v", pid.ShortString(), n, err)
		return
	}

	msg := &pubsub.Message{Message: &pb.Message{}}
	if err := json.Unmarshal(raw, msg); err != nil {
		log.Printf("[pubsubfw] unmarshal from %s: %v", pid.ShortString(), err)
		return
	}
	msg.ID = pubsub.DefaultMsgIdFn(msg.Message)
	msg.ReceivedFrom = pid

	if isMsgSeen(msg.ID) {
		log.Printf("[pubsubfw] dup %s from %s", msg.ID, pid.ShortString())
		return
	}

	log.Printf("[pubsubfw] fwd from=%s seq_time=%s topic=%s peer=%s",
		peer.ID(msg.Message.From).ShortString(),
		time.Unix(0, int64(binary.BigEndian.Uint64(msg.Message.Seqno))).Format("15:04:05.000000"),
		*msg.Topic, pid.ShortString())
	msg.ID = ""
	msg.Message.Signature = nil
	msg.Message.Key = nil
	rawChan <- Event{Type: "pubsub", Topic: *msg.Message.Topic, Value: pubsubEvent{
		From:         string(msg.Message.From),
		Data:         string(msg.Message.Data),
		Seqno:        base64.StdEncoding.EncodeToString(msg.Message.Seqno),
		Topic:        *msg.Message.Topic,
		ReceivedFrom: msg.ReceivedFrom.ShortString(),
	}}
}

func ForwardToLimitedPeers(topic string, data []byte) error {
	if bootres.Host == nil {
		return fmt.Errorf("host not ready")
	}

	pid := bootres.Host.ID()
	seqno := make([]byte, 8)
	binary.BigEndian.PutUint64(seqno, uint64(time.Now().UnixNano()))

	msg := &pubsub.Message{
		Message: &pb.Message{
			From:  []byte(pid),
			Data:  data,
			Seqno: seqno,
			Topic: &topic,
		},
	}
	msg.ID = pubsub.DefaultMsgIdFn(msg.Message)
	msg.ReceivedFrom = pid
	isMsgSeen(msg.ID)

	var topicSet map[peer.ID]struct{}
	pso := getPSO()
	if pso != nil {
		peers := pso.ListPeers(topic)
		topicSet = make(map[peer.ID]struct{}, len(peers))
		for _, tp := range peers {
			topicSet[tp] = struct{}{}
		}
	}

	var okN, failN int
	for _, r := range bootres.PeerDB.List() {
		if _, in := topicSet[r.PeerID]; in {
			okN++
			continue
		}
		if err := ForwardPubSubToPeer(r.PeerID, msg); err != nil {
			log.Printf("[pubsubfw] fwd to %s: %v", r.PeerID.ShortString(), err)
			failN++
		} else {
			okN++
		}
	}
	if failN > 0 || time.Since(pubsubfwLastTime) > 9*time.Second {
		log.Printf("[pubsubfw] fwd %q limited=%d ok=%d fail=%d", topic, okN+failN, okN, failN)
		pubsubfwLastTime = time.Now()
	}
	return nil
}

var pubsubfwLastTime = time.Now()
