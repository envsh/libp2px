package p2put

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ipfs/go-cid"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/metrics"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multihash"
)

var bootres = &BootNode{
	AddrMgr:   NewAddrManager(),
	PeerDB:    NewPeerDB(600 * time.Minute),
	Bwc:       metrics.NewBandwidthCounter(),
	RelayPool: NewRelayPool(WeightConfig{}),
}

//////////////

type Event struct {
	EventID int64 `json:"event_id"`
	Type    string
	Topic   string
	Value   any
}

type pubsubEvent struct {
	From         string `json:"from"`
	Data         string `json:"data"`
	Seqno        string `json:"seqno"`
	Topic        string `json:"topic"`
	ReceivedFrom string `json:"ReceivedFrom"`
}

const (
	eventBufCap = 128
	maxCap      = 0
)

type eventBuffer struct {
	mu     sync.Mutex
	events []Event
	lastID int64
}

var (
	rawChan       chan any
	clients       map[chan Event]struct{}
	clientsMu     sync.RWMutex
	clientTopics  map[chan Event][]string
	topicSubs     sync.Map // map[string]*pubsub.Topic
	topicBuf      sync.Map // map[string]*eventBuffer
	discoveryTags sync.Map // set[string]

	eventCallbacks   map[uintptr]func(any)
	eventCallbacksMu sync.Mutex
)

func init() {
	rawChan = make(chan any, 100)
	clients = make(map[chan Event]struct{})
	clientTopics = make(map[chan Event][]string)
	eventCallbacks = make(map[uintptr]func(any))
	go broadcastLoop()
	// discoveryTags.Store("libp2p-bootstrap-test", struct{}{})
	discoveryTags.Store("envsh-d2hub", struct{}{})
	discoveryTags.Store(officalHubName, struct{}{})
}

func (eb *eventBuffer) push(evt Event) {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	id := time.Now().UnixNano()
	if id <= eb.lastID {
		id = eb.lastID + 1
	}
	eb.lastID = id
	evt.EventID = id
	eb.events = append(eb.events, evt)
	if len(eb.events) > eventBufCap {
		eb.events = eb.events[len(eb.events)-eventBufCap:]
	}
}

func (eb *eventBuffer) replay(afterID int64) []Event {
	eb.mu.Lock()
	defer eb.mu.Unlock()
	var out []Event
	for _, e := range eb.events {
		if e.EventID > afterID {
			out = append(out, e)
		}
	}
	return out
}

func pushToBuf(evt Event) {
	v, _ := topicBuf.LoadOrStore(evt.Topic, &eventBuffer{})
	v.(*eventBuffer).push(evt)
}

func AddDiscoveryTag(tag string) {
	discoveryTags.Store(tag, struct{}{})
}

func RemoveDiscoveryTag(tag string) {
	discoveryTags.Delete(tag)
}

func broadcastLoop() {
	for raw := range rawChan {
		v := reflect.ValueOf(raw)
		for v.Kind() == reflect.Ptr || v.Kind() == reflect.Interface {
			v = v.Elem()
		}
		if v.Kind() == reflect.Struct {
			var keys []string
			for i := 0; i < v.NumField(); i++ {
				keys = append(keys, v.Type().Field(i).Name)
			}
			log.Printf("[broadcastLoop] raw keys=%v type=%s", keys, v.Type().String())
		}

		evt := Event{EventID: time.Now().UnixNano()}
		if e, ok := raw.(Event); ok {
			// raw 已是 Event（来自 pubsubfw 等），直接提取字段，
			// 避免双层包装成 Event{Type:"p2put.Event", Value:Event{...}}
			evt.Type = e.Type
			evt.Topic = e.Topic
			evt.Value = e.Value
		} else {
			// raw 是 libp2p 原生事件等非 Event 类型，照旧包裹
			evt.Type = reflect.TypeOf(raw).String()
			evt.Value = raw
		}

		fireClients(evt)
		pushToBuf(evt)
		fireCallbacks(raw)
	}
}

func fireClients(evt Event) {
	clientsMu.RLock()
	defer clientsMu.RUnlock()
	for ch := range clients {
		select {
		case ch <- evt:
		default:
			log.Printf("[events] drop %s to slow client", evt.Type)
		}
	}
}

func fireCallbacks(raw any) {
	eventCallbacksMu.Lock()
	cbs := make([]func(any), 0, len(eventCallbacks))
	for _, cb := range eventCallbacks {
		cbs = append(cbs, cb)
	}
	eventCallbacksMu.Unlock()
	for _, cb := range cbs {
		go func(fn func(any)) {
			defer recover()
			fn(raw)
		}(cb)
	}
}

func hasTopic(topics []string, topic string) bool {
	for _, t := range topics {
		if t == topic {
			return true
		}
	}
	return false
}

func getOrSubscribeTopic(topic string) (*pubsub.Topic, error) {
	if bootres == nil || bootres.PSO == nil {
		return nil, fmt.Errorf("pso not ready")
	}
	if val, ok := topicSubs.Load(topic); ok {
		return val.(*pubsub.Topic), nil
	}

	t, err := bootres.PSO.Join(topic)
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

func substrSafe(s string, maxRunes int) string {
	runes := []rune(s)
	if len(runes) > maxRunes {
		runes = runes[:maxRunes]
	}
	return string(runes)
}

func topicListener(sub *pubsub.Subscription, topic string) {
	ctx := context.Background()
	for {
		msg, err := sub.Next(ctx)
		if err != nil {
			return
		}
		// isme := msg.ReceivedFrom == bootres.PeerID
		// log.Println("<< submsg", isme, msg.ReceivedFrom.ShortString(), topic, len(msg.Data), substrSafe(string(msg.Data), 48))
		if isMsgSeen(msg.ID) {
			// /d2hub/pubsub/1.0 forward handler 已处理过，跳过避免重复
			continue
		}
		if msg.ReceivedFrom == bootres.PeerID {
			ForwardToLimitedPeers(*msg.Topic, msg.Data)
		}
		// msg.ID 是二进制拼接 key，Event JSON 序列化会被 \uXXXX 膨胀
		// Event 下游不依赖 msg.ID 寻址，清掉节省内存和序列化开销
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

const maxPublishSize = 1 << 20 // 1MB, matches DefaultMaxMessageSize

func UnsubscribeTopic(topic string) error {
	if bootres == nil || bootres.PSO == nil {
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

func PublishTopic(topic string, data []byte) error {
	t, err := getOrSubscribeTopic(topic)
	if err != nil {
		return err
	}
	// log.Printf("[pso] subscribe topic=%q, peers=%d", topic, len(bootres.PSO.ListPeers(topic)))
	if len(data) > maxPublishSize {
		return fmt.Errorf("payload too large: %d bytes, max %d", len(data), maxPublishSize)
	}
	err = t.Publish(context.Background(), data)
	if err == nil && len(bootres.PSO.ListPeers(topic)) == 0 {
		err = fmt.Errorf("no peers found for %v", topic)
		if time.Since(pubtopicLastTime) > 5*time.Second {
			pubtopicLastTime = time.Now()
			log.Printf("[pso] publish topic=%q, peers=%d", topic, len(bootres.PSO.ListPeers(topic)))
		}
	}
	return err
}
var pubtopicLastTime = time.Now()

type BoardResp struct {
	PeerID    string         `json:"peer_id"`
	Pubkey    string         `json:"pubkey"`
	NATStatus string         `json:"nat_status"`
	MyAddrs   []AddrResp     `json:"my_addrs"`
	Relays0   int            `json:"relays0"`
	Relays1   int            `json:"relays1"`
	Conns     int            `json:"connections"`
	Bandwidth *BandwidthResp `json:"bandwidth"`
	Resources *ResourcesResp `json:"resources,omitempty"`
}

type ResourcesResp struct {
	System    ScopeStatResp `json:"system"`
	Transient ScopeStatResp `json:"transient"`
}

type ScopeStatResp struct {
	StreamsIn  int   `json:"streams_in"`
	StreamsOut int   `json:"streams_out"`
	ConnsIn    int   `json:"connections_in"`
	ConnsOut   int   `json:"connections_out"`
	FD         int   `json:"fd"`
	Memory     int64 `json:"memory_bytes"`
}

type BandwidthResp struct {
	TotalIn  int64   `json:"total_in_bytes"`
	TotalOut int64   `json:"total_out_bytes"`
	RateIn   float64 `json:"rate_in"`
	RateOut  float64 `json:"rate_out"`
}

type RelayResp struct {
	Connected  []string `json:"connected"`
	Candidates []string `json:"candidates"`
}

type AddrResp struct {
	Addr  string `json:"addr"`
	Relay bool   `json:"is_relay"`
	Priv  bool   `json:"is_private"`
}

type ConnResp struct {
	PeerID      string   `json:"peer_id"`
	Addr        string   `json:"addr"`
	Direction   string   `json:"direction"`    // c.Stat().Direction
	IsRelay     bool     `json:"is_relay"`
	RelayPeer   string   `json:"relay_peer,omitempty"`
	DirectAddrs []string `json:"direct_addrs,omitempty"`
	OpenAt      string   `json:"open_at,omitempty"`   // c.Stat().Opened
	NumStreams  int      `json:"num_streams,omitempty"` // c.Stat().NumStreams
}

type DHTResp struct {
	ClusterSize int      `json:"cluster_size"`
	Topics      []string `json:"topics"`
	PeerCount   int      `json:"peer_count"`
	Peers       []string `json:"peers"`
}

type StorePeerEntry struct {
	PeerID string   `json:"peer_id"`
	Addrs  []string `json:"addrs"`
}

type TopicEntry struct {
	Topic      string   `json:"topic"`
	Peers      []string `json:"peers"`
	Subscribed bool     `json:"subscribed"`
	IsTag      bool     `json:"is_tag"`
}

type NoopValidator struct{}

func (NoopValidator) Validate(key string, value []byte) error { return nil }
func (NoopValidator) Select(key string, values [][]byte) (int, error) {
	return len(values) - 1, nil
}

type FoundPeer struct {
	PeerID string   `json:"peer_id"`
	Addrs  []string `json:"addrs"`
}

func FindPeers(tag string, limit int) ([]FoundPeer, error) {
	if bootres == nil {
		return nil, fmt.Errorf("libp2p not ready")
	}
	return dhtFindPeers(tag, limit)
}

func toPeerID(v any) (peer.ID, error) {
	switch p := v.(type) {
	case peer.ID:
		return p, nil
	case string:
		return peer.Decode(p)
	default:
		return "", fmt.Errorf("expected peer.ID or string, got %T", v)
	}
}

func IsPeerInTopic(pid any, topic string) bool {
	p, err := toPeerID(pid)
	if err != nil {
		return false
	}
	if bootres == nil || bootres.PSO == nil {
		return false
	}
	for _, q := range bootres.PSO.ListPeers(topic) {
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
	if bootres == nil || bootres.PSO == nil {
		return false
	}
	for _, topic := range bootres.PSO.GetTopics() {
		for _, q := range bootres.PSO.ListPeers(topic) {
			if q == p {
				return true
			}
		}
	}
	return false
}

func IsPeerConnected(pid any, out bool) bool {
	p, err := toPeerID(pid)
	if err != nil {
		return false
	}
	if bootres == nil || bootres.Host == nil {
		return false
	}
	for _, c := range bootres.Host.Network().Conns() {
		if c.RemotePeer() == p {
			if out && c.Stat().Direction == network.DirOutbound {
				return true
			}
		}
	}
	return false
}

func IsPeerInStore(pid any) bool {
	p, err := toPeerID(pid)
	if err != nil {
		return false
	}
	if bootres == nil || bootres.Host == nil {
		return false
	}
	for _, q := range bootres.Host.Peerstore().Peers() {
		if q == p {
			return true
		}
	}
	return false
}

func FindPeer(pid any) (FoundPeer, error) {
	p, err := toPeerID(pid)
	if err != nil {
		return FoundPeer{}, err
	}
	if bootres == nil {
		return FoundPeer{}, fmt.Errorf("libp2p not ready")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	info, err := dhtFindPeer(ctx, p)
	if err != nil {
		return FoundPeer{}, err
	}
	addrs := make([]string, len(info.Addrs))
	for i, a := range info.Addrs {
		addrs[i] = a.String()
	}
	return FoundPeer{PeerID: info.ID.String(), Addrs: addrs}, nil
}

func PutKV(ctx context.Context, key string, value []byte) error {
	if useJamiDHTProxy {
		return jamidhtproxy.Put(key, value)
	}
	if bootres == nil {
		return fmt.Errorf("libp2p not ready")
	}
	return dhtPutKV(ctx, key, value)
}

func GetKV(ctx context.Context, key string) ([]byte, error) {
	if useJamiDHTProxy {
		return jamidhtproxy.Get(key)
	}
	if bootres == nil {
		return nil, fmt.Errorf("libp2p not ready")
	}
	return dhtGetKV(ctx, key)
}

func DelKV(ctx context.Context, key string) error {
	if useJamiDHTProxy {
		return jamidhtproxy.Put(key, []byte{})
	}
	if bootres == nil {
		return fmt.Errorf("libp2p not ready")
	}
	return dhtDelKV(ctx, key)
}

func CollectBoard() (BoardResp, error) {
	if bootres == nil || bootres.Host == nil {
		return BoardResp{}, fmt.Errorf("libp2p not ready")
	}
	h := bootres.Host

	var addrs []AddrResp
	for _, a := range h.Addrs() {
		addrs = append(addrs, AddrResp{
			Addr:  a.String(),
			Relay: isRelayAddr(a),
			Priv:  isPrivateIP(extractIPFromAddr(a)),
		})
	}

	conns, _ := CollectConns()
	relays, _ := CollectRelays()

	var bw *BandwidthResp
	if bootres.Bwc != nil {
		s := bootres.Bwc.GetBandwidthTotals()
		bw = &BandwidthResp{
			TotalIn:  s.TotalIn,
			TotalOut: s.TotalOut,
			RateIn:   s.RateIn,
			RateOut:  s.RateOut,
		}
	}

	var res *ResourcesResp
	if rm := h.Network().ResourceManager(); rm != nil {
		var sys, trans ScopeStatResp
		rm.ViewSystem(func(s network.ResourceScope) error {
			st := s.Stat()
			sys = ScopeStatResp{
				StreamsIn:  st.NumStreamsInbound,
				StreamsOut: st.NumStreamsOutbound,
				ConnsIn:    st.NumConnsInbound,
				ConnsOut:   st.NumConnsOutbound,
				FD:         st.NumFD,
				Memory:     st.Memory,
			}
			return nil
		})
		rm.ViewTransient(func(s network.ResourceScope) error {
			st := s.Stat()
			trans = ScopeStatResp{
				StreamsIn:  st.NumStreamsInbound,
				StreamsOut: st.NumStreamsOutbound,
				ConnsIn:    st.NumConnsInbound,
				ConnsOut:   st.NumConnsOutbound,
				FD:         st.NumFD,
				Memory:     st.Memory,
			}
			return nil
		})
		res = &ResourcesResp{System: sys, Transient: trans}
	}

	return BoardResp{
		PeerID:    h.ID().String(),
		Pubkey:    bootres.PubkeyHex,
		NATStatus: bootres.NATStatus.String(),
		MyAddrs:   addrs,
		Relays0:   len(relays.Candidates),
		Relays1:   len(relays.Connected),
		Conns:     len(conns),
		Bandwidth: bw,
		Resources: res,
	}, nil
}

func CollectConns() ([]ConnResp, error) {
	if bootres == nil || bootres.Host == nil {
		return nil, fmt.Errorf("libp2p not ready")
	}
	var out []ConnResp
	for _, c := range bootres.Host.Network().Conns() {
		if isBootstrapPeer(c.RemotePeer()) {
			continue
		}
		dir := "outbound"
		if c.Stat().Direction == network.DirInbound {
			dir = "inbound"
		}
		entry := ConnResp{
			PeerID:     c.RemotePeer().String(),
			Addr:       c.RemoteMultiaddr().String(),
			Direction:  dir,
			OpenAt:     c.Stat().Opened.Format(time.RFC3339),
			NumStreams: c.Stat().NumStreams,
		}
		if c.Stat().Limited {
			entry.IsRelay = true
			if rpid, err := extractRelayPeerID(c.RemoteMultiaddr()); err == nil {
				entry.RelayPeer = rpid.String()
			}
			for _, a := range bootres.Host.Peerstore().Addrs(c.RemotePeer()) {
				if !isRelayAddr(a) {
					entry.DirectAddrs = append(entry.DirectAddrs, a.String())
				}
			}
			if entry.DirectAddrs == nil {
				entry.DirectAddrs = []string{}
			}
		}
		out = append(out, entry)
	}
	return out, nil
}

func CollectDHT() (DHTResp, error) {
	if bootres == nil || bootres.Host == nil || bootres.PSO == nil {
		return DHTResp{}, fmt.Errorf("libp2p not ready")
	}
	size, strs := dhtCollectDHT()

	topics := bootres.PSO.GetTopics()
	log.Println(topics)

	return DHTResp{
		PeerCount: size,
		Peers:     strs,
		Topics:    topics,
	}, nil
}

func CollectRelays() (RelayResp, error) {
	if bootres == nil || bootres.Host == nil {
		return RelayResp{}, fmt.Errorf("libp2p not ready")
	}
	h := bootres.Host

	var candidates []string
	candidatePeers := make(map[peer.ID]struct{})
	for _, a := range h.Addrs() {
		if !isRelayAddr(a) {
			continue
		}
		addrStr := a.String()
		candidates = append(candidates, addrStr)

		trimmed := strings.TrimSuffix(addrStr, "/p2p-circuit")
		ma, err := multiaddr.NewMultiaddr(trimmed)
		if err != nil {
			continue
		}
		pidStr, err := ma.ValueForProtocol(multiaddr.P_P2P)
		if err != nil {
			continue
		}
		pid, err := peer.Decode(pidStr)
		if err != nil {
			continue
		}
		candidatePeers[pid] = struct{}{}
	}

	var connected []string
	for _, c := range h.Network().Conns() {
		if _, ok := candidatePeers[c.RemotePeer()]; ok {
			connected = append(connected, c.RemoteMultiaddr().String()+"/p2p/"+c.RemotePeer().String())
		}
	}

	if candidates == nil {
		candidates = []string{}
	}
	if connected == nil {
		connected = []string{}
	}

	return RelayResp{
		Candidates: candidates,
		Connected:  connected,
	}, nil
}

func CollectStorePeers() []StorePeerEntry {
	if  bootres.Host == nil {
		return nil
	}
	ps := bootres.Host.Peerstore()
	var out []StorePeerEntry
	for _, p := range ps.Peers() {
		addrs := ps.Addrs(p)
		if len(addrs) == 0 {
			continue
		}
		var as []string
		for _, a := range addrs {
			s := a.String()
			if strings.Contains(s, "/ip6/") ||
				strings.Contains(s, "/udp/") ||
				strings.Contains(s, "/quic") ||
				strings.Contains(s, "webrtc") ||
				strings.Contains(s, "/dns/") {
				continue
			}
			as = append(as, s)
		}
		if len(as) == 0 {
			continue
		}
		out = append(out, StorePeerEntry{
			PeerID: p.String(),
			Addrs:  as,
		})
	}
	if out == nil {
		out = []StorePeerEntry{}
	}
	return out
}

func GetClusterPeers() []string {
	return bootres.PeerDB.ListIDs()
}

func CollectTopics() []TopicEntry {
	if bootres.Host == nil || bootres.PSO == nil {
		return []TopicEntry{}
	}
	seen := make(map[string]*TopicEntry)
	for _, t := range bootres.PSO.GetTopics() {
		seen[t] = &TopicEntry{Topic: t}
	}
	topicSubs.Range(func(key, val any) bool {
		t := key.(string)
		topic := val.(*pubsub.Topic)
		if _, ok := seen[t]; !ok {
			seen[t] = &TopicEntry{Topic: t}
		}
		seen[t].Subscribed = true
		for _, p := range topic.ListPeers() {
			seen[t].Peers = append(seen[t].Peers, p.String())
		}
		return true
	})
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

// OnEvent registers a callback for all raw events (libp2p system events, pubsub messages, etc).
// The same function (by pointer identity) cannot be registered twice.
// Note: each closure literal is a distinct pointer; reuse the same function variable for dedup.
func OnEvent(fn func(any)) error {
	ptr := reflect.ValueOf(fn).Pointer()
	eventCallbacksMu.Lock()
	defer eventCallbacksMu.Unlock()
	if _, ok := eventCallbacks[ptr]; ok {
		return fmt.Errorf("callback already registered")
	}
	eventCallbacks[ptr] = fn
	return nil
}

// OffEvent removes a previously registered event callback.
func OffEvent(fn func(any)) error {
	ptr := reflect.ValueOf(fn).Pointer()
	eventCallbacksMu.Lock()
	defer eventCallbacksMu.Unlock()
	if _, ok := eventCallbacks[ptr]; !ok {
		return fmt.Errorf("callback not found")
	}
	delete(eventCallbacks, ptr)
	return nil
}

// StringToCID 将任意字符串转换为确定性 CIDv1
func StringToCID(s string) string {
	h := sha256.Sum256([]byte(s))
	mh, err := multihash.Encode(h[:], multihash.SHA2_256)
	if err != nil {
		return ""
	}
	return cid.NewCidV1(cid.Raw, mh).String()
}

// FindProvidersBycid 查询公共路由端点，参数 cid 为任意字符串，内部自动转换
func FindProvidersBycid(cidStr string) ([]FoundPeer, error) {
	cidStr = StringToCID(cidStr)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		"https://delegated-ipfs.dev/routing/v1/providers/"+cidStr, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("query delegated-ipfs: %w", err)
	}
	defer resp.Body.Close()

	var body struct {
		Providers []struct {
			ID    string   `json:"ID"`
			Addrs []string `json:"Addrs"`
		} `json:"Providers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil, err
	}

	var out []FoundPeer
	for _, p := range body.Providers {
		out = append(out, FoundPeer{PeerID: p.ID, Addrs: p.Addrs})
	}
	return out, nil
}
