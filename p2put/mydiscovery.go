package p2put

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	dht_pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	"github.com/libp2p/go-msgio/protoio"
	"github.com/multiformats/go-multiaddr"
	"github.com/multiformats/go-multibase"
)

var trackers = []string{
	"https://delegated-ipfs.dev", // 0: GET only
	"https://routing.lol",        // 1: PUT 500
	"https://peers.pleb.bot",     // 2: PUT+GET
}

const activeTracker = 2

const advertiseInterval = 50 * time.Second
const discoverInterval = 60 * time.Second

// target_peers.json 格式：JSON 字符串数组
// ["12D3KooXXXX...", "12D3KooYYYY..."]
func loadTargetPeers() []string {
	data, err := os.ReadFile("target_peers.json")
	if err != nil {
		return nil
	}
	var peers []string
	if err := json.Unmarshal(data, &peers); err != nil {
		log.Printf("[discovery] target_peers.json unmarshal error: %v", err)
		return nil
	}
	return peers
}

func trackerURL() string { return trackers[activeTracker] }

// Internal types for PUT request (IPIP-526 signed Bitswap format).
type ipfsPutPayload struct {
	Keys        []string `json:"Keys"`
	Timestamp   int64    `json:"Timestamp"`
	AdvisoryTTL int64    `json:"AdvisoryTTL,omitempty"`
	ID          string   `json:"ID"`
	Addrs       []string `json:"Addrs"`
}

type ipfsPutRecord struct {
	Schema    string         `json:"Schema"`
	Protocol  string         `json:"Protocol"`
	Signature string         `json:"Signature,omitempty"`
	Payload   ipfsPutPayload `json:"Payload"`
}

type ipfsPutRequest struct {
	Providers []ipfsPutRecord `json:"Providers"`
}

// Internal types for GET response (IPIP-417 flat format).
type ipfsProviderRecord struct {
	ID    string   `json:"ID"`
	Addrs []string `json:"Addrs"`
}

type ipfsProvidersResponse struct {
	Providers []ipfsProviderRecord `json:"Providers"`
}

// IpfsHttpTrackerApi encapsulates HTTP calls to an IPFS Delegated Routing V1 API tracker.
//
// Different methods use different tracker endpoints:
//
//	FindProviders → GET /routing/v1/providers/{cid}       (trackers[0]: delegated-ipfs.dev)
//	Provide       → PUT /routing/v1/providers/             (trackers[0]: delegated-ipfs.dev)
//
// Methods default to trackers[0] due to other trackers returning errors on these endpoints.
type IpfsHttpTrackerApi struct {
	baseURL string
	cli     *http.Client
}

func NewIpfsHttpTrackerApi(baseURL string) *IpfsHttpTrackerApi {
	return &IpfsHttpTrackerApi{
		baseURL: strings.TrimRight(baseURL, "/"),
		cli:     http.DefaultClient,
	}
}

// FindProviders queries GET /routing/v1/providers/{cid}.
//
// Response format (IPIP-417 flat, confirmed by boxo server code):
//
//	{"Providers":[{"ID":"12...","Addrs":["/ip4/..."],"Schema":"peer"}]}
//	ID    ✅ fully confirmed: directly on provider level
//	Addrs ✅ fully confirmed: directly on provider level
//
// Parameters:
//
//	cid — CIDv1 string (e.g., "bafkrei...")
func (api *IpfsHttpTrackerApi) FindProviders(ctx context.Context, cid string) ([]FoundPeer, error) {
	if cid == "" {
		return nil, fmt.Errorf("empty cid")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		api.baseURL+"/routing/v1/providers/"+cid, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := api.cli.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var pr ipfsProvidersResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		return nil, err
	}

	var out []FoundPeer
	for _, p := range pr.Providers {
		if p.ID == "" {
			continue
		}
		out = append(out, FoundPeer{PeerID: p.ID, Addrs: p.Addrs})
	}
	return out, nil
}

// PUT /routing/v1/providers/ 是 实验性遗留端点（IPIP-526），从未被 HTTP Routing V1 规范正式标准化。
// Provide sends PUT /routing/v1/providers/ to register this peer as a provider of cid.
//
// Request format (IPIP-526 signed Bitswap, confirmed by boxo server code):
//
//	{"Providers":[{"Schema":"bitswap","Protocol":"transport-bitswap",
//	  "Signature":"mbase64...","Payload":{...}}]}
//
//	Schema    ✅ fully confirmed: server discriminates on Schema=="bitswap"
//	Protocol  ✅ fully confirmed: spec value is "transport-bitswap"
//	Signature ✅ fully confirmed: SHA256(Payload JSON) → privKey.Sign → multibase(Base64)
//	Payload.ID        ✅ fully confirmed: peer ID string
//	Payload.Addrs     ✅ fully confirmed: multiaddr string array
//	Payload.Keys      ✅ fully confirmed: CID string array
//	Payload.Timestamp ⚠️ pending confirm: RFC3339 format (inferred from boxo types.Time)
//	Payload.AdvisoryTTL  ⚠️ pending confirm: Go duration string (e.g., "48h0m0s", optional)
//
// Private key is obtained via bootres.Host.Peerstore().PrivKey(bootres.Host.ID()).
func (api *IpfsHttpTrackerApi) Provide(ctx context.Context, cid string, id peer.ID, addrs []multiaddr.Multiaddr, key crypto.PrivKey) error {
	strs := make([]string, len(addrs))
	for i, a := range addrs {
		strs[i] = a.String()
	}

	payload := ipfsPutPayload{
		Keys:        []string{cid},
		Timestamp:   time.Now().UTC().UnixMilli(),
		AdvisoryTTL: int64(48 * time.Hour),
		ID:          id.String(),
		Addrs:       strs,
	}
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	hash := sha256.Sum256(payloadJSON)
	sigBytes, err := key.Sign(hash[:])
	if err != nil {
		return fmt.Errorf("sign: %w", err)
	}
	sigStr, err := multibase.Encode(multibase.Base64, sigBytes)
	if err != nil {
		return fmt.Errorf("encode sig: %w", err)
	}

	body := ipfsPutRequest{Providers: []ipfsPutRecord{{
		Schema:    "bitswap",
		Protocol:  "transport-bitswap",
		Signature: sigStr,
		Payload:   payload,
	}}}
	data, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("marshal body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut,
		api.baseURL+"/routing/v1/providers/", bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := api.cli.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("PUT %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

// FindClosestPeers queries GET /routing/v1/dht/closest/peers/{key}.
//
// Response format:
//
//	{"Peers":[{"ID":"12...","Addrs":["/ip4/..."],"Schema":"peer"}]}
//
// Uses existing dhtPeersResponse / dhtPeer types.
//
// Tracker availability:
//
//	trackers[0] ✅ (confirmed via V6 production use)
func (api *IpfsHttpTrackerApi) FindClosestPeers(ctx context.Context, key string) ([]FoundPeer, error) {
	if key == "" {
		return nil, fmt.Errorf("empty key")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		api.baseURL+"/routing/v1/dht/closest/peers/"+key, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := api.cli.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var pr dhtPeersResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		return nil, err
	}

	var out []FoundPeer
	for _, p := range pr.Peers {
		if p.ID == "" {
			continue
		}
		out = append(out, FoundPeer{PeerID: p.ID, Addrs: p.Addrs})
	}
	return out, nil
}

// FindPeers queries GET /routing/v1/peers/{peer-id}.
//
// Response format:
//
//	{"Peers":[{"ID":"12...","Addrs":["/ip4/..."],"Schema":"peer"}]}
//
// Uses existing dhtPeersResponse / dhtPeer types.
//
// Tracker availability:
//
//	trackers[0] ✅ (confirmed via lookupPeerOnHTTP)
func (api *IpfsHttpTrackerApi) FindPeers(ctx context.Context, peerID string) ([]FoundPeer, error) {
	if peerID == "" {
		return nil, fmt.Errorf("empty peerID")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet,
		api.baseURL+"/routing/v1/peers/"+peerID, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := api.cli.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var pr dhtPeersResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		return nil, err
	}

	var out []FoundPeer
	for _, p := range pr.Peers {
		if p.ID == "" {
			continue
		}
		out = append(out, FoundPeer{PeerID: p.ID, Addrs: p.Addrs})
	}
	return out, nil
}

func AdvertiseHTTP(ctx context.Context) {
	cid := StringToCID(currConfig.HubName)
	if cid == "" {
		log.Println("[advertise] StringToCID failed")
		return
	}

	api := NewIpfsHttpTrackerApi(trackers[0])
	ticker := time.NewTicker(advertiseInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if bootres == nil || bootres.Host == nil {
				continue
			}
			key := bootres.Host.Peerstore().PrivKey(bootres.Host.ID())
			if key == nil {
				log.Println("[advertise] no private key")
				continue
			}
			if err := api.Provide(ctx, cid, bootres.Host.ID(), bootres.Host.Addrs(), key); err != nil {
				log.Printf("[advertise] PUT error: %v", err)
			}

		case <-ctx.Done():
			return
		}
	}
}

func discoveryV4(ctx context.Context) {
	api := NewIpfsHttpTrackerApi(trackers[0])
	ticker := time.NewTicker(discoverInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if bootres == nil || bootres.Host == nil {
				continue
			}
			peers, err := api.FindProviders(ctx, StringToCID(currConfig.HubName))
			if err != nil {
				log.Printf("[discovery] query: %v", err)
				continue
			}
			log.Println("http content provider: peers for topic", len(peers), currConfig.HubName)
			myID := bootres.Host.ID()
			for _, p := range peers {
				pid, err := peer.Decode(p.PeerID)
				if err != nil {
					continue
				}
				if pid == myID {
					continue
				}
				if bootres.Host.Network().Connectedness(pid) == network.Connected {
					continue
				}
				var maddrs []multiaddr.Multiaddr
				for _, s := range p.Addrs {
					m, err := multiaddr.NewMultiaddr(s)
					if err != nil {
						continue
					}
					maddrs = append(maddrs, m)
				}
				if len(maddrs) == 0 {
					continue
				}
				info := peer.AddrInfo{ID: pid, Addrs: maddrs}
				if err := bootres.Host.Connect(ctx, info); err != nil {
					log.Printf("[discovery] connect %s: %v", pid.ShortString(), err)
				} else {
					log.Printf("[discovery] connected %s", pid.ShortString())
				}
			}

		case <-ctx.Done():
			return
		}
	}
}

type dhtPeer struct {
	ID    string   `json:"ID"`
	Addrs []string `json:"Addrs"`
}

type dhtPeersResponse struct {
	Peers []dhtPeer `json:"Peers"`
}

func reconnectFromPeerDB(ctx context.Context) {
	if bootres == nil || bootres.Host == nil || bootres.PeerDB == nil || bootres.PSO == nil {
		return
	}

	topicPeers := make(map[peer.ID]struct{})
	for _, pid := range bootres.PSO.ListPeers(currConfig.HubName) {
		topicPeers[pid] = struct{}{}
	}

	records := bootres.PeerDB.List()
	log.Printf("[peerdb] peers/records %v/%v %s", len(topicPeers), len(records), currConfig.HubName)
	for _, r := range records {
		if _, in := topicPeers[r.PeerID]; in {
			continue
		}
		if bootres.Host.Network().Connectedness(r.PeerID) != network.NotConnected {
			continue
		}
		if len(bootres.Host.Network().Conns()) > 20 {
			break
		}
		if len(r.Addrs) == 0 {
			continue
		}

		info := peer.AddrInfo{ID: r.PeerID, Addrs: r.Addrs}

		cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
		ctx2 := network.WithAllowLimitedConn(cctx, "peerdb-reconnect")
		if err := bootres.Host.Connect(ctx2, info); err != nil {
			cancel()
			log.Printf("[peerdb] reconnect %s: %v", r.PeerID.ShortString(), err)
			continue
		}
		cancel()
		peernum := len(bootres.PSO.ListPeers(currConfig.HubName))
		log.Printf("[peerdb] reconnected %s %s %v", r.PeerID.ShortString(), "peers", peernum)

		pctx, pcancel := context.WithTimeout(ctx, 10*time.Second)
		res := <-ping.Ping(pctx, bootres.Host, r.PeerID)
		pcancel()
		if res.Error != nil {
			log.Printf("[peerdb] ping %s: %v", r.PeerID.ShortString(), res.Error)
		} else {
			log.Printf("[peerdb] ping %s RTT: %s", r.PeerID.ShortString(), res.RTT)
			bootres.PeerDB.Update(r.PeerID, r.Addrs)
		}

		select {
		case <-time.After(5 * time.Second):
		case <-ctx.Done():
			return
		}
	}
}

func DiscoveryV6(ctx context.Context) {
	api := NewIpfsHttpTrackerApi(trackers[0])
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				reconnectFromPeerDB(ctx)
			case <-ctx.Done():
				return
			}
		}
	}()

	interval := discoverInterval

	for i := 0; ; i++ {
		if i == 0 {
			interval = discoverInterval / 3
		} else {
			interval = discoverInterval
		}
		select {
		case <-time.After(interval):
			if bootres == nil || bootres.Host == nil {
				continue
			}
			randomKey := fmt.Sprintf("r-%d", time.Now().UnixNano())
			cid := StringToCID(randomKey)
			if cid == "" {
				continue
			}

			peers, err := api.FindClosestPeers(ctx, cid)
			if err != nil {
				log.Printf("[discoveryV6] query: %v", err)
				continue
			}

			myID := bootres.Host.ID()
			for n, p := range peers {
				if n%3 == 1 {
					time.Sleep(1 * time.Second)
					continue
				}
				pid, err := peer.Decode(p.PeerID)
				if err != nil {
					continue
				}
				if pid == myID {
					continue
				}
				if bootres.Host.Network().Connectedness(pid) == network.Connected {
					continue
				}
				if len(bootres.Host.Network().Conns()) > 20 {
					break
				}
				var maddrs []multiaddr.Multiaddr
				for _, s := range p.Addrs {
					m, err := multiaddr.NewMultiaddr(s)
					if err != nil {
						continue
					}
					maddrs = append(maddrs, m)
				}
				if len(maddrs) == 0 {
					continue
				}
				info := peer.AddrInfo{ID: pid, Addrs: maddrs}
				if !IsGoodPeerAddr(info) {
					continue
				}
				if err := bootres.Host.Connect(ctx, info); err != nil {
					log.Printf("[discoveryV6] %v connect %s: %v", n, pid.ShortString(), err)
				} else {
					log.Printf("[discoveryV6] %v connected %s", n, pid.ShortString())
					// addrs, _ := queryObservedAddr(ctx, bootres.Host, pid)
					// _ = addrs
					pushToConnected(ctx, bootres.Host, pid, bootres.AddrMgr.GetAll())
					for _, s := range loadTargetPeers() {
						_ = s
						// tryStreamToTarget(ctx, s)
						// tryPingToTarget(ctx, s)
					}
					log.Println("topic peers for reddit", len(bootres.PSO.ListPeers("reddit")))
				}
				time.Sleep(5 * time.Second)
			}

			time.Sleep(discoverInterval / 2)
		case <-ctx.Done():
			return
		}
	}
}

func tryStreamToTarget(ctx context.Context, targetID string) error {
	pid, err := peer.Decode(targetID)
	if err != nil {
		return fmt.Errorf("decode target: %w", err)
	}
	label := pid.ShortString()

	err = doStream(ctx, pid, label)
	if err == nil {
		return nil
	}
	if strings.Contains(err.Error(), "no address") {
	}
	log.Printf("[discoveryV6] newstream %s: %v", label, err)
	return err
}

func doStream(ctx context.Context, pid peer.ID, label string) error {
	ctx2 := network.WithAllowLimitedConn(ctx, "discoveryV6-stream")
	s, err := bootres.Host.NewStream(ctx2, pid, "/d2hub/echo/1.0")
	if err != nil {
		return err
	}
	defer s.Close()
	log.Printf("[discoveryV6] newstream to %s OK", label)

	_, err = s.Write([]byte("hello from DiscoveryV6"))
	if err != nil {
		return err
	}
	if sc, ok := s.(interface{ CloseWrite() error }); ok {
		sc.CloseWrite()
	}
	buf, err := io.ReadAll(s)
	if err != nil {
		return err
	}
	log.Printf("[discoveryV6] reply: %s", string(buf))
	return nil
}

func tryPingToTarget(ctx context.Context, targetID string) error {
	pid, err := peer.Decode(targetID)
	if err != nil {
		return fmt.Errorf("decode target: %w", err)
	}
	label := pid.ShortString()

	res := <-ping.Ping(ctx, bootres.Host, pid)
	errmsg := "OK"
	if res.Error != nil {
		arr := strings.Split(res.Error.Error(), ": ")
		errmsg = arr[len(arr)-1]
	}
	log.Printf("[discoveryV6] ping %s RTT: %s, err: %v", label, res.RTT, errmsg)
	if res.Error != nil {
		return fmt.Errorf("ping %s: %w", label, res.Error)
	}
	return nil
}

func rawFindNode(ctx context.Context, h host.Host, target peer.ID, queryPeer peer.ID) ([]peer.AddrInfo, error) {
	s, err := h.NewStream(ctx, queryPeer, "/ipfs/kad/1.0.0")
	if err != nil {
		return nil, fmt.Errorf("new stream: %w", err)
	}
	defer s.Close()

	req := &dht_pb.Message{
		Type: dht_pb.Message_FIND_NODE,
		Key:  []byte(target),
	}

	wr := protoio.NewDelimitedWriter(s)
	if err := wr.WriteMsg(req); err != nil {
		s.Reset()
		return nil, fmt.Errorf("write: %w", err)
	}

	rd := protoio.NewDelimitedReader(s, network.MessageSizeMax)
	var resp dht_pb.Message
	if err := rd.ReadMsg(&resp); err != nil {
		return nil, fmt.Errorf("read: %w", err)
	}

	closer := dht_pb.PBPeersToPeerInfos(resp.GetCloserPeers())

	found := false
	for _, cp := range closer {
		if cp == nil {
			continue
		}
		// log.Printf("[rawFindNode] closer: %s addrs=%d",
		// cp.ID.ShortString(), len(cp.Addrs))
		if cp.ID == target && len(cp.Addrs) > 0 {
			found = true
			//log.Printf("[rawFindNode] target %s FOUND in closer peers, connecting",
			//	target.ShortString())
			if h.Network().Connectedness(target) != network.Connected {
				// h.Connect(ctx, *cp)
			}
		}
	}
	if !found {
		log.Printf("[rawFindNode] target %s NOT in closer peers",
			target.ShortString())
	}

	out := make([]peer.AddrInfo, len(closer))
	for i, p := range closer {
		if p != nil {
			out[i] = *p
		}
	}
	return out, nil
}

func lookupPeerOnHTTP(peerIDStr string) []multiaddr.Multiaddr {
	req, err := http.NewRequest(http.MethodGet,
		trackers[0]+"/routing/v1/peers/"+peerIDStr, nil)
	if err != nil {
		return nil
	}
	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()

	var result struct {
		Peers []struct {
			ID    string   `json:"ID"`
			Addrs []string `json:"Addrs"`
		} `json:"Peers"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil
	}

	for _, p := range result.Peers {
		if p.ID != peerIDStr {
			continue
		}
		var maddrs []multiaddr.Multiaddr
		for _, s := range p.Addrs {
			m, err := multiaddr.NewMultiaddr(s)
			if err != nil {
				continue
			}
			maddrs = append(maddrs, m)
		}
		return maddrs
	}
	return nil
}
