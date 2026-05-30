package p2put

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	dht_pb "github.com/libp2p/go-libp2p-kad-dht/pb"
	"github.com/libp2p/go-msgio/protoio"
	"github.com/multiformats/go-multiaddr"
)

var trackers = []string{
	"https://delegated-ipfs.dev", // 0: GET only
	"https://routing.lol",        // 1: PUT 500
	"https://peers.pleb.bot",    // 2: PUT+GET
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

type providerPayload struct {
	Addrs []string `json:"Addrs"`
	ID    string   `json:"ID"`
	Keys  []string `json:"Keys"`
}

type providerEntry struct {
	Schema   string          `json:"Schema"`
	Protocol string          `json:"Protocol"`
	Payload  providerPayload `json:"Payload"`
}

type providersResponse struct {
	Providers []providerEntry `json:"Providers"`
}

func AdvertiseHTTP(ctx context.Context) {
	cid := StringToCID(currConfig.HubName)
	if cid == "" {
		log.Println("[advertise] StringToCID failed")
		return
	}

	ticker := time.NewTicker(advertiseInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if bootres == nil || bootres.Host == nil {
				continue
			}
			addrs := bootres.Host.Addrs()
			strs := make([]string, len(addrs))
			for i, a := range addrs {
				strs[i] = a.String()
			}
			log.Println("regme", cid, currConfig.HubName, len(addrs))

			body := providersResponse{Providers: []providerEntry{{
				Schema:   "peer",
				Protocol: "transport-bitswap",
				Payload: providerPayload{
					Addrs: strs,
					ID:    bootres.Host.ID().String(),
					Keys:  []string{cid},
				},
			}}}
			data, _ := json.Marshal(body)

			req, err := http.NewRequestWithContext(ctx, http.MethodPut,
				trackerURL()+"/routing/v1/providers/", bytes.NewReader(data))
			if err != nil {
				log.Printf("[advertise] create req: %v", err)
				continue
			}
			req.Header.Set("Content-Type", "application/json")

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				log.Printf("[advertise] PUT error: %v", err)
				continue
			}
			resp.Body.Close()
			if resp.StatusCode >= 300 {
				log.Printf("[advertise] PUT %d", resp.StatusCode)
			}

		case <-ctx.Done():
			return
		}
	}
}

func discoveryV4(ctx context.Context) {
	ticker := time.NewTicker(discoverInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if bootres == nil || bootres.Host == nil {
				continue
			}
			peers, err := findProviders(trackerURL(), currConfig.HubName)
			if err != nil {
				log.Printf("[discovery] query: %v", err)
				continue
			}
			log.Println("peers", len(peers), currConfig.HubName)
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

func findProviders(baseURL, hubname string) ([]FoundPeer, error) {
	cid := StringToCID(hubname)
	if cid == "" {
		return nil, fmt.Errorf("StringToCID returned empty for hubname %q", hubname)
	}

	req, err := http.NewRequest(http.MethodGet,
		baseURL+"/routing/v1/providers/"+cid, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var pr providersResponse
	if err := json.NewDecoder(resp.Body).Decode(&pr); err != nil {
		return nil, err
	}

	var out []FoundPeer
	for _, prov := range pr.Providers {
		if prov.Schema != "peer" || prov.Payload.ID == "" {
			continue
		}
		out = append(out, FoundPeer{
			PeerID: prov.Payload.ID,
			Addrs:  prov.Payload.Addrs,
		})
	}
	return out, nil
}

type dhtPeer struct {
	ID    string   `json:"ID"`
	Addrs []string `json:"Addrs"`
}

type dhtPeersResponse struct {
	Peers []dhtPeer `json:"Peers"`
}

func DiscoveryV6(ctx context.Context) {
	interval := discoverInterval

	for i := 0; ; i++ {
		if i == 0 {
			interval = discoverInterval/3
		} else {
			interval = discoverInterval
		}
		select {
		case <- time.After(interval):
			if bootres == nil || bootres.Host == nil {
				continue
			}
			randomKey := fmt.Sprintf("r-%d", time.Now().UnixNano())
			cid := StringToCID(randomKey)
			if cid == "" {
				continue
			}
			url := trackers[0] + "/routing/v1/dht/closest/peers/" + cid

			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			if err != nil {
				log.Printf("[discoveryV6] create req: %v", err)
				continue
			}
			req.Header.Set("Accept", "application/json")

			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				log.Printf("[discoveryV6] GET error: %v", err)
				continue
			}

			var result dhtPeersResponse
			if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
				resp.Body.Close()
				log.Printf("[discoveryV6] decode: %v", err)
				continue
			}
			resp.Body.Close()

			myID := bootres.Host.ID()
			for n, p := range result.Peers {
				if n%3==1 { time.Sleep(1*time.Second); continue }
				pid, err := peer.Decode(p.ID)
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
					log.Println("topic peers", len(bootres.PSO.ListPeers("reddit")))
				}
				time.Sleep(5 * time.Second)
			}

			time.Sleep(discoverInterval/2)
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
		if cp == nil { continue }
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
