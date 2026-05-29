package p2put

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
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
	ticker := time.NewTicker(discoverInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
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
			for _, p := range result.Peers {
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
					log.Printf("[discoveryV6] connect %s: %v", pid.ShortString(), err)
				} else {
					log.Printf("[discoveryV6] connected %s", pid.ShortString())
					tryStreamToTarget(ctx, "12D3KooWDVExaeKp1YzYvhS7E6oZDdDnEB3HENS9VrYp3vKME7m1")
					tryStreamToTarget(ctx, "12D3KooWSgyQhqayreZ6UequLq3ZGxJm1WG4tyszD29ps8zNtYLT")
					tryStreamToTarget(ctx, "12D3KooWHXjoE8cMhPPD7JaUGHHiXCNLHQcbgUQrXFc788oq6ahm")
				}
				time.Sleep(3 * time.Second)
			}

		case <-ctx.Done():
			return
		}
	}
}

func tryStreamToTarget(ctx context.Context, targetID string) {
	pid, err := peer.Decode(targetID)
	if err != nil {
		log.Printf("[discoveryV6] decode target: %v", err)
		return
	}
	label := pid.ShortString()

	err = doStream(ctx, pid, label)
	if err == nil {
		return
	}
	if strings.Contains(err.Error(), "no address") {
		log.Printf("[discoveryV6] lookup %s on HTTP", label)
		addrs := lookupPeerOnHTTP(targetID)
		if len(addrs) > 0 {
			bootres.Host.Peerstore().AddAddrs(pid, addrs, 10*time.Minute)
			err = doStream(ctx, pid, label)
			if err == nil {
				return
				}
				time.Sleep(3 * time.Second)
			}
	}
	log.Printf("[discoveryV6] newstream %s: %v", label, err)
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
	buf, err := io.ReadAll(s)
	if err != nil {
		return err
	}
	log.Printf("[discoveryV6] reply: %s", string(buf))
	return nil
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
