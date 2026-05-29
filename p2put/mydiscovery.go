package p2put

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
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
