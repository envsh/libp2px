//go:build nopubsub

package p2put

import (
	"context"
	"fmt"
	"log"
	"sort"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

func BuildGossipSub(_ context.Context, _ host.Host, _ []peer.AddrInfo) (any, error) {
	return nil, nil
}

func getPSOTopics() []string {
	return nil
}

func getSubscribedTopicPeers() map[string][]peer.ID {
	return nil
}

func getOrSubscribeTopic(topic string) (any, error) {
	return nil, fmt.Errorf("pubsub disabled")
}

func PublishTopic(topic string, data []byte) error {
	return fmt.Errorf("pubsub disabled")
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
	return nil
}

