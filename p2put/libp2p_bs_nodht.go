//go:build nodht

package p2put

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"
)

func init() {
}

// only !IsMobile
func (bsres *BootNode) bootDHT(ctx context.Context) (any, error) {
	return nil, nil
}

// only find HubName
func (bootres *BootNode) myDiscoveryV3() {
	for {
		time.Sleep(120 * time.Second)
		log.Println("DHT disabled (build with nodht)")
	}
}

// 搜索所有的tags/topics，流量太大
func myDiscoveryV2ddd() {
	select {}
}

func findAndConnect(tag string, rd any, limit int) []peer.AddrInfo {
	return nil
}

func myDiscoveryV1(bootCtx context.Context, routingDiscovery any, testCID string, myID peer.ID) (discoveredSet map[peer.ID]struct{}) {
	return nil
}

func myDumpBoot(h host.Host, dhtNode any) {
	conns := GetCurrConns(h)
	log.Printf("conns %v", len(conns))
	log.Println()
}

// ===== DHT wrapper functions (stubs) =====

func dhtFindPeers(tag string, limit int) ([]FoundPeer, error) {
	return nil, fmt.Errorf("DHT disabled (build with nodht)")
}

func dhtFindPeer(ctx context.Context, pid peer.ID) (peer.AddrInfo, error) {
	return peer.AddrInfo{}, fmt.Errorf("DHT disabled (build with nodht)")
}

func dhtGetKV(ctx context.Context, key string) ([]byte, error) {
	return nil, fmt.Errorf("DHT disabled (build with nodht)")
}

func dhtPutKV(ctx context.Context, key string, value []byte) error {
	return fmt.Errorf("DHT disabled (build with nodht)")
}

func dhtDelKV(ctx context.Context, key string) error {
	return fmt.Errorf("DHT disabled (build with nodht)")
}

func dhtCollectDHT() (int, []string) {
	return 0, nil
}
