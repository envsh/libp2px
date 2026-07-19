//go:build !nodht

package p2put

import (
	"context"
	"fmt"
	"hash/fnv"
	"log"
	"sync"
	"time"

	dht "github.com/libp2p/go-libp2p-kad-dht"
	discovery2 "github.com/libp2p/go-libp2p/core/discovery"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	corerouting "github.com/libp2p/go-libp2p/core/routing"
	routing "github.com/libp2p/go-libp2p/p2p/discovery/routing"
	discovery "github.com/libp2p/go-libp2p/p2p/discovery/util"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
)

func init() {
	if len(libp2pBootstrap) != len(dht.DefaultBootstrapPeers) {
		log.Println("Need update bootstrap data")
	}
}

// only !IsMobile
func (bsres *BootNode) bootDHT(ctx context.Context) (any, error) {
	h := bsres.Host

	bootaddrs := libp2pBootstrap
	if true {
		bootaddrs = resolvedBootstrapNodes
	}
	bootstrapInfos := filterConvertBootstrapInfos(bootaddrs)

	fmt.Printf("[+] %d bootstrap peers ready\n\n", len(bootaddrs))
	if len(bootstrapInfos) == 0 {
		return nil, fmt.Errorf("no valid bootstrap nodes")
	}

	fmt.Println("=== Phase 3: DHT Bootstrap ===")
	fmt.Println("[*] Starting Kademlia DHT in client mode...")

	bootCtx, cancel := context.WithTimeout(ctx, 32*time.Second)
	defer cancel()
	kadDHT, err := dht.New(bootCtx, h,
		dht.Mode(dht.ModeClient),
		dht.BootstrapPeers(bootstrapInfos...),
		// dht.DisableAutoRefresh(),
		dht.Concurrency(3), // ← 并发从 10 降到 3
		dht.RoutingTableRefreshPeriod(5*time.Minute), // ← 再加这行, 默认10min
	)
	if err != nil {
		return nil, fmt.Errorf("create DHT: %w", err)
	}

	if err := kadDHT.Bootstrap(bootCtx); err != nil {
		fmt.Printf("  [!] DHT bootstrap warning: %v\n", err)
		return nil, err
	}
	errch := kadDHT.RefreshRoutingTable()
	btime := time.Now()
	<-errch

	log.Println("[*] Waiting DHT routing table online...", time.Since(btime))
	routingDiscovery := routing.NewRoutingDiscovery(kadDHT)
	testCID := currConfig.HubName                              // "libp2p-bootstrap-test"
	rettl := discovery2.TTL(10 * time.Minute)                  // 3h
	discovery.Advertise(ctx, routingDiscovery, testCID, rettl) // broadcast self
	discovery.Advertise(ctx, routingDiscovery, officalHubName, rettl)

	if false {
		pingService := ping.NewPingService(h)
		_ = pingService
	}

	bsres.DHT = kadDHT
	bsres.Discovery = routingDiscovery
	return nil, nil
}

// only find HubName
func (bootres *BootNode) myDiscoveryV3() {
	rd, _ := bootres.Discovery.(*routing.RoutingDiscovery)
	// dht := bootres.DHT
	sec100 := 120 * time.Second
	known := make(map[string]peer.AddrInfo)

	for i := 0; ; i++ {
		time.Sleep(3 * time.Second)
		log.Println("start DHT finding...", i)
		tags := []string{currConfig.HubName, officalHubName}
		var result []peer.AddrInfo
		for _, tag := range tags {
			r := findAndConnect(tag, rd, 0)
			result = append(result, r...)
		}
		validcnt := 0
		for _, p := range result {
			log.Println(p.ID.ShortString(), p.Addrs)
			if len(p.Addrs) == 0 {
				if _, ok := known[p.ID.String()]; !ok {
					known[p.ID.String()] = p
				}
			} else {
				known[p.ID.String()] = p
				validcnt += 1
			}
			if len(known[p.ID.String()].Addrs) == 0 {
				ps := bootres.Host.Peerstore()
				p.Addrs = ps.Addrs(p.ID)
			}
		}
		log.Println("found peers count:", len(known), "valid", validcnt, "round", i)
		if i < 3 && len(result) == 0 {
			time.Sleep(time.Duration(2+i) * time.Second)
			continue
		}

		btime := time.Now()
		newconnfixer(known, sec100).dofix()
		dur := time.Since(btime)

		log.Println("refresh mydht info...", bootres.PeerID.ShortString())
		ctx1 := context.Background()
		ctx2, cancel2 := context.WithTimeout(ctx1, 3*time.Second)

		// 替代 RefreshRoutingTable()
		// 轻量：只更新自己的路由信息
		dhtNode, _ := bootres.DHT.(*dht.IpfsDHT)
		if dhtNode != nil {
			dhtNode.FindPeer(ctx2, bootres.PeerID)
		}
		// bootres.DHT.Provide(context.Background(), currConfig.HubName, true)

		// 或
		// bootres.DHT.GetClosestPeers(ctx, bootres.PeerID.String())
		cancel2()

		if dhtNode != nil {
			dhtNode.GetClosestPeers(context.Background(), bootres.PeerID.String())
		}
		if dur > sec100 {
			continue
		}
		time.Sleep(sec100 - dur)
	}
}

// 搜索所有的tags/topics，流量太大
func myDiscoveryV2ddd() {
	type tagState struct {
		nextAt time.Time
		busy   bool
	}

	rd, _ := bootres.Discovery.(*routing.RoutingDiscovery)
	var tagStates sync.Map

	for {
		time.Sleep(300 * time.Millisecond)
		discoveryTags.Range(func(key, _ any) bool {
			tag := key.(string)
			st, loaded := tagStates.Load(tag)
			if !loaded {
				h := fnv.New32a()
				h.Write([]byte(tag))
				phase := time.Duration(h.Sum32()%20) * time.Second
				st = &tagState{nextAt: time.Now().Add(-20*time.Second + phase)}
				tagStates.Store(tag, st)
			}
			s := st.(*tagState)
			if s.busy || time.Since(s.nextAt) < 0 {
				return true
			}
			s.busy = true
			s.nextAt = time.Now().Add(30 * time.Second)
			go func(tag string) {
				findAndConnect(tag, rd, 0)
				if v, _ := tagStates.Load(tag); v != nil {
					v.(*tagState).busy = false
				}
			}(tag)
			return true
		})
	}
}

func findAndConnect(tag string, rd *routing.RoutingDiscovery, limit int) []peer.AddrInfo {
	findCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	// 连接够多了，不查
	if len(bootres.Host.Network().Conns()) > 9 {
		// return
	}

	if limit <= 0 {
		limit = 5
	}
	if rd == nil {
		return nil
	}
	peerChan, err := rd.FindPeers(findCtx, tag,
		discovery2.Limit(limit), // ← 只取 10 个结果
	)
	if err != nil {
		return nil
	}
	var found []peer.AddrInfo
	var foundm = make(map[string]peer.AddrInfo)
	for p := range peerChan { // 有重复值
		if p.ID == bootres.Host.ID() || p.ID == "" {
			continue
		}
		op := foundm[p.ID.String()]
		p.Addrs = append(p.Addrs, op.Addrs...)
		foundm[p.ID.String()] = p
		if true {
			continue
		}

		if bootres.Host.Network().Connectedness(p.ID) != network.Connected {
			// 每连一个前再检查一次，防止批量连
			// if len(bootres.Host.Network().Conns()) > 12 { break }

			// 每两个连接之间间隔 2s
			time.Sleep(3 * time.Second)
			dialCtx, dialCancel := context.WithTimeout(context.Background(), 10*time.Second)
			dialCtx = withBackoffBypass(dialCtx, bootres.Host, p.ID)
			t0 := time.Now()
			if err := bootres.Host.Connect(dialCtx, p); err != nil {
				log.Printf("[discovery] connect %s: %v", p.ID.ShortString(), err)
			} else {
				elapsed := time.Since(t0)
				log.Printf("[discovery] connected to %s in %v", p.ID.ShortString(), elapsed)
				updatePeerLatency(p.ID, elapsed)
			}
			dialCancel()
		}
	}
	for _, p := range foundm {
		found = append(found, p)
	}
	return found
}

func myDiscoveryV1(bootCtx context.Context, routingDiscovery *routing.RoutingDiscovery, testCID string, myID peer.ID) (discoveredSet map[peer.ID]struct{}) {
	findCtx, findCancel := context.WithTimeout(bootCtx, 10*time.Second)
	defer findCancel()
	peerChan, err := routingDiscovery.FindPeers(findCtx, testCID)
	if err == nil {
		for p := range peerChan {
			if p.ID == myID || p.ID == "" {
				continue
			}
			discoveredSet[p.ID] = struct{}{}
		}
	} else {
		log.Println(err)
	}
	return
}

func myDumpBoot(h host.Host, dhtNode any) {
	dhtsz := 0
	if realDHT, ok := dhtNode.(*dht.IpfsDHT); ok && realDHT != nil {
		dhtsz = realDHT.RoutingTable().Size()
	}

	conns := GetCurrConns(h)

	log.Printf("conns %v, dht %v", len(conns), dhtsz)
	log.Println()
}

// ===== DHT wrapper functions for utapi.go / fixconn.go =====

func dhtFindPeers(tag string, limit int) ([]FoundPeer, error) {
	if tag == "" {
		tag = currConfig.HubName
	}
	if limit <= 0 {
		limit = 5
	}
	if bootres.Discovery == nil {
		return nil, fmt.Errorf("discovery not ready")
	}
	rd, ok := bootres.Discovery.(*routing.RoutingDiscovery)
	if !ok {
		return nil, fmt.Errorf("discovery not initialized")
	}
	findCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	peerChan, err := rd.FindPeers(findCtx, tag, discovery2.Limit(limit))
	if err != nil {
		return nil, err
	}
	var out []FoundPeer
	for p := range peerChan {
		if p.ID == bootres.PeerID || p.ID == "" {
			continue
		}
		addrs := make([]string, len(p.Addrs))
		for i, a := range p.Addrs {
			addrs[i] = a.String()
		}
		out = append(out, FoundPeer{PeerID: p.ID.String(), Addrs: addrs})
	}
	if out == nil {
		out = []FoundPeer{}
	}
	return out, nil
}

func dhtFindPeer(ctx context.Context, pid peer.ID) (peer.AddrInfo, error) {
	if bootres.DHT == nil {
		return peer.AddrInfo{}, fmt.Errorf("dht not ready")
	}
	d, ok := bootres.DHT.(*dht.IpfsDHT)
	if !ok {
		return peer.AddrInfo{}, fmt.Errorf("dht not initialized")
	}
	return d.FindPeer(ctx, pid)
}

func dhtGetKV(ctx context.Context, key string) ([]byte, error) {
	if bootres.DHT == nil {
		return nil, fmt.Errorf("libp2p not ready")
	}
	d, ok := bootres.DHT.(*dht.IpfsDHT)
	if !ok {
		return nil, fmt.Errorf("dht not initialized")
	}
	getCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	val, err := d.GetValue(getCtx, key)
	if err != nil {
		return nil, err
	}
	if len(val) == 0 {
		return nil, corerouting.ErrNotFound
	}
	return val, nil
}

func dhtPutKV(ctx context.Context, key string, value []byte) error {
	if bootres.DHT == nil {
		return fmt.Errorf("libp2p not ready")
	}
	d, ok := bootres.DHT.(*dht.IpfsDHT)
	if !ok {
		return fmt.Errorf("dht not initialized")
	}

	// not support put custom key, opendht does
	// {"error":"create temp dht: protocol prefix /ipfs must have exactly two namespaced validators - /pk and /ipns"}
	tempDHT, err := dht.New(ctx, bootres.Host, dht.Mode(dht.ModeClient))
	if err != nil {
		return fmt.Errorf("create temp dht: %w", err)
	}
	defer tempDHT.Close()

	_ = tempDHT.Bootstrap(ctx)

	waitCtx, waitCancel := context.WithTimeout(ctx, 30*time.Second)
	defer waitCancel()
	for {
		if tempDHT.RoutingTable().Size() >= 3 {
			break
		}
		select {
		case <-waitCtx.Done():
			return fmt.Errorf("routing table too small: %d, need >= 3", tempDHT.RoutingTable().Size())
		case <-time.After(500 * time.Millisecond):
		}
	}

	putCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	key2 := "/pk/" + key
	println(key2)
	if err := tempDHT.PutValue(putCtx, key2, value); err != nil {
		return fmt.Errorf("put value: %w", err)
	}

	getCtx, cancel2 := context.WithTimeout(ctx, 15*time.Second)
	defer cancel2()
	if _, err := d.GetValue(getCtx, key2); err != nil {
		return fmt.Errorf("verify failed: %w", err)
	}

	return nil
}

func dhtDelKV(ctx context.Context, key string) error {
	if bootres.DHT == nil {
		return fmt.Errorf("libp2p not ready")
	}
	d, ok := bootres.DHT.(*dht.IpfsDHT)
	if !ok {
		return fmt.Errorf("dht not initialized")
	}
	putCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	return d.PutValue(putCtx, key, []byte{})
}

func dhtCollectDHT() (int, []string) {
	if bootres.DHT == nil {
		return 0, nil
	}
	d, ok := bootres.DHT.(*dht.IpfsDHT)
	if !ok || d == nil {
		return 0, nil
	}
	rt := d.RoutingTable()
	peers := rt.ListPeers()
	strs := make([]string, len(peers))
	for i, p := range peers {
		strs[i] = p.String()
	}
	return rt.Size(), strs
}
