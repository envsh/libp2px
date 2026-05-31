////go:build libp2p

package p2put

import (
	"context"
	"crypto/ed25519"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	// "flag"
	"fmt"
	"hash/fnv"
	// "math/rand"
	"reflect"
	"net"
	"os"
	"log"
	"strings"
	// "slices"
	// "maps"
	"sync"
	"time"

	"github.com/envsh/toxera/fedkey"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	discovery2 "github.com/libp2p/go-libp2p/core/discovery"
	"github.com/libp2p/go-libp2p/p2p/discovery/routing"
	"github.com/libp2p/go-libp2p/p2p/host/autorelay"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	"github.com/libp2p/go-libp2p/core/metrics"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/libp2p/go-libp2p/p2p/transport/websocket"
	discovery "github.com/libp2p/go-libp2p/p2p/discovery/util"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	dht "github.com/libp2p/go-libp2p-kad-dht"
	"github.com/libp2p/go-libp2p-pubsub"
	"github.com/multiformats/go-multiaddr"
)

////////////

type Libp2p struct {

}

// var _ = regme(&Libp2p{})

func (o *Libp2p) Start() error {
	return nil
}

func (o *Libp2p) Stop() error {
	return nil
}

func (o *Libp2p) Info() string {
	return "{}"
}

func MainLibp2p(cfg Config) {
	mainLibp2p(cfg)
}

//////


const (
	defaultListenPort = 4001
	p2pServiceNode    = 1
	p2pServiceChat    = 1 << 24

	IpfsPingProtocl = "/ipfs/ping/1.0.0"
	IpfsIdProtocol = "/ipfs/id/1.0.0"
	RelayHopProtocol  = protocol.ID("/libp2p/circuit/relay/0.2.0/hop")
	RelayStopProtocol = protocol.ID("/libp2p/circuit/relay/0.2.0/stop")

	minPeerCount = 16
)

type Libp2pAddrInfo struct {
	Addr        multiaddr.Multiaddr
	IsRelay     bool
	IsPrivateIP bool
	IP          net.IP
}


var libp2pBootstrap = []string{
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
	"/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
}

var peerstorePath = "/tmp/libp2p_peerstore.json"
var savedPeerstoreSum string

func init() {
	log.SetFlags((log.Flags() | log.Lshortfile | log.Ltime) &^ log.Ldate)
	if len(libp2pBootstrap) != len(dht.DefaultBootstrapPeers) {
		log.Println("Need update bootstrap data")
	}
}

type BootNode struct {
	Host          host.Host
	DHT           *dht.IpfsDHT
	PSO           *pubsub.PubSub
	Bwc           metrics.Reporter
	PeerID        peer.ID
	AddrMgr       *AddrManager
	PubkeyHex     string
	BootTime      time.Duration
	NATStatus     network.Reachability
	Discovery     *routing.RoutingDiscovery
	PeerDB        *PeerDB
	RelayPool     *RelayPool
}

// 缓存文件格式: map[string][]string (原始地址 → 解析后的地址列表)
var dnsaddrsResultFile = "/tmp/libp2p_bootstrap_dnsaddrs.json"
var dnsaddrsCacheDur = 4*3600*time.Second

func loadOrResolveAllDNSAddrs() {
	if data, err := os.ReadFile(dnsaddrsResultFile); err == nil {
		var info os.FileInfo
		info, err = os.Stat(dnsaddrsResultFile)
		if err == nil && time.Since(info.ModTime()) < dnsaddrsCacheDur {
			var resolved map[string][]string
			if err = json.Unmarshal(data, &resolved); err == nil {
				for _, addrs := range resolved {
					for _, addr := range addrs {
						if strings.Contains(addr, ":") ||
							strings.Contains(addr, "/udp/") {
							continue
						}
						if !containsAddr(resolvedBootstrapNodes, addr) {
							resolvedBootstrapNodes = append(resolvedBootstrapNodes, addr)
						}
					}
				}
				return
			}
		}
	}

	ctx := context.Background()
	resolved := resolveAllDNSAddrsQuiet(ctx, libp2pBootstrap)

	if data, err := json.Marshal(resolved); err == nil {
		err = os.WriteFile(dnsaddrsResultFile, data, 0644)
		if err != nil { panic(err) }
	}

	for _, addrs := range resolved {
		for _, addr := range addrs {
			if strings.Contains(addr, ":") ||
				strings.Contains(addr, "/udp/") {
				continue
			}
			if !containsAddr(resolvedBootstrapNodes, addr) {
				resolvedBootstrapNodes = append(resolvedBootstrapNodes, addr)
			}
		}
	}
}

func mainLibp2p(cfg Config) {
	fmt.Println("=== DNSADDR 解析结果汇总 ===")
	fmt.Printf("[*] 原始 bootstrap 地址: %d 个\n", len(libp2pBootstrap))
	// resolveAllDNSAddrsInit()
	loadOrResolveAllDNSAddrs()
	fmt.Printf("[*] 解析后的额外地址: %d 个\n", len(resolvedBootstrapNodes))
	fmt.Println()

	if len(resolvedBootstrapNodes) > 0 {
		fmt.Println("[*] 解析后的地址列表:")
		for i, addr := range resolvedBootstrapNodes {
			fmt.Printf("  [%02d] %s\n", i+1, addr)
		}
	}
	fmt.Println()

	if cfg.fset.Parsed() {
		log.Println("KeyFile", cfg.KeyFile)
		log.Println("ListenPort", cfg.ListenPort)
	}
	currConfig = cfg
	currConfig.fset = nil

	res, err := Bootstrap(context.Background(), currConfig)
	if err != nil {
		panic(err)
	}

	myDumpBoot(res.Host, res.DHT)
	bootres = res
	loadPeerstore(res.Host, peerstorePath)
	replayProtocols()

	res.PeerDB = NewPeerDB(600 * time.Minute)
	NewPeerGossip(res.Host, res.PSO, res.PeerDB, currConfig.HubName).Start(context.Background())
	if currConfig.IsMobile {
		go AdvertiseHTTP(context.Background())
		go discoveryV4(context.Background())
		go DiscoveryV6(context.Background())
	} else {
		go res.myDiscoveryV3()
		//go myDiscoveryV2()
	}

	for _, topic := range currConfig.Topics {
		if len(topic) <= 0 { continue }
		getOrSubscribeTopic(topic)
	}

	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			if err := savePeerstore(peerstorePath); err != nil {
				log.Printf("[peerstore] save error: %v\n", err)
			}
			cleanPeerstore()
		}
	}()

	mode := "Server"
	if currConfig.IsMobile {
		mode = "Light"
	}
	log.Printf("%#v\n", currConfig)
	log.Printf("Run node in *%v* mode\n", mode)
	select {}
}

func Bootstrap(ctx context.Context, cfg Config) (*BootNode, error) {
	start := time.Now()

	fmt.Println("=== Phase 1: Key Loading ===")
	kr, err := fedkey.LoadKeyRing(cfg.KeyFile, true)
	if err != nil {
		return nil, fmt.Errorf("load keyring: %w", err)
	}
	fmt.Println("[+] Loaded key from:", cfg.KeyFile)

	edPriv := kr.BTDHTKey()
	pubKey := edPriv.Public().(ed25519.PublicKey)
	pubHex := hex.EncodeToString(pubKey)
	fmt.Printf("    My pubkey: %s...\n\n", pubHex[:32])

	libp2pPriv, err := crypto.UnmarshalEd25519PrivateKey(edPriv)
	if err != nil {
		return nil, fmt.Errorf("unmarshal privkey: %w", err)
	}

	staticRelays := parseStringAddrs(manualRelays)
	// staticRelays := parseStaticRelays()
	fmt.Printf("[+] Parsed %d static relay candidates\n", len(staticRelays))

	listenAddr, _ := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", cfg.ListenPort))

	fmt.Println("=== Phase 1.5: Creating Host with Relay/AutoRelay/HolePunching ===")

	// static relay seems fixed relay, not auto find more relays
	autoRelayOpt := libp2p.EnableAutoRelayWithStaticRelays(
		staticRelays,
		autorelay.WithNumRelays(5),
		autorelay.WithMinCandidates(5),
		autorelay.WithBootDelay(60*time.Second),
		autorelay.WithBackoff(3*time.Minute),
	)
	if currConfig.IsMobile {
		autoRelayOpt = libp2p.EnableAutoRelayWithStaticRelays(
			staticRelays,
			// dht.GetDefaultBootstrapPeerAddrInfos(),
			autorelay.WithNumRelays(2),
			autorelay.WithMinCandidates(3),
			autorelay.WithBootDelay(30*time.Second),
			autorelay.WithBackoff(3*time.Minute),
		)
	}
	bwc := metrics.NewBandwidthCounter()

	cm, err := connmgr.NewConnManager(
		100,
		200,
		connmgr.WithGracePeriod(30*time.Second),
	)
	if err != nil {
		return nil, fmt.Errorf("create connmgr: %w", err)
	}

	log.Println("using static relays:", staticRelays)
	h, err := libp2p.New(
		libp2p.Identity(libp2pPriv),
		libp2p.ListenAddrs(listenAddr),
		libp2p.ResourceManager(myResourceManager()),
		libp2p.ConnectionManager(cm),

		libp2p.EnableRelay(),
		autoRelayOpt,
		libp2p.EnableHolePunching(),

		libp2p.Transport(tcp.NewTCPTransport),
		libp2p.Transport(websocket.New),
		libp2p.UserAgent("universal-connectivity/go-peer"),

		libp2p.BandwidthReporter(bwc),

		libp2p.AddrsFactory(myAddrsFactory),
	)
	if err != nil {
		return nil, fmt.Errorf("create libp2p host: %w", err)
	}

	rp := NewRelayPool(WeightConfig{})
	for _, r := range staticRelays {
		rp.Add(r.Addrs[0].String() + "/p2p/" + r.ID.String())
		rp.Protect(r.ID)
	}

	for _, r := range staticRelays {
		h.ConnManager().Protect(r.ID, "relay")
	}
	if false {
		go watchStaticRelays(context.Background(), h, staticRelays)
	}else {
		go relayPoolManager(context.Background(), h, rp, 3)
	}

	myID := h.ID()
	fmt.Printf("[+] Host created, Peer ID: %s\n", myID.String())
	fmt.Println("    [AutoRelay]       Enabled (5 relays, 5 candidates, 60s boot delay)")
	for _, addr := range h.Addrs() {
		fmt.Printf("    Listening: %s/p2p/%s\n", addr, myID)
	}
	fmt.Println()

	// dht
	bsres := &BootNode {
		Host:      h,
		// DHT:       kadDHT,
		// PSO:       pso,
		Bwc:       bwc,
		PeerID:    myID,
		AddrMgr:   NewAddrManager(),
		PubkeyHex: pubHex,
		BootTime:  time.Since(start),
		RelayPool: rp,
		// Discovery: routingDiscovery,
	}

	if !currConfig.IsMobile {
		bsres.bootDHT(ctx)
	}

	fmt.Println("=== Phase 4: Go Online ===")
	fmt.Printf("[*] Node is now online. Press Ctrl+C to exit.\n")

	myEventSuber(h, new(event.EvtLocalReachabilityChanged),
		new(event.EvtPeerConnectednessChanged),
		new(event.EvtLocalAddressesUpdated),
		new(event.EvtPeerProtocolsUpdated),
		new(event.EvtPeerIdentificationCompleted))
	pso, err := pubsub.NewGossipSub(context.Background(), h,
		pubsub.WithPeerExchange(true),
		pubsub.WithFloodPublish(true), // can publish to peer, not wait to mesh
		pubsub.WithDirectPeers(staticRelays),
		// pubsub.WithDiscovery(bsres.Discovery),
		// pubsub.WithDirectPeers(dht.GetDefaultBootstrapPeerAddrInfos()),
		// half default
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
	if err != nil { log.Println(err) }

	log.Println("bootstrap ret...")
	bsres.PSO = pso
	return bsres, nil
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
		dht.Concurrency(3),                   // ← 并发从 10 降到 3
		dht.RoutingTableRefreshPeriod(5 * time.Minute),  // ← 再加这行, 默认10min
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
	<- errch

	log.Println("[*] Waiting DHT routing table online...", time.Since(btime))
	routingDiscovery := routing.NewRoutingDiscovery(kadDHT)
	testCID := currConfig.HubName // "libp2p-bootstrap-test"
	rettl := discovery2.TTL(10*time.Minute) // 3h
	discovery.Advertise(ctx, routingDiscovery, testCID, rettl) // broadcast self

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
	rd := bootres.Discovery
	// dht := bootres.DHT
	tag := currConfig.HubName
	sec100 := 120*time.Second
	known := make(map[string]peer.AddrInfo)

	for i := 0 ;; i++{
		time.Sleep(3*time.Second)
		log.Println("start DHT finding...", i)
		result := findAndConnect(tag, rd, 0)
		validcnt := 0
		for _, p := range result {
			log.Println(p.ID.ShortString(), p.Addrs)
			if len(p.Addrs) == 0 {
				if _, ok := known[p.ID.String()]; !ok {
					known[p.ID.String()] = p
				}
			}else{
				known[p.ID.String()] = p
				validcnt += 1
			}
			if len(known[p.ID.String()].Addrs) == 0{
				ps := bootres.Host.Peerstore()
				p.Addrs = ps.Addrs(p.ID)
			}
		}
		log.Println("found peers count:", len(known), "valid", validcnt, "round", i)
		if i < 3 && len(result) == 0 {
			time.Sleep(time.Duration(2+i)*time.Second)
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
        bootres.DHT.FindPeer(ctx2, bootres.PeerID)
		// bootres.DHT.Provide(context.Background(), currConfig.HubName, true)

        // 或
        // bootres.DHT.GetClosestPeers(ctx, bootres.PeerID.String())
		cancel2()

		bootres.DHT.GetClosestPeers(context.Background(),bootres.PeerID.String())
		if dur > sec100 {
			continue
		}
	    time.Sleep(sec100-dur)
	}
}

// 搜索所有的tags/topics，流量太大
func myDiscoveryV2ddd() {
	type tagState struct {
		nextAt time.Time
		busy   bool
	}

	rd := bootres.Discovery
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

	if limit <= 0 { limit = 5 }
	peerChan, err := rd.FindPeers(findCtx, tag,
				discovery2.Limit(limit),          // ← 只取 10 个结果
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
		if true { continue }

		if bootres.Host.Network().Connectedness(p.ID) != network.Connected {
            // 每连一个前再检查一次，防止批量连
            // if len(bootres.Host.Network().Conns()) > 12 { break }

			// 每两个连接之间间隔 2s
			time.Sleep(3 * time.Second)
			dialCtx, dialCancel := context.WithTimeout(context.Background(), 10*time.Second)
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

func tryConnect(p peer.AddrInfo) error {
	if bootres.Host.Network().Connectedness(p.ID) == network.Connected {
		return nil
	}

	// 每连一个前再检查一次，防止批量连
	// if len(bootres.Host.Network().Conns()) > 12 { break }

	// 每两个连接之间间隔 2s
	time.Sleep(3 * time.Second)
	dialCtx, dialCancel := context.WithTimeout(context.Background(), 10*time.Second)
	t0 := time.Now()
	err := bootres.Host.Connect(dialCtx, p)
	if err != nil {
		log.Printf("[discovery] connect %s: %v", p.ID.ShortString(), err)
	} else {
		elapsed := time.Since(t0)
		log.Printf("[discovery] connected to %s in %v", p.ID.ShortString(), elapsed)
		updatePeerLatency(p.ID, elapsed)
	}
	dialCancel()
	return err
}

func myDiscoveryV1 (bootCtx context.Context, routingDiscovery *routing.RoutingDiscovery, testCID string, myID peer.ID) (discoveredSet map[peer.ID]struct{}) {
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

// new(event.EvtLocalReachabilityChanged)...
func myEventSuber(h host.Host, evts ...any) {
	// sub, err := h.EventBus().Subscribe(new(event.EvtLocalReachabilityChanged))
	sub, err := h.EventBus().Subscribe(evts)
	if err != nil { panic(err) }
	go func() {
		for evt := range sub.Out() {
			evname := reflect.TypeOf(evt).String()
			evname = strings.Split(evname, ".")[1][3:]
			if false {
				log.Printf("<< %v %+v\n", evname, evt)
			}
			rawChan <- evt

			switch e := evt.(type) {
			case event.EvtLocalReachabilityChanged:
				bootres.NATStatus = e.Reachability
			case event.EvtPeerConnectednessChanged:
				handlePeerConnectednessChanged(e)
				if e.Connectedness == network.Connected {
					if addr := IsGoodPeer(e.Peer); addr != "" {
						log.Printf("[goodpeer] %s/p2p/%s", addr, e.Peer.String())
					}
				}
				if e.Connectedness == network.Limited {
					protocols, err := bootres.Host.Peerstore().GetProtocols(e.Peer)
					support := "N"
					if err == nil {
						for _, p := range protocols {
							if protocol.ID(p) == LimitedPxProtocol {
								support = "Y"
								break
							}
						}
					}
					log.Printf("[limited-px] %s push/1.0=%s conn=%s protocols=%v", e.Peer.ShortString(), support, h.Network().Connectedness(e.Peer), protocols)
				}
			case event.EvtLocalAddressesUpdated:
				if bootres != nil {
					var curAddrs, relayCircuits []multiaddr.Multiaddr
					for _, ua := range e.Current {
						curAddrs = append(curAddrs, ua.Address)
					}
					bootres.AddrMgr.SetLocal(mergeAddrs(nil, curAddrs))

					for _, ua := range e.Removed {
						if isRelayAddr(ua.Address) {
							relayCircuits = append(relayCircuits, ua.Address)
						}
					}
					bootres.AddrMgr.SetRelayCircuit(mergeAddrs(nil, relayCircuits))
				}
				log.Println(evname, "collected addrs", len(e.Current))
				if e.SignedPeerRecord != nil {
					pr := &peer.PeerRecord{}
					if err := e.SignedPeerRecord.TypedRecord(pr); err == nil {
						// log.Printf("[addrs] signed record: seq=%d addrs=%v", pr.Seq, pr.Addrs)
					}
				}
				go func(){
					// bootres.DHT.RefreshRoutingTable()
					// discovery.Advertise(context.Background(), bootres.Discovery, currConfig.HubName)
				}()
			case event.EvtPeerProtocolsUpdated:
				if e.Peer == bootres.Host.ID() {
					break
				}
				for _, p := range e.Added {
					if p == LimitedPxProtocol {
						log.Printf("[limited-px] %s push/1.0=N→Y (updated conn=%s)",
							e.Peer.ShortString(), h.Network().Connectedness(e.Peer))
					}
				}
				for _, p := range e.Removed {
					if p == LimitedPxProtocol {
						log.Printf("[limited-px] %s push/1.0=Y→N (updated conn=%s)",
							e.Peer.ShortString(), h.Network().Connectedness(e.Peer))
					}
				}
			case event.EvtPeerIdentificationCompleted:
				if e.Peer == bootres.Host.ID() {
					break
				}
				support := "N"
				for _, p := range e.Protocols {
					if p == LimitedPxProtocol {
						support = "Y"
						break
					}
				}
				log.Printf("[limited-px] %s push/1.0=%s conn=%s (ident)",
					e.Peer.ShortString(), support, h.Network().Connectedness(e.Peer))
				// pushx := support=="Y" && h.Network().Connectedness(e.Peer) == network.Limited
				pushx := support=="Y"
				if pushx {
					go func() {
						btime := time.Now()
						ctx, cancel := AllowLimitedConn(15, "limitpx")
						defer cancel()
						err := PushToPeer(ctx, e.Peer)
						log.Printf("[limited-px] %s push/1.0=%s pushed %v %v",
							e.Peer.ShortString(), support, time.Since(btime), err)
					}()
				}
			}
		}
	}()
}

func myDumpBoot(h host.Host, dht *dht.IpfsDHT) {
	dhtsz := 0
	if dht != nil {
		dhtsz = dht.RoutingTable().Size()
	}

	conns := GetCurrConns(h)

	log.Printf("conns %v, dht %v", len(conns), dhtsz)
	log.Println()
}

func GetCurrConns (h host.Host) (discoveredSet map[peer.ID]struct{}) {
	discoveredSet = make(map[peer.ID]struct{})
	for _, conn := range h.Network().Conns() {
		discoveredSet[conn.RemotePeer()] = struct{}{}
	}
	return discoveredSet
}

func savePeerstore(path string) error {
	if bootres == nil || bootres.Host == nil {
		return fmt.Errorf("peerstore not ready")
	}
	ps := bootres.Host.Peerstore()
	m := make(map[string][]string)
	for _, p := range ps.Peers() {
		addrs := ps.Addrs(p)
		if len(addrs) == 0 {
			continue
		}
		var as []string
		for _, a := range addrs {
			ip := extractIPFromAddr(a)
			if ip != nil && (ip.IsLoopback() || ip.IsPrivate()) {
				continue
			}
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
		m[p.String()] = as
	}
	if len(m) < minPeerCount {
		return nil
	}
	raw, err := json.Marshal(m)
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	cs := fmt.Sprintf("%x", md5.Sum(raw))
	if cs == savedPeerstoreSum {
		return nil
	}
	savedPeerstoreSum = cs

	if err := os.WriteFile(path, raw, 0644); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	log.Printf("[peerstore] saved %d peers", len(m))
	return nil
}

func loadPeerstore(h host.Host, path string) error {
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read: %w", err)
	}
	var m map[string][]string
	if err := json.Unmarshal(raw, &m); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}
	for idStr, addrs := range m {
		pid, err := peer.Decode(idStr)
		if err != nil {
			continue
		}
		var mas []multiaddr.Multiaddr
		for _, a := range addrs {
			ma, err := multiaddr.NewMultiaddr(a)
			if err != nil {
				continue
			}
			mas = append(mas, ma)
		}
		if len(mas) > 0 {
			h.Peerstore().AddAddrs(pid, mas, peerstore.PermanentAddrTTL)
		}
	}
	log.Printf("[peerstore] loaded %d peers from %s", len(m), path)
	return nil
}

func cleanPeerstore() {
	if bootres == nil || bootres.Host == nil {
		return
	}
	h := bootres.Host
	ps := h.Peerstore()
	if len(ps.Peers()) < minPeerCount {
		return
	}
	for _, pid := range ps.Peers() {
		// skip self — host ID is in Peers() (via AddPrivKey) but Connectedness returns NotConnected
		if pid == bootres.Host.ID() || isBootstrapPeer(pid) {
			continue
		}
		if h.Network().Connectedness(pid) != network.Connected {
			if aps, ok := ps.(peerstore.AddrBook); ok {
				aps.ClearAddrs(pid)
			}
			ps.RemovePeer(pid)
		}
	}
}

func watchStaticRelays(ctx context.Context, h host.Host, relays []peer.AddrInfo) {
	go func() {
		pingTicker := time.NewTicker(30 * time.Second)
		defer pingTicker.Stop()
		for {
			select {
			case <-pingTicker.C:
				for _, r := range relays {
					if h.Network().Connectedness(r.ID) != network.Connected {
						continue
					}
					pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
					res := <-ping.Ping(pctx, h, r.ID)
					cancel()
					if bootres != nil && bootres.RelayPool != nil {
						if res.Error != nil {
							bootres.RelayPool.RecordResult(r.ID, res.Error)
						} else {
							bootres.RelayPool.RecordResult(r.ID, nil)
						}
					}
				}
				if bootres != nil && bootres.RelayPool != nil {
					if maddr := bootres.RelayPool.Select(); maddr != nil {
						log.Printf("[relaypool] select: %s", maddr.String())
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for _, r := range relays {
				if h.Network().Connectedness(r.ID) != network.Connected {
					log.Printf("[relay] re-reserving %s", r.ID.ShortString())
					cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
					res, err := client.Reserve(cctx, h, r)
					cancel()
					if err != nil {
						log.Printf("[relay] reserve %s: %v", r.ID.ShortString(), err)
						if bootres != nil && bootres.RelayPool != nil {
							bootres.RelayPool.RecordResult(r.ID, err)
						}
					} else {
						log.Printf("[relay] reserved from %s, expires %s, addrs: %v",
							r.ID.ShortString(), res.Expiration.Format(time.TimeOnly), res.Addrs)
						if bootres != nil {
							bootres.AddrMgr.SetRelayVouch(r.ID, res.Addrs, res.Expiration)
							if bootres.RelayPool != nil {
								bootres.RelayPool.SetReservationTTL(r.ID, res.Expiration)
								bootres.RelayPool.RecordResult(r.ID, nil)
							}
						}
					}
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func relayPoolManager(ctx context.Context, h host.Host, rp *RelayPool, k int) {
	initial := rp.SelectN(k)
	rp.AddManaged(pidsFromAddrs(initial)...)
	for _, ai := range initial {
		doRelayReserve(ctx, h, rp, ai)
	}

	ticker := time.NewTicker(60 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if rp == nil {
				continue
			}

			var toRemove []peer.ID
			for _, pid := range rp.ListManaged() {
				if h.Network().Connectedness(pid) != network.Connected {
					toRemove = append(toRemove, pid)
					continue
				}
				if rp.IsCircuitOpen(pid) {
					h.Network().ClosePeer(pid)
					toRemove = append(toRemove, pid)
					continue
				}
				cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
				res, err := client.Reserve(cctx, h, peer.AddrInfo{ID: pid})
				cancel()
				if err != nil {
					log.Printf("[relaypool] reserve %s: %v", pid.ShortString(), err)
					rp.RecordResult(pid, err)
				} else {
					if bootres != nil {
						bootres.AddrMgr.SetRelayVouch(pid, res.Addrs, res.Expiration)
					}
					rp.SetReservationTTL(pid, res.Expiration)
					rp.RecordResult(pid, nil)
				}
			}
			rp.RemoveManaged(toRemove...)

			if len(rp.ListManaged()) < k {
				need := k - len(rp.ListManaged())
				for _, ai := range rp.SelectN(need) {
					if h.Network().Connectedness(ai.ID) == network.Connected {
						continue
					}
					rp.AddManaged(ai.ID)
					doRelayReserve(ctx, h, rp, ai)
				}
			}

		case <-ctx.Done():
			return
		}
	}
}

func doRelayReserve(ctx context.Context, h host.Host, rp *RelayPool, ai peer.AddrInfo) {
	log.Printf("[relaypool] connecting %s", ai.ID.ShortString())
	if err := h.Connect(ctx, ai); err != nil {
		log.Printf("[relaypool] connect %s: %v", ai.ID.ShortString(), err)
		rp.RecordResult(ai.ID, err)
		return
	}
	rp.RecordResult(ai.ID, nil)

	cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	res, err := client.Reserve(cctx, h, ai)
	if err != nil {
		log.Printf("[relaypool] reserve %s: %v", ai.ID.ShortString(), err)
		rp.RecordResult(ai.ID, err)
	} else {
		log.Printf("[relaypool] reserved %s, expires %s",
			ai.ID.ShortString(), res.Expiration.Format(time.TimeOnly))
		if bootres != nil {
			bootres.AddrMgr.SetRelayVouch(ai.ID, res.Addrs, res.Expiration)
		}
		rp.SetReservationTTL(ai.ID, res.Expiration)
		rp.RecordResult(ai.ID, nil)
	}
}

func pidsFromAddrs(infos []peer.AddrInfo) []peer.ID {
	pids := make([]peer.ID, len(infos))
	for i, ai := range infos {
		pids[i] = ai.ID
	}
	return pids
}

func mergeAddrs(existing, incoming []multiaddr.Multiaddr) []multiaddr.Multiaddr {
	seen := make(map[string]struct{}, len(existing)+len(incoming))
	out := make([]multiaddr.Multiaddr, 0, len(existing)+len(incoming))
	for _, a := range existing {
		key := a.String()
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, a)
	}
	for _, a := range incoming {
		key := a.String()
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, a)
	}
	return out
}
