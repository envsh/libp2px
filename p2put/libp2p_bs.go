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
	// "math/rand"
	"log"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	// "slices"
	// "maps"
	"time"

	"github.com/envsh/toxera/fedkey"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/metrics"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/libp2p/go-libp2p/p2p/host/autorelay"
	"github.com/libp2p/go-libp2p/p2p/net/connmgr"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/client"
	"github.com/libp2p/go-libp2p/p2p/protocol/ping"
	"github.com/libp2p/go-libp2p/p2p/transport/tcp"
	"github.com/libp2p/go-libp2p/p2p/transport/websocket"
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

	IpfsPingProtocl   = "/ipfs/ping/1.0.0"
	IpfsIdProtocol    = "/ipfs/id/1.0.0"
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
// 缓存文件格式: map[string][]string (原始地址 → 解析后的地址列表)
var dnsaddrsResultFile = filepath.Join(os.TempDir(), "libp2p_bootstrap_dnsaddrs.json")
var dnsaddrsCacheDur = 5 * 3600 * time.Second

type latencyReq struct {
	addr string
	peer peer.ID
}

var latencyCh = make(chan latencyReq, 32)

func init() {
	log.SetFlags((log.Flags() | log.Lshortfile | log.Ltime) &^ log.Ldate)
}

type BootNode struct {
	Host            host.Host
	DHT             any
	PSO             any
	Bwc             metrics.Reporter
	PeerID          peer.ID
	AddrMgr         *AddrManager
	PubkeyHex       string
	BootTime        time.Duration
	NATStatus       network.Reachability
	Discovery       any
	PeerDB          *PeerDB
	RelayPool       *RelayPool
	OfflineDetector *OfflineDetector
}


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
		if err != nil {
			panic(err)
		}
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

	bootres.Host = res.Host
	bootres.DHT = res.DHT
	bootres.PSO = res.PSO
	bootres.PeerID = res.PeerID
	bootres.PubkeyHex = res.PubkeyHex
	bootres.NATStatus = res.NATStatus
	bootres.Discovery = res.Discovery
	bootres.BootTime = res.BootTime
	bootres.OfflineDetector = NewOfflineDetector(res.Host, res.RelayPool)
	go bootres.OfflineDetector.Run(context.Background())

	loadPeerstore(bootres.Host, peerstorePath)

	if currConfig.IsMobile {
		go AdvertiseHTTP(context.Background())
		go discoveryV4(context.Background())
		go DiscoveryV6(context.Background())
	} else {
		go bootres.myDiscoveryV3()
		//go myDiscoveryV2()
	}

	for _, topic := range currConfig.Topics {
		if len(topic) <= 0 {
			continue
		}
		getOrSubscribeTopic(topic)
	}
	if currConfig.HubName != "" {
		getOrSubscribeTopic(currConfig.HubName)
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

	jamidhtproxy = NewJamiDHTProxy("http://dhtproxy.jami.net:80")

	turnPool.AddServer(TurnServerConfig{
		Addr:     "turn.jami.net:3478",
		Protocol: "tcp",
		Username: "ring",
		Password: "ring",
		Realm:    "ring",
	})
	if currConfig.enableTurnRelay {
		turnPool.Start(context.Background())
	}

	if currConfig.enableIrohRelay {
		p := GetIrohRelayPool()
		p.AddRelay("wss://usw1-1.relay.n0.iroh-canary.iroh.link/relay")
		if err := p.Start(context.Background(), currConfig.KeyFile); err != nil {
			log.Printf("[irohrelay] start: %v", err)
		}
	}

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

		libp2p.BandwidthReporter(bootres.Bwc),

		libp2p.AddrsFactory(myAddrsFactory),
	)
	if err != nil {
		return nil, fmt.Errorf("create libp2p host: %w", err)
	}

	replayProtocols(h)

	for _, r := range staticRelays {
		bootres.RelayPool.Add(r.Addrs[0].String() + "/p2p/" + r.ID.String())
		bootres.RelayPool.Protect(r.ID)
	}

	for _, r := range staticRelays {
		h.ConnManager().Protect(r.ID, "relay")
	}
	if false {
		go watchStaticRelays(context.Background(), h, staticRelays)
	} else {
		go relayPoolManager(context.Background(), h, bootres.RelayPool, 3)
	}

	myID := h.ID()
	fmt.Printf("[+] Host created, Peer ID: %s\n", myID.String())
	fmt.Println("    [AutoRelay]       Enabled (5 relays, 5 candidates, 60s boot delay)")
	for _, addr := range h.Addrs() {
		fmt.Printf("    Listening: %s/p2p/%s\n", addr, myID)
	}
	fmt.Println()

	// dht
	bsres := &BootNode{
		Host: h,
		// DHT:       kadDHT,
		// PSO:       pso,
		PeerID:    myID,
		PubkeyHex: pubHex,
		BootTime:  time.Since(start),
		RelayPool: bootres.RelayPool,
		// Discovery: routingDiscovery,
	}

	if !currConfig.IsMobile {
		bsres.bootDHT(ctx)
	}

	fmt.Println("=== Phase 4: Go Online ===")
	fmt.Printf("[*] Node is now online. Press Ctrl+C to exit.\n")

	startLatencyWorker()
	myEventSuber(h, new(event.EvtLocalReachabilityChanged),
		new(event.EvtPeerConnectednessChanged),
		new(event.EvtLocalAddressesUpdated),
		new(event.EvtPeerProtocolsUpdated),
		new(event.EvtPeerIdentificationCompleted))
	pso, err := BuildGossipSub(context.Background(), h, staticRelays)
	if err != nil {
		log.Println(err)
	}

	log.Println("bootstrap ret...")
	bsres.PSO = pso

	return bsres, nil
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

// new(event.EvtLocalReachabilityChanged)...
func startLatencyWorker() {
	go func() {
		cnter := 0
		for req := range latencyCh {
			dur := time.Duration(0)
			if m, err := multiaddr.NewMultiaddr(req.addr); err == nil {
				dur = detectPeerLatency(m)
			}
			if dur > 200*time.Millisecond || dur == 0 {
				continue
			}
			cnter ++
			log.Printf("[goodpeer<200] latency=%v cnt=%v %s/p2p/%s", dur, cnter, req.addr, req.peer.String())
		}
	}()
}

func myEventSuber(h host.Host, evts ...any) {
	// sub, err := h.EventBus().Subscribe(new(event.EvtLocalReachabilityChanged))
	sub, err := h.EventBus().Subscribe(evts)
	if err != nil {
		panic(err)
	}
	go func() {
		for evt := range sub.Out() {
			evname := reflect.TypeOf(evt).String()
			evname = strings.Split(evname, ".")[1][3:]
			if false {
				log.Printf("<< %v %+v\n", evname, evt)
			}
			switch v := evt.(type) {
			case event.EvtLocalAddressesUpdated:
				if v.SignedPeerRecord != nil {
					v.SignedPeerRecord.RawPayload = nil
					evt = v
				}
			case event.EvtPeerIdentificationCompleted:
				if v.SignedPeerRecord != nil {
					v.SignedPeerRecord.RawPayload = nil
					evt = v
				}
			}
			switch e := evt.(type) {
			case event.EvtLocalReachabilityChanged:
				rawChan <- evt
			default:
				_ = e
			}

			switch e := evt.(type) {
			case event.EvtLocalReachabilityChanged:
				bootres.NATStatus = e.Reachability
			case event.EvtPeerConnectednessChanged:
				handlePeerConnectednessChanged(e)
				if e.Connectedness == network.Connected {
					if addr := IsGoodPeer(e.Peer); addr != "" {
						select {
						case latencyCh <- latencyReq{addr: addr, peer: e.Peer}:
						default:
							log.Printf("[goodpeer] %s/p2p/%s latency=nodt", addr, e.Peer.String())
						}
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
				log.Println(evname, "collected addrs", len(e.Current))
				if e.SignedPeerRecord != nil {
					pr := &peer.PeerRecord{}
					if err := e.SignedPeerRecord.TypedRecord(pr); err == nil {
						// log.Printf("[addrs] signed record: seq=%d addrs=%v", pr.Seq, pr.Addrs)
					}
				}
				go func() {
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
				pushx := support == "Y"
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

func GetCurrConns(h host.Host) (discoveredSet map[peer.ID]struct{}) {
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
					if bootres.RelayPool != nil {
						if res.Error != nil {
							bootres.RelayPool.RecordResult(r.ID, res.Error)
						} else {
							bootres.RelayPool.RecordResult(r.ID, nil)
						}
					}
				}
				if bootres.RelayPool != nil {
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
						if bootres.RelayPool != nil {
							bootres.RelayPool.RecordResult(r.ID, err)
						}
					} else {
						log.Printf("[relay] reserved from %s, expires %s, addrs: %v",
							r.ID.ShortString(), res.Expiration.Format(time.TimeOnly), res.Addrs)
						bootres.AddrMgr.SetRelayVouch(r.ID, res.Addrs, res.Expiration)
						if bootres.RelayPool != nil {
							bootres.RelayPool.SetReservationTTL(r.ID, res.Expiration)
							bootres.RelayPool.RecordResult(r.ID, nil)
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
					bootres.AddrMgr.SetRelayVouch(pid, res.Addrs, res.Expiration)
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
		bootres.AddrMgr.SetRelayVouch(ai.ID, res.Addrs, res.Expiration)
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
