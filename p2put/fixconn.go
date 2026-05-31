package p2put

// fix cluster node connection
// non => any connect, relayed or direct
// limit => direct
// inbound => outbound
// quick build gossip mesh
// too less conns < 3 => more 5+
// query peer info from connected cluster peers

import (
	"fmt"
	"log"
	"math/rand"
	"time"
	// "strings"
	"context"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

type connfixer struct {
	known  map[string]peer.AddrInfo
	keys   []string
	maxdur time.Duration
}

func newconnfixer(known map[string]peer.AddrInfo, maxdur time.Duration) *connfixer {
	fxr := &connfixer{}
	fxr.known = known
	fxr.maxdur = maxdur

	for k, _ := range known {
		fxr.keys = append(fxr.keys, k)
	}

	return fxr
}
func connByPeerID(pid peer.ID) network.Conn {
	for _, c := range bootres.Host.Network().Conns() {
		if c.RemotePeer() == pid {
			return c
		}
	}
	return nil
}

func (fxr *connfixer) dofix() {
	btime := time.Now()
	sec100 := fxr.maxdur
	known := fxr.known
	dht := bootres.DHT

	var err error
	var p2 peer.AddrInfo
	time.Sleep(3 * time.Second)
	// random select 3 and try connect
	for j := 0; ; j++ {
		if time.Since(btime) > sec100 {
			break
		}
		time.Sleep(3 * time.Second)
		keys := []string{}
		for k, _ := range known {
			keys = append(keys, k)
		}
		// for _, p := range known {
		for n := 0; n < len(keys); n++ {
			key := keys[int(rand.Uint32()/2)%len(keys)]
			p := known[key]

			log.Println("prepconn rc", p.ID.ShortString(), keys)
			if IsPeerInAnyTopic(p.ID) || IsPeerConnected(p.ID, true) {
				continue
			}

			if len(p.Addrs) == 0 {
				err = tryConnect(p)
			} else {
				err = tryConnect(p)
			}
			p2 = p
			if err == nil {
				// if is relay, try openstream direct
				c := connByPeerID(p.ID)
				if c != nil {
					if isRelayAddr(c.RemoteMultiaddr()) {
						fxr.connect_direct(p)
					}
				}
				break
			} else {
				// err = fxr.connect_relay(p.ID)
				fxr.connect_direct(p)
			}
			if currConfig.IsMobile {
				break
			}

			// udp heavy
			time.Sleep(time.Second)
			t1 := time.Now()
			log.Println("(UDP) dht.FindPeer'ing ...", p2.ID.ShortString())
			// findAndConnect(p2.ID.String(), rd, 1)
			addrinfo, err := dht.FindPeer(context.Background(), p2.ID)
			_ = addrinfo
			log.Println("(UDP) dht.FindPeer'ed ...", time.Since(t1), p2.ID.ShortString(), addrinfo, err)
			if err != nil {
			} else {
				tryConnect(addrinfo)
			}
			break
		}

		time.Sleep(13 * time.Second)
		fxr.connect_more()
	}
	if err != nil {
		// time.Sleep(5*time.Second)
		// findAndConnect(p2.ID.String(), rd, 1)
		// addrinfo, err := dht.FindPeer(context.Background(), p2.ID)
		// _ = addrinfo
		// if err != nil {
		// }
	}

}

func (fxr *connfixer) auto_connect() {

}

func (fxr *connfixer) connect_outbound() {

}

func (fxr *connfixer) connect_direct(p peer.AddrInfo) error {
	//
	log.Println("conn relayed, try direct", p.ID.ShortString())
	// 尝试直连目标
	// ctx1 := network.WithAllowLimitedConn(, "reason")
	ctx1 := context.Background()
	ctx2, cancel := context.WithTimeout(ctx1, 5*time.Second)
	// 所有节点都有的协议，防止出现
	// concurrent active dial through the same relay failed with a protocol error
	// /ipfs/ping/1.0.0
	// /ipfs/id/1.0.0
	stream, err := bootres.Host.NewStream(ctx2, p.ID, "/ipfs/ping/1.0.0")
	// stream, err := c.NewStream(directCtx)
	log.Println("direct conn:", p.ID.ShortString(), "err:", err)
	if err == nil {
		stream.Close()
	}
	cancel()

	return err
}

// 需要预先知道对方在哪个relay上。没啥用。
func (fxr *connfixer) connect_relay(peerid peer.ID) error {
	if bootres == nil || bootres.Host == nil {
		return fmt.Errorf("libp2p not ready")
	}

	return fmt.Errorf("connect_relay: no relay to %s", peerid.ShortString())
}

func (fxr *connfixer) connect_more() {
	h := bootres.Host
	conns := h.Network().Conns()
	if len(conns) >= 3 {
		return
	}
	log.Println("conns too less", len(conns))
}
