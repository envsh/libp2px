package p2put

// fix cluster node connection
// non => any connect, relayed or direct
// limit => direct
// inbound => outbound
// quick build gossip mesh
// too less conns < 3 => more 5+
// query peer info from connected cluster peers

import (
	"time"
	"log"
	"math/rand"
	"strings"
	"context"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/network"
)

type connfixer struct {
	known map[string]peer.AddrInfo
	keys []string
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
	time.Sleep(3*time.Second)
	// random select 3 and try connect
	for j := 0; ; j++ {
		if time.Since(btime) > sec100 {
			break
		}
		time.Sleep(3*time.Second)
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
			// hotfix lost but still not cleared nodes
			if strings.HasSuffix(p.ID.String(), "6Y5TDQ") ||
				strings.HasSuffix(p.ID.String(), "2u1qRU") {
				continue
			}
			if len(p.Addrs) == 0 {
				ctx1 := context.Background()
				ctx2, cancel2 := context.WithTimeout(ctx1, 13*time.Second)
				relayma := "/ip4/65.109.60.254/tcp/4001/p2p/12D3KooWL96RJHMjvPzkDzEwSBNei4Ftak7n8gF5Tfn8Dc1cSYQn"
				// relayma := "/ip4/54.38.47.166/tcp/4001/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb"
				err = ConnectViaRelay(ctx2, relayma, p.ID.String())
				log.Println(err)
				cancel2()
			} else {
				err = tryConnect(p)
			}
			p2 = p
			if err == nil {
				// if is relay, try openstream direct
				c := connByPeerID(p.ID)
				if c != nil {
					if isRelayAddr(c.RemoteMultiaddr()) {
						//
						log.Println("conn relayed, try direct", p.ID.ShortString())
						// 尝试直连目标
						// ctx1 := network.WithAllowLimitedConn(, "reason")
						ctx1 := context.Background()
						directCtx, cancel := context.WithTimeout(ctx1, 5*time.Second)
						stream, err := bootres.Host.NewStream(directCtx, p.ID, "/myapp/dirfoo/1.0")
						// stream, err := c.NewStream(directCtx)
						log.Println("direct conn:", p.ID.ShortString(), "err:", err)
						if err == nil {
							stream.Close()
						}
						cancel()
					}
				}
				break
			} else {
				ctx1 := context.Background()
				ctx2, cancel2 := context.WithTimeout(ctx1, 13*time.Second)
				relayma := "/ip4/65.109.60.254/tcp/4001/p2p/12D3KooWL96RJHMjvPzkDzEwSBNei4Ftak7n8gF5Tfn8Dc1cSYQn"
				// relayma := "/ip4/54.38.47.166/tcp/4001/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb"
				err = ConnectViaRelay(ctx2, relayma, p.ID.String())
				log.Println(err)
				cancel2()
			}
			if currConfig.IsMobile { break }

			// udp heavy
			time.Sleep(time.Second)
			t1 := time.Now()
			log.Println("(UDP) dht.FindPeer'ing ...", p2.ID.ShortString())
			// findAndConnect(p2.ID.String(), rd, 1)
			addrinfo, err := dht.FindPeer(context.Background(), p2.ID)
			_ = addrinfo
			log.Println("(UDP) dht.FindPeer'ed ...", time.Since(t1), p2.ID.ShortString(), addrinfo, err)
			if err != nil {
			}else{
				tryConnect(addrinfo)
			}
			break
		}
		time.Sleep(13*time.Second)
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

func (fxr *connfixer) connect_direct() {

}

func (fxr *connfixer) connect_relay() {

}

func (fxr *connfixer) connect_more() {
	h := bootres.Host
	conns := h.Network().Conns()
	if len(conns) < 3 {
	}
}
