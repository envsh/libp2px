package p2put

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/multiformats/go-multiaddr"
	// "github.com/libp2p/go-libp2p/core/event"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

func isPrivateIP(ip net.IP) bool {
	if ip == nil {
		return true
	}

	if ip.IsUnspecified() {
		return true
	}

	if ip.IsLoopback() {
		return true
	}

	if ip.IsLinkLocalUnicast() {
		return true
	}

	if ip4 := ip.To4(); ip4 != nil {
		switch {
		case ip4[0] == 10:
			return true
		case ip4[0] == 172 && ip4[1] >= 16 && ip4[1] <= 31:
			return true
		case ip4[0] == 192 && ip4[1] == 168:
			return true
		}
		return false
	}

	if ip6 := ip.To16(); ip6 != nil {
		if ip6[0] == 0xfe && ip6[1] >= 0x80 && ip6[1] <= 0xbf {
			return true
		}
		if ip6[0] == 0xfc || ip6[0] == 0xfd {
			return true
		}
	}

	return false
}

func isRelayAddr(addr multiaddr.Multiaddr) bool {
	for _, proto := range addr.Protocols() {
		if proto.Name == "p2p-circuit" {
			return true
		}
	}
	return false
}

func extractIPFromAddr(addr multiaddr.Multiaddr) net.IP {
	if addr == nil {
		return nil
	}

	for _, proto := range addr.Protocols() {
		if proto.Name == "ip4" || proto.Name == "ip6" {
			ipStr, err := addr.ValueForProtocol(proto.Code)
			if err == nil {
				return net.ParseIP(ipStr)
			}
		}
	}
	return nil
}

func extractRelayPeerID(addr multiaddr.Multiaddr) (peer.ID, error) {
	relayPart, _ := multiaddr.SplitFunc(addr, func(c multiaddr.Component) bool {
		return c.Protocol().Code == multiaddr.P_CIRCUIT
	})
	if relayPart == nil {
		return "", fmt.Errorf("not a relay address")
	}
	pidStr, err := relayPart.ValueForProtocol(multiaddr.P_P2P)
	if err != nil {
		return "", err
	}
	return peer.Decode(pidStr)
}

func collectListeningAddrs(h host.Host) []Libp2pAddrInfo {
	var addrs []Libp2pAddrInfo

	for _, addr := range h.Addrs() {
		isRelay := isRelayAddr(addr)
		ip := extractIPFromAddr(addr)
		isPrivateIPVal := false

		if ip != nil {
			isPrivateIPVal = isPrivateIP(ip)
		}

		addrs = append(addrs, Libp2pAddrInfo{
			Addr:        addr,
			IsRelay:     isRelay,
			IsPrivateIP: isPrivateIPVal,
			IP:          ip,
		})
	}

	return addrs
}

func parseStringAddrs(addrs []string) []peer.AddrInfo {
	var relays []peer.AddrInfo
	for _, addrStr := range addrs {
		ma, err := multiaddr.NewMultiaddr(addrStr)
		if err != nil {
			continue
		}
		ai, err := peer.AddrInfoFromP2pAddr(ma)
		if err != nil {
			continue
		}
		relays = append(relays, *ai)
	}
	return relays
}

func supportsRelayHop(ctx context.Context, h host.Host, p peer.ID) bool {
	protocols, err := h.Peerstore().GetProtocols(p)
	if err == nil {
		for _, proto := range protocols {
			if protocol.ID(proto) == RelayHopProtocol {
				return true
			}
		}
	}

	streamCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	s, err := h.NewStream(streamCtx, p, RelayHopProtocol)
	if err != nil {
		return false
	}
	s.Close()
	return true
}

func myAddrsFactory(addrs []multiaddr.Multiaddr) []multiaddr.Multiaddr {
	var out []multiaddr.Multiaddr
	for _, a := range addrs {
		if isRelayAddr(a) {
			out = append(out, a)
		} else {
			ip4 := false
			tcp := false
			ip := extractIPFromAddr(a)
			islo := ip != nil && ip.IsLoopback()
			for _, p := range a.Protocols() {
				if p.Code == multiaddr.P_IP4 {
					ip4 = true
				}
				if p.Code == multiaddr.P_TCP {
					tcp = true
				}
			}
			_, _ = ip4, tcp
			// if ip4 && tcp && !islo {
			if !islo {
				out = append(out, a)
			}
		}
		// log.Println(a)
	}
	if len(addrs) != len(out) {
		// log.Println("addrs filter", len(addrs), "=>", len(out))
	}
	return out
}

func IsGoodPeer(pid any) string {
	p, err := toPeerID(pid)
	if err != nil {
		return ""
	}
	if isBootstrapPeer(p) {
		return ""
	}
	if bootres == nil || bootres.Host == nil {
		return ""
	}
	addrs := bootres.Host.Peerstore().Addrs(p)
	for _, c := range bootres.Host.Network().ConnsToPeer(p) {
		addrs = append(addrs, c.RemoteMultiaddr())
	}
	for _, a := range addrs {
		ai := peer.AddrInfo{ID: p, Addrs: []multiaddr.Multiaddr{a}}
		if IsGoodPeerAddr(ai) {
			return a.String()
		}
	}
	return ""
}

// relayMa: /ip4/1.1.1.1/tcp/4001/p2p/QmRelayPeerID
func ConnectViaRelay(ctx context.Context, relayMa, targetPeerID string) error {
	if bootres == nil || bootres.Host == nil {
		return fmt.Errorf("libp2p not ready")
	}
	h := bootres.Host

	relayInfo, err := peer.AddrInfoFromString(relayMa)
	if err != nil {
		return fmt.Errorf("parse relay addr: %w", err)
	}
	if h.Network().Connectedness(relayInfo.ID) != network.Connected {
		if err := h.Connect(ctx, *relayInfo); err != nil {
			return fmt.Errorf("connect relay: %w", err)
		}
		log.Printf("[relay] connected to relay %s", relayInfo.ID.ShortString())
	}

	circuitAddr, err := multiaddr.NewMultiaddr(
		relayMa + "/p2p-circuit/p2p/" + targetPeerID,
	)
	if err != nil {
		return fmt.Errorf("build circuit addr: %w", err)
	}
	targetInfo, err := peer.AddrInfoFromP2pAddr(circuitAddr)
	if err != nil {
		return fmt.Errorf("parse circuit addr: %w", err)
	}

	if err := h.Connect(ctx, *targetInfo); err != nil {
		return fmt.Errorf("connect via relay: %w", err)
	}
	log.Printf("[relay] connected to %s via %s", targetPeerID, relayInfo.ID.ShortString())
	return nil
}

func IsGoodPeerAddr(ai peer.AddrInfo) bool {
	if isBootstrapPeer(ai.ID) {
		return false
	}
	for _, a := range ai.Addrs {
		if isRelayAddr(a) {
			continue
		}

		s := a.String()
		// filter examples:
		//   /ip6/64:ff9b::587:a25c/tcp/4001/p2p/Qm...
		//   /ip4/94.130.66.166/tcp/4001/tls/sni/94-130-66-166.k51qzi5uqu5djnsmc3jqu444j94k2a3rma3tiu1h1dk5ofq5smshiiq6ezeg01.libp2p.direct/ws/p2p/12D3KooWKD5ZdK13CELLDkqskQGE5c1pkoT5ZjP6GcSoKCjmsKBe
		if strings.Contains(s, "libp2p.direct") {
			continue
		}
		if _, err := a.ValueForProtocol(multiaddr.P_IP6); err == nil {
			continue
		}
		if !strings.Contains(s, "/tcp/4001") && !strings.Contains(s, "/tcp/443") {
			continue
		}
		if ipStr, err := a.ValueForProtocol(multiaddr.P_IP4); err == nil {
			if !isPrivateIP(net.ParseIP(ipStr)) {
				return true
			}
		}
	}
	return false
}

func AllowLimitedConn(timeoutsec int, name string) (context.Context, func()) {
	if timeoutsec <= 0 {
		timeoutsec = 999_999_999
	}
	ctx := context.Background()
	ctx2, cancel := context.WithTimeout(ctx, time.Duration(timeoutsec)*time.Second)
	ctx3 := network.WithAllowLimitedConn(ctx2, name)
	return ctx3, cancel
}

// PeerIsConnected checks if peer pid has any connection.
// If direct=true, only non-Limited (direct) connections count.
func PeerIsConnected(pid peer.ID, direct bool) bool {
	if bootres.Host == nil {
		return false
	}
	for _, c := range bootres.Host.Network().ConnsToPeer(pid) {
		if !direct || !c.Stat().Limited {
			return true
		}
	}
	return false
}
