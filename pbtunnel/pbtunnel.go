package pbtunnel

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc64"
	"io"
	"log"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"math/rand"

	"github.com/envsh/libp2px/p2put"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	manet "github.com/multiformats/go-multiaddr/net"
)

var ShouldReject func(network.Stream) bool

const (
	tunnelProto   = "tunnel/1.0"
	udpTunnelProto = "udptunnel/1.0"
	bufSize       = 3072
	udpBufSize    = 65535
)

var Stats struct {
	BytesSent int64
	BytesRecv int64
	ConnSeq   int64
	Dur       int64
}

var (
	tunnelTarget string
	tunnelPort   = 9229
)

func SetTarget(addr string) { tunnelTarget = addr }
func SetPort(port int)       { tunnelPort = port }

// targetAddr returns the local TCP address for tunnel forwarding.
// When proto carries a port suffix after tunnelProto (e.g.
// "/d2hub/tunnel/1.09339" → port 9339), it overrides tunnelPort.
func targetAddr(proto ...string) string {
	if tunnelTarget != "" {
		return tunnelTarget
	}
	port := tunnelPort
	if len(proto) < 1 {
		// no protocol string, use tunnelPort default
	} else {
		idx := strings.Index(string(proto[0]), tunnelProto)
		if idx < 0 {
			log.Panicln("targetAddr: proto missing tunnelProto:", proto[0])
		}
		suffix := string(proto[0])[idx+len(tunnelProto):]
		if suffix == "" || suffix == "0" {
			// no port (legacy "tunnel/1.0") or zero, use tunnelPort default
		} else {
			n, err := strconv.Atoi(suffix)
			if err != nil {
				log.Panicln("targetAddr: bad port suffix:", suffix, "proto:", proto[0])
			}
			port = n
		}
	}
	return fmt.Sprintf("127.0.0.1:%d", port)
}

func init() {
	p2put.MustRegisterProtocol(tunnelProto, handleTunnel, true)
	p2put.MustRegisterProtocol(udpTunnelProto, handleUDPTunnel, true)
}

func handleTunnel(s network.Stream) {
	seq := atomic.AddInt64(&Stats.ConnSeq, 1)
	start := time.Now()
	peerid := s.Conn().RemotePeer().ShortString()
	log.Printf("[pbtunnel] conn=%d protocol=%s newed: %v\n", seq, s.Protocol(), peerid)

	if ShouldReject != nil && ShouldReject(s) {
		s.Reset()
		return
	}

	addr := targetAddr(string(s.Protocol()))

	conn, err := net.DialTimeout("tcp", addr, 30*time.Second)
	if err != nil {
		log.Printf("[pbtunnel] dial %s: %v", addr, err)
		s.Close()
		return
	}

	var closeOnce sync.Once
	closeStream := func() { closeOnce.Do(func() { s.Close() }) }
	defer closeStream()

	var connCloseOnce sync.Once
	connClose := func() { connCloseOnce.Do(func() { conn.Close() }) }
	defer connClose()

	var wg sync.WaitGroup
	wg.Add(2)

	var localSent, localRecv int64

	go func() {
		defer log.Println("xfer tun <- sock", seq, peerid)
		defer wg.Done()
		defer closeStream()
		buf := make([]byte, bufSize)
		for {
			conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
			n, rerr := conn.Read(buf)
			if n > 0 {
				wn, _ := writen(s, buf, n)
				atomic.AddInt64(&Stats.BytesSent, int64(wn))
				localSent += int64(wn)
				if wn != n {
					return
				}
			}
			if rerr != nil {
				if ne, ok := rerr.(net.Error); ok && ne.Timeout() {
					continue
				}
				return
			}
		}
	}()

	go func() {
		defer log.Println("xfer tun -> sock", seq, peerid)
		defer wg.Done()
		defer connClose()
		buf := make([]byte, bufSize)
		for {
			s.SetReadDeadline(time.Now().Add(5 * time.Minute))
			n, rerr := s.Read(buf)
			if n > 0 {
				wn, _ := writen(conn, buf, n)
				atomic.AddInt64(&Stats.BytesRecv, int64(wn))
				localRecv += int64(wn)
				if wn != n {
					return
				}
			}
			if rerr != nil {
				if ne, ok := rerr.(net.Error); ok && ne.Timeout() {
					continue
				}
				return
			}
		}
	}()

	log.Println("wg.Wait() ...", seq, peerid)
	wg.Wait()
	dur := time.Since(start)
	atomic.AddInt64(&Stats.Dur, dur.Nanoseconds())
	log.Printf("[pbtunnel] conn=%d closed: sent=%d recv=%d dur=%s", seq, localSent, localRecv, dur.Round(time.Millisecond))
}

func handleUDPTunnel(s network.Stream) {
	seq := atomic.AddInt64(&Stats.ConnSeq, 1)
	start := time.Now()
	peerid := s.Conn().RemotePeer().ShortString()
	log.Printf("[pbtunnel] udp conn=%d protocol=%s newed: %v\n", seq, s.Protocol(), peerid)

	if ShouldReject != nil && ShouldReject(s) {
		s.Reset()
		return
	}

	addr := targetAddr()
	udpConn, err := net.Dial("udp", addr)
	if err != nil {
		log.Printf("[pbtunnel] udp dial %s: %v", addr, err)
		s.Close()
		return
	}

	var closeOnce sync.Once
	closeStream := func() { closeOnce.Do(func() { s.Close() }) }
	defer closeStream()

	var connCloseOnce sync.Once
	connClose := func() { connCloseOnce.Do(func() { udpConn.Close() }) }
	defer connClose()

	var wg sync.WaitGroup
	wg.Add(2)
	var localSent, localRecv int64

	go func() {
		defer wg.Done()
		defer closeStream()
		hdr := make([]byte, 2)
		for {
			_, err := io.ReadFull(s, hdr)
			if err != nil {
				return
			}
			length := int(binary.BigEndian.Uint16(hdr))
			buf := make([]byte, length)
			_, err = io.ReadFull(s, buf)
			if err != nil {
				return
			}
			wn, _ := udpConn.Write(buf)
			localSent += int64(wn)
			atomic.AddInt64(&Stats.BytesSent, int64(wn))
		}
	}()

	go func() {
		defer wg.Done()
		defer connClose()
		buf := make([]byte, udpBufSize)
		for {
			n, rerr := udpConn.Read(buf)
			if n > 0 {
				hdr := []byte{0, 0}
				binary.BigEndian.PutUint16(hdr, uint16(n))
				wn, _ := writen(s, append(hdr, buf[:n]...), n+2)
				atomic.AddInt64(&Stats.BytesRecv, int64(wn))
				localRecv += int64(wn)
				if wn != n+2 {
					return
				}
			}
			if rerr != nil {
				return
			}
		}
	}()

	wg.Wait()
	dur := time.Since(start)
	log.Printf("[pbtunnel] udp conn=%d closed: sent=%d recv=%d dur=%s", seq, localSent, localRecv, dur.Round(time.Millisecond))
}

// peerID format: 123xxx[:port]
// if want a net.Conn, just wrap like this: &pbtunnel.P2PConn{stm}
func Dial(peerID string, ctx ...context.Context) (network.Stream, error) {
	var c context.Context
	if len(ctx) > 0 {
		c = ctx[0]
	} else {
		c = context.Background()
	}
	arr := strings.Split(peerID, ":")
	protoWithPort := fmt.Sprintf("%s%v", tunnelProto, tunnelPort)
	if len(arr) == 2 {
		protoWithPort = fmt.Sprintf("%s%v", tunnelProto, arr[1])
		peerID = arr[0]
	} else if len(arr) == 1{
	} else {
		log.Panicln("wtf", arr)
	}
	// log.Println("opening", peerID, protoWithPort)
	return p2put.OpenStream(c, peerID, protoWithPort)
}

type P2PConn struct {
	network.Stream
}

func (c *P2PConn) LocalAddr() net.Addr  { a, _ := manet.ToNetAddr(c.Conn().LocalMultiaddr()); return a }
func (c *P2PConn) RemoteAddr() net.Addr { a, _ := manet.ToNetAddr(c.Conn().RemoteMultiaddr()); return a }

// 假设协议对端的端口是http proxy端口
// NewHttpClient creates an *http.Client that tunnels all requests over a p2p
// stream to peerID via CONNECT handshake. Each request opens a new stream
// (DisableKeepAlives, MaxIdleConnsPerHost=-1). Usage:
//
//	client := pbtunnel.NewHttpClient("12D3KooW...")
//	resp, err := client.Get("https://example.com")
func NewHttpClient(peerID string) *http.Client {
	return &http.Client{
		Transport: &http.Transport{
			DisableKeepAlives:   true,
			MaxIdleConnsPerHost: -1,
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				stream, err := Dial(peerID, ctx)
				if err != nil {
					return nil, err
				}
				log.Println("pbtunnel sending CONNECT", addr, peerID, "...")
				_, err = fmt.Fprintf(stream, "CONNECT %s HTTP/1.1\r\n\r\n", addr)
				if err != nil {
					stream.Close()
					return nil, err
				}
				var buf bytes.Buffer
				var cnter int
				for cnter < 9999 {
					cnter ++
					var b [1]byte
					_, err = stream.Read(b[:])
					if err != nil {
						stream.Close()
						return nil, fmt.Errorf("CONNECT response: %w", err)
					}
					buf.WriteByte(b[0])
					if buf.Len() >= 4 && bytes.HasSuffix(buf.Bytes(), []byte("\r\n\r\n")) {
						break
					}
				}
				if cnter >= 9999 {
					return nil, fmt.Errorf("CONNECT failed: no 200 continue or establish %d", cnter)
				}
				if !bytes.Contains(buf.Bytes(), []byte("200")) {
					log.Println("pbtunnel:", string(buf.Bytes()))
					stream.Close()
					return nil, fmt.Errorf("CONNECT failed: %s", bytes.TrimRight(buf.Bytes(), "\r\n"))
				}
				return &P2PConn{Stream: stream}, nil
			},
		},
	}
}

type DriftServer struct {
	peerID   string
	listener net.Listener
	udpConn  net.PacketConn
	mu       sync.Mutex
	wg       sync.WaitGroup
	udpSessions map[string]*udpSession
	udpTimeout  time.Duration
	udpCtx      context.Context
	udpCancel   context.CancelFunc
}

type udpSession struct {
	remoteAddr net.Addr
	stream     network.Stream
	cancel     context.CancelFunc
	lastUse    time.Time
}

func NewDriftServer(peerID string) *DriftServer {
	return &DriftServer{
		peerID:    peerID,
		udpTimeout: 5 * time.Minute,
	}
}

func (s *DriftServer) SwitchPeer(peerID string) string {
	oldval := s.peerID
	s.peerID = peerID
	return oldval
}

func (s *DriftServer) Listen(addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s.mu.Lock()
	s.listener = l
	s.mu.Unlock()

	for {
		conn, err := l.Accept()
		if err != nil {
			break
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handle(conn)
		}()
	}
	return nil
}

func (s *DriftServer) Close() error {
	s.mu.Lock()
	l := s.listener
	uc := s.udpConn
	cancel := s.udpCancel
	sessions := make([]*udpSession, 0, len(s.udpSessions))
	for _, sess := range s.udpSessions {
		sessions = append(sessions, sess)
	}
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if l != nil {
		l.Close()
	}
	if uc != nil {
		uc.Close()
	}
	for _, sess := range sessions {
		sess.cancel()
		sess.stream.Close()
	}
	s.wg.Wait()
	return nil
}

func (s *DriftServer) ListenUDP(addr string) error {
	pc, err := net.ListenPacket("udp", addr)
	if err != nil {
		return err
	}

	s.mu.Lock()
	s.udpConn = pc
	s.udpSessions = make(map[string]*udpSession)
	s.udpCtx, s.udpCancel = context.WithCancel(context.Background())
	s.mu.Unlock()

	go s.reapUDP()

	buf := make([]byte, udpBufSize)
	for {
		n, remoteAddr, err := pc.ReadFrom(buf)
		if err != nil {
			break
		}

		key := remoteAddr.String()

		s.mu.Lock()
		sess, exists := s.udpSessions[key]
		if exists {
			sess.lastUse = time.Now()
		}
		s.mu.Unlock()

		if !exists {
			sess = s.newUDPSession(remoteAddr)
			if sess == nil {
				continue
			}
		}

		hdr := []byte{0, 0}
		binary.BigEndian.PutUint16(hdr, uint16(n))
		if _, err := writen(sess.stream, append(hdr, buf[:n]...), n+2); err != nil {
			s.removeUDPSession(key)
		}
	}
	return nil
}

func (s *DriftServer) newUDPSession(remoteAddr net.Addr) *udpSession {
	peerhum := s.peerID
	rdport := (rand.Uint32()/2)%(65535-21) + 21
	rdproto := fmt.Sprintf("%s%v", udpTunnelProto, rdport)
	rdproto = tunnelProto // disable rand port now

	stream, err := p2put.OpenStream(context.Background(), peerhum, rdproto)
	if err != nil {
		log.Printf("[pbtunnel] udp-session dial %s: %v", peerhum, err)
		return nil
	}

	_, cancel := context.WithCancel(s.udpCtx)
	sess := &udpSession{
		remoteAddr: remoteAddr,
		stream:     stream,
		cancel:     cancel,
		lastUse:    time.Now(),
	}

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer cancel()
		hdr := make([]byte, 2)
		for {
			_, err := io.ReadFull(stream, hdr)
			if err != nil {
				return
			}
			length := int(binary.BigEndian.Uint16(hdr))
			buf := make([]byte, length)
			_, err = io.ReadFull(stream, buf)
			if err != nil {
				return
			}
			if _, err := s.udpConn.WriteTo(buf, remoteAddr); err != nil {
				return
			}
			s.mu.Lock()
			// update lastUse on successful response
			if existing, ok := s.udpSessions[remoteAddr.String()]; ok && existing == sess {
				existing.lastUse = time.Now()
			}
			s.mu.Unlock()
		}
	}()

	key := remoteAddr.String()
	s.mu.Lock()
	s.udpSessions[key] = sess
	s.mu.Unlock()

	return sess
}

func (s *DriftServer) removeUDPSession(key string) {
	s.mu.Lock()
	sess, ok := s.udpSessions[key]
	if ok {
		delete(s.udpSessions, key)
	}
	s.mu.Unlock()
	if ok {
		sess.cancel()
		sess.stream.Close()
	}
}

func (s *DriftServer) reapUDP() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-s.udpCtx.Done():
			return
		case <-ticker.C:
			s.mu.Lock()
			now := time.Now()
			for key, sess := range s.udpSessions {
				if now.Sub(sess.lastUse) > s.udpTimeout {
					delete(s.udpSessions, key)
					sess.cancel()
					sess.stream.Close()
				}
			}
			s.mu.Unlock()
		}
	}
}

///////

var (
	peerips   = map[string]string{} // ip => peerid
	peeripsMu sync.Mutex
)
var vlanpfx = "10.0.0."

func stringToHostPart(s string) int {
	tbl := crc64.MakeTable(crc64.ECMA)
	h := crc64.Checksum([]byte(s), tbl)
	return int(h%253) + 2
}

// return empty for default
func peeridByConnIP(ipport string) string {
	rawIP, _, err := net.SplitHostPort(ipport)
	if err != nil {
		return ""
	}
	if rawIP == "127.0.0.1" {
		return ""
	}

	peeripsMu.Lock()
	defer peeripsMu.Unlock()

	if id, ok := peerips[rawIP]; ok {
		return id
	}

	ids := p2put.GetClusterPeers()
	sort.Strings(ids)
	for _, id := range ids {
		hostPart := stringToHostPart(id)
		mappedIP := vlanpfx + strconv.Itoa(hostPart)
		peerips[mappedIP] = id
	}
	return peerips[rawIP]
}

func (s *DriftServer) handle(conn net.Conn) {
	s.handleP2x(conn)
}
func (s *DriftServer) handleTox2x(conn net.Conn) {
}
func (s *DriftServer) handleTurn2x(conn net.Conn) {
}
func (s *DriftServer) handleP2x(conn net.Conn) {
	remoteAddr := conn.RemoteAddr().String()
	localAddr := conn.LocalAddr().String()
	start := time.Now()
	peerhum := s.peerID
	if p := peeridByConnIP(localAddr); p != "" {
		peerhum = p
	}

	var connCloseOnce sync.Once
	connClose := func() { connCloseOnce.Do(func() { conn.Close() }) }
	defer connClose()

	peerid, _ := peer.Decode(peerhum)
	var preConnType = "[UNCONNECT]"
	if p2put.PeerIsConnected(peerid, false) {
		if p2put.PeerIsConnected(peerid, true) {
			preConnType = "[DIRECT]"
		} else {
			preConnType = "[RELAY]"
		}
	}
	log.Printf("[pbtunnel] pre-conn: %s %s %s", peerid.ShortString(), preConnType, localAddr)

	openStart := time.Now()
	rdport := (rand.Uint32()/2)%(65535-21) + 21
	rdproto := fmt.Sprintf("%s%v", tunnelProto, rdport)
	rdproto = tunnelProto // disable rand port now
	p2pStream, err := p2put.OpenStream(context.Background(), peerhum, rdproto)
	openDur := time.Since(openStart)
	log.Printf("[pbtunnel] drift dial %s: %v (open=%s) %s %s", peerid.ShortString(), err, openDur.Round(time.Millisecond), rdproto, preConnType)
	if err != nil {
		return
	}

	var connType string
	if p2pStream.Conn().Stat().Limited {
		connType = "[RELAY]"
	} else {
		connType = "[DIRECT]"
	}

	var streamCloseOnce sync.Once
	streamClose := func() { streamCloseOnce.Do(func() { p2pStream.Close() }) }
	defer streamClose()

	var localSent, localRecv int64
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer log.Println("xfer tun <- sock", peerid.ShortString(), connType)
		defer wg.Done()
		defer streamClose()
		buf := make([]byte, bufSize)
		for {
			n, rerr := conn.Read(buf)
			if n > 0 {
				wn, _ := writen(p2pStream, buf, n)
				localRecv += int64(wn)
				if wn != n {
					return
				}
			}
			if rerr != nil {
				if sc, ok := p2pStream.(interface{ CloseWrite() error }); ok {
					sc.CloseWrite()
				}
				return
			}
		}
	}()

	go func() {
		defer log.Println("xfer tun -> sock", peerid.ShortString(), connType)
		defer wg.Done()
		defer connClose()
		buf := make([]byte, bufSize)
		for {
			n, rerr := p2pStream.Read(buf)
			if n > 0 {
				wn, _ := writen(conn, buf, n)
				localSent += int64(wn)
				if wn != n {
					return
				}
			}
			if rerr != nil {
				return
			}
		}
	}()

	log.Println("wg.Wait() ...", peerid.ShortString(), connType)
	wg.Wait()
	dur := time.Since(start)
	log.Printf("[pbtunnel] drift closed: %s peer=%s recv=%d sent=%d open=%s dur=%s%s", remoteAddr, peerid.ShortString(), localRecv, localSent, openDur.Round(time.Millisecond), dur.Round(time.Millisecond), connType)
}
