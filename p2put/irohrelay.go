package p2put

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"net"
//	"net/http"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/envsh/toxera/fedkey"
	"github.com/gorilla/websocket"
	"lukechampine.com/blake3"
)

// ───── Protocol Constants ─────

const relaySubprotocol = "iroh-relay-v2"
const domainSepChallenge = "iroh-relay handshake v1 challenge signature"
const maxDatagramPayload = 64000
const relayPingInterval = 9 * time.Second
const streamIDBase = 1000

// Handshake frame types
const (
	hsServerChallenge    byte = 0
	hsClientAuth         byte = 1
	hsServerConfirmsAuth byte = 2
	hsServerDeniesAuth   byte = 3
)

// Relay data frame types
const (
	frameClientToRelayDatagram      byte = 4
	frameClientToRelayDatagramBatch byte = 5
	frameRelayToClientDatagram      byte = 6
	frameRelayToClientDatagramBatch byte = 7
	frameEndpointGone               byte = 8
	framePing                       byte = 9
	framePong                       byte = 10
	frameHealth                     byte = 11
	frameRestarting                 byte = 12
	frameStatus                     byte = 13
)

// Status frame discriminants (RelayToClientMsg::Status)
const (
	statusHealthy               = 0
	statusSameEndpointIdConnected = 1
)

// Stream flags
const (
	flagDATA  byte = 0
	flagCLOSE byte = 1
	flagRESET byte = 2
)

var relayNodes = map[string]string{
	"use1": "wss://use1-1.relay.n0.iroh-canary.iroh.link/relay",
	"usw1": "wss://usw1-1.relay.n0.iroh-canary.iroh.link/relay",
	"euc1": "wss://euc1-1.relay.n0.iroh-canary.iroh.link/relay",
	"aps1": "wss://aps1-1.relay.n0.iroh-canary.iroh.link/relay",
}

// ───── VarInt / LEB128 (compatible with iroh relay protocol) ─────

func appendVarInt(buf []byte, v uint64) []byte {
	switch {
	case v <= 63:
		return append(buf, byte(v))
	case v <= 16383:
		return append(buf, byte(v>>8)|0x40, byte(v))
	case v <= 1073741823:
		return append(buf, byte(v>>24)|0x80, byte(v>>16), byte(v>>8), byte(v))
	default:
		return append(buf, byte(v>>56)|0xC0, byte(v>>48), byte(v>>40), byte(v>>32), byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
	}
}

func appendLEB128(buf []byte, v uint64) []byte {
	for {
		b := byte(v & 0x7F)
		v >>= 7
		if v != 0 {
			b |= 0x80
		}
		buf = append(buf, b)
		if v == 0 {
			break
		}
	}
	return buf
}

func consumeLEB128(buf []byte) (uint64, int, error) {
	var v uint64
	for i, b := range buf {
		v |= uint64(b&0x7F) << (7 * i)
		if b&0x80 == 0 {
			return v, i + 1, nil
		}
	}
	return 0, 0, io.ErrUnexpectedEOF
}

// ───── IrohRelayState ─────

type IrohRelayState int

const (
	IrohRelayDisconnected IrohRelayState = iota
	IrohRelayConnecting
	IrohRelayAuthenticating
	IrohRelayAlive
	IrohRelayReconnecting
)

func (s IrohRelayState) String() string {
	switch s {
	case IrohRelayDisconnected:
		return "disconnected"
	case IrohRelayConnecting:
		return "connecting"
	case IrohRelayAuthenticating:
		return "authenticating"
	case IrohRelayAlive:
		return "alive"
	case IrohRelayReconnecting:
		return "reconnecting"
	default:
		return "unknown"
	}
}

type IrohRelayStats struct {
	URL       string
	State     IrohRelayState
	Uptime    time.Duration
	PubkeyHex string
	TxBytes   int64
	RxBytes   int64
	FailCount int
}

// ───── IrohRelayStream (implements net.Conn) ─────

type irohAddr struct {
	pubkey [32]byte
}

func (a *irohAddr) Network() string { return "iroh" }
func (a *irohAddr) String() string  { return hex.EncodeToString(a.pubkey[:]) }

type IrohRelayStream struct {
	id     uint64
	remote [32]byte
	pool   *IrohRelayPool

	mu     sync.Mutex
	rxBuf  bytes.Buffer
	rxCond *sync.Cond
	closed bool
	reset  bool

	localAddr  *irohAddr
	remoteAddr *irohAddr
}

func newStream(id uint64, remote [32]byte, pool *IrohRelayPool) *IrohRelayStream {
	s := &IrohRelayStream{
		id:         id,
		remote:     remote,
		pool:       pool,
		localAddr:  &irohAddr{pool.localPub},
		remoteAddr: &irohAddr{remote},
	}
	s.rxCond = sync.NewCond(&s.mu)
	return s
}

func (s *IrohRelayStream) Read(b []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for s.rxBuf.Len() == 0 {
		if s.reset {
			return 0, net.ErrClosed
		}
		if s.closed {
			return 0, io.EOF
		}
		s.rxCond.Wait()
	}
	return s.rxBuf.Read(b)
}

func (s *IrohRelayStream) Write(b []byte) (int, error) {
	total := len(b)
	for len(b) > 0 {
		chunk := b
		if len(chunk) > maxDatagramPayload {
			chunk = chunk[:maxDatagramPayload]
		}
		frame := make([]byte, 0, 9+len(chunk))
		frame = binary.LittleEndian.AppendUint64(frame, s.id)
		frame = append(frame, flagDATA)
		frame = append(frame, chunk...)

		if err := s.pool.sendTo(s.remote, frame); err != nil {
			return total - len(b), err
		}
		b = b[len(chunk):]
	}
	return total, nil
}

func (s *IrohRelayStream) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return net.ErrClosed
	}
	s.closed = true
	s.mu.Unlock()

	frame := make([]byte, 9)
	binary.LittleEndian.PutUint64(frame, s.id)
	frame[8] = flagCLOSE
	s.pool.sendTo(s.remote, frame)

	s.pool.removeStream(s.remote, s.id)
	return nil
}

func (s *IrohRelayStream) Reset() error {
	s.mu.Lock()
	s.reset = true
	s.rxBuf.Reset()
	s.rxCond.Broadcast()
	s.mu.Unlock()

	frame := make([]byte, 9)
	binary.LittleEndian.PutUint64(frame, s.id)
	frame[8] = flagRESET
	s.pool.sendTo(s.remote, frame)

	s.pool.removeStream(s.remote, s.id)
	return nil
}

func (s *IrohRelayStream) LocalAddr() net.Addr  { return s.localAddr }
func (s *IrohRelayStream) RemoteAddr() net.Addr { return s.remoteAddr }

// dispatch is called from readLoop goroutine
func (s *IrohRelayStream) dispatch(flags byte, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch {
	case flags == flagRESET:
		s.reset = true
		s.rxBuf.Reset()
		s.rxCond.Broadcast()
	case flags == flagCLOSE:
		s.closed = true
		s.rxCond.Broadcast()
	case flags == flagDATA:
		s.rxBuf.Write(data)
		s.rxCond.Broadcast()
	}
}

// ───── peerStreamManager ─────

type peerStreamManager struct {
	pool      *IrohRelayPool
	remotePub [32]byte
	oddBit    uint64
	nextID    uint64
	streams   map[uint64]*IrohRelayStream
	mu        sync.Mutex
}

func newPeerStreamManager(pool *IrohRelayPool, remotePub [32]byte) *peerStreamManager {
	oddBit := uint64(0)
	if bytes.Compare(pool.localPub[:], remotePub[:]) < 0 {
		oddBit = 1
	}
	return &peerStreamManager{
		pool:      pool,
		remotePub: remotePub,
		oddBit:    oddBit,
		nextID:    streamIDBase,
		streams:   make(map[uint64]*IrohRelayStream),
	}
}

func (m *peerStreamManager) allocID() uint64 {
	id := atomic.AddUint64(&m.nextID, 2) - 2
	return id | m.oddBit
}

func (m *peerStreamManager) dispatch(sid uint64, flags byte, data []byte) {
	m.mu.Lock()
	s, ok := m.streams[sid]

	if !ok {
		if flags != flagDATA {
			m.mu.Unlock()
			return
		}
		s = newStream(sid, m.remotePub, m.pool)
		m.streams[sid] = s
		m.mu.Unlock()

		select {
		case m.pool.acceptCh <- s:
		default:
			m.mu.Lock()
			delete(m.streams, sid)
			m.mu.Unlock()
			go m.pool.sendRESET(m.remotePub, sid)
		}
		return
	}
	m.mu.Unlock()

	s.dispatch(flags, data)
}

func (m *peerStreamManager) remove(sid uint64) {
	m.mu.Lock()
	delete(m.streams, sid)
	m.mu.Unlock()
}

// ───── irohRelayConn (single WSS connection to one relay server) ─────

type irohRelayConn struct {
	url    string
	pool   *IrohRelayPool
	pubkey [32]byte

	state   IrohRelayState
	stateMu sync.RWMutex

	mu        sync.Mutex
	ws        *websocket.Conn
	createdAt time.Time

	stopCh       chan struct{}
	reconnectNow chan struct{}

	lastRecv    int64  // atomic, UnixNano of last received frame
	pingSeq     uint64 // atomic, next ping seq
	pendingPing uint64 // atomic, outstanding ping seq (0=none)
	lastPong    int64  // atomic, UnixNano of last received pong
	pongCnt     int64  // atomic, total pongs received

	txBytes   int64
	rxBytes   int64
	failCount int32
}

func newRelayConn(url string, pool *IrohRelayPool) *irohRelayConn {
	return &irohRelayConn{
		url:          url,
		pool:         pool,
		createdAt:    time.Now(),
		stopCh:       make(chan struct{}),
		reconnectNow: make(chan struct{}, 1),
		lastRecv:     time.Now().UnixNano(),
		pingSeq:      0,
		pendingPing:  0,
		lastPong:     time.Now().UnixNano(),
		pongCnt:      0,
	}
}

func (rc *irohRelayConn) start() {
	go rc.runLoop()
}

func (rc *irohRelayConn) stop() {
	close(rc.stopCh)
}

func (rc *irohRelayConn) runLoop() {
	if err := rc.connect(); err != nil {
		log.Printf("[irohrelay] runLoop: initial connect %s: %v", rc.url, err)
	}

	keepAliveTick := time.NewTicker(relayPingInterval)
	defer keepAliveTick.Stop()

	for {
		if rc.getState() != IrohRelayAlive {
			rc.shutdown()
			select {
			case <-rc.stopCh:
				return
			default:
			}
			rc.connectWithBackoff()
			continue
		}

		select {
		case <-rc.stopCh:
			rc.shutdown()
			return
		case <-rc.reconnectNow:
			rc.shutdown()
			continue
		case <-keepAliveTick.C:
			lr := time.Unix(0, atomic.LoadInt64(&rc.lastRecv))
			if time.Since(lr) > relayPingInterval*3 {
				log.Printf("[irohrelay] ping timeout %s", rc.url)
				rc.shutdown()
				continue
			}
			if atomic.LoadUint64(&rc.pendingPing) != 0 {
				lp := time.Unix(0, atomic.LoadInt64(&rc.lastPong))
				lr := time.Unix(0, atomic.LoadInt64(&rc.lastRecv))
				pc := atomic.LoadInt64(&rc.pongCnt)
				log.Printf("[irohrelay] pendingPing skip, lastPong=%.0fs ago lastRecv=%.0fs ago pongCnt=%d %s", time.Since(lp).Seconds(), time.Since(lr).Seconds(), pc, rc.url)
				continue
			}
			seq := atomic.AddUint64(&rc.pingSeq, 1)
			atomic.StoreUint64(&rc.pendingPing, seq)
			buf := make([]byte, 8)
			binary.LittleEndian.PutUint64(buf, seq)
			if err := rc.writeFrame(framePing, buf); err != nil {
				log.Printf("[irohrelay] ping write error %s: %v", rc.url, err)
				rc.shutdown()
			}
		}
	}
}

func (rc *irohRelayConn) connect() error {
	rc.setState(IrohRelayConnecting)
	select { // drain stale reconnect signal from previous lifecycle
	case <-rc.reconnectNow:
	default:
	}
	log.Printf("[irohrelay] dial %s ...", rc.url)

	u, err := url.Parse(rc.url)
	if err != nil {
		rc.setState(IrohRelayDisconnected)
		return fmt.Errorf("parse url %s: %w", rc.url, err)
	}

	dialer := &websocket.Dialer{
		TLSClientConfig: &tls.Config{ServerName: u.Hostname()},
		Subprotocols:    []string{relaySubprotocol},
	}

	ws, resp, err := dialer.Dial(rc.url, nil)
	if err != nil {
		rc.setState(IrohRelayDisconnected)
		return fmt.Errorf("dial %s: %w", rc.url, err)
	}
	ws.SetReadLimit(1 << 20)

	if resp != nil {
		log.Printf("[irohrelay] %s: upgrade %s proto=%q server=%q",
			rc.url, resp.Status, resp.Header.Get("Sec-WebSocket-Protocol"),
			resp.Header.Get("Server"))
	}

	if ws.Subprotocol() != relaySubprotocol {
		log.Printf("[irohrelay] %s: subprotocol %q, expected %q", rc.url, ws.Subprotocol(), relaySubprotocol)
	}

	rc.mu.Lock()
	rc.ws = ws
	rc.mu.Unlock()

	rc.setState(IrohRelayAuthenticating)
	log.Printf("[irohrelay] handshake %s ...", rc.url)
	if err := rc.handshake(); err != nil {
		ws.Close()
		rc.setState(IrohRelayDisconnected)
		return fmt.Errorf("handshake %s: %w", rc.url, err)
	}

	go rc.readLoop()

	atomic.StoreInt64(&rc.lastRecv, time.Now().UnixNano())
	rc.pubkey = rc.pool.localPub
	rc.failCount = 0
	rc.setState(IrohRelayAlive)
	log.Printf("[irohrelay] connected %s pubkey=%X", rc.url, rc.pubkey[:8])
	return nil
}

func (rc *irohRelayConn) handshake() error {
	_, msg, err := rc.ws.ReadMessage()
	if err != nil {
		return fmt.Errorf("read ServerChallenge: %w", err)
	}
	if len(msg) < 17 || msg[0] != hsServerChallenge {
		return fmt.Errorf("expected ServerChallenge, got type %d len %d", msg[0], len(msg))
	}
	var challenge [16]byte
	copy(challenge[:], msg[1:17])

	msgToSign := make([]byte, 32)
	blake3.DeriveKey(msgToSign, domainSepChallenge, challenge[:])
	sig := ed25519.Sign(rc.pool.localKey, msgToSign)

	auth := make([]byte, 0, 1+32+1+64)
	auth = append(auth, byte(hsClientAuth))
	auth = append(auth, rc.pool.localPub[:]...)
	auth = appendLEB128(auth, 64)
	auth = append(auth, sig...)

	if err := rc.ws.WriteMessage(websocket.BinaryMessage, auth); err != nil {
		return fmt.Errorf("send ClientAuth: %w", err)
	}

	_, msg, err = rc.ws.ReadMessage()
	if err != nil {
		return fmt.Errorf("read auth response: %w", err)
	}
	if len(msg) == 0 {
		return fmt.Errorf("empty auth response")
	}
	switch msg[0] {
	case hsServerConfirmsAuth:
		return nil
	case hsServerDeniesAuth:
		reasonLen, n, err := consumeLEB128(msg[1:])
		if err != nil {
			return fmt.Errorf("parse denial: %w", err)
		}
		reason := string(msg[1+n : 1+n+int(reasonLen)])
		return fmt.Errorf("server denied auth: %s", reason)
	default:
		return fmt.Errorf("unexpected auth response type %d", msg[0])
	}
}

func (rc *irohRelayConn) readLoop() {
	for {
		select {
		case <-rc.stopCh:
			return
		default:
		}

		rc.mu.Lock()
		ws := rc.ws
		rc.mu.Unlock()
		if ws == nil {
			return
		}

		_, msg, err := ws.ReadMessage()
		if err != nil {
			select {
			case <-rc.stopCh:
			default:
				if closeErr, ok := err.(*websocket.CloseError); ok {
					log.Printf("[irohrelay] close %s: code=%d text=%q", rc.url, closeErr.Code, closeErr.Text)
				} else {
					log.Printf("[irohrelay] read error %s: %v", rc.url, err)
				}
				select {
				case rc.reconnectNow <- struct{}{}:
				default:
				}
			}
			return
		}
		if len(msg) == 0 {
			continue
		}

		// B: any frame = alive
		atomic.StoreInt64(&rc.lastRecv, time.Now().UnixNano())

		frameType := msg[0]
		body := msg[1:]

		switch frameType {
		case framePing:
			log.Printf("[irohrelay] pong %s", rc.url)
			if err := rc.sendPong(body); err != nil {
				log.Printf("[irohrelay] send pong error %s: %v, exiting", rc.url, err)
				return
			}
		case framePong:
			// A: match seq to clear pending
			if len(body) >= 8 {
				seq := binary.LittleEndian.Uint64(body[:8])
				if seq == atomic.LoadUint64(&rc.pendingPing) {
					atomic.StoreUint64(&rc.pendingPing, 0)
				}
			atomic.StoreInt64(&rc.lastPong, time.Now().UnixNano())
			atomic.AddInt64(&rc.pongCnt, 1)
		}
		case frameRelayToClientDatagram, frameRelayToClientDatagramBatch:
			rc.handleRecvDatagram(body)
		case frameEndpointGone:
			if len(body) >= 32 {
				var gonePub [32]byte
				copy(gonePub[:], body[:32])
				rc.pool.peerGone(gonePub)
			}
		case frameHealth:
			log.Printf("[irohrelay] %s: Health %q", rc.url, string(body))
		case frameRestarting:
			if len(body) >= 8 {
				reconnectIn := binary.BigEndian.Uint32(body[:4])
				tryFor := binary.BigEndian.Uint32(body[4:8])
				log.Printf("[irohrelay] %s: Restarting reconnect_in=%dms try_for=%dms", rc.url, reconnectIn, tryFor)
			}
		case frameStatus:
			if len(body) >= 1 {
				switch body[0] {
				case statusHealthy:
					log.Printf("[irohrelay] %s: Status Healthy", rc.url)
				case statusSameEndpointIdConnected:
					log.Printf("[irohrelay] %s: Status SameEndpointIdConnected", rc.url)
				default:
					log.Printf("[irohrelay] %s: Status Unknown(%d)", rc.url, body[0])
				}
			}
		default:
			log.Printf("[irohrelay] %s: unknown frame type %d", rc.url, frameType)
		}
	}
}

func (rc *irohRelayConn) handleRecvDatagram(body []byte) {
	if len(body) < 33 {
		return
	}
	var src [32]byte
	copy(src[:], body[:32])
	payload := body[33:]

	if len(payload) < 9 {
		return
	}
	sid := binary.LittleEndian.Uint64(payload[:8])
	flags := payload[8]
	data := payload[9:]

	rc.pool.dispatchStreamFrame(src, sid, flags, data)
}

func (rc *irohRelayConn) writeFrame(tag byte, payload []byte) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()
	if rc.ws == nil {
		return fmt.Errorf("relay %s: ws closed", rc.url)
	}
	return rc.ws.WriteMessage(websocket.BinaryMessage, append([]byte{tag}, payload...))
}

func (rc *irohRelayConn) sendDatagram(remotePub [32]byte, payload []byte) error {
	msg := make([]byte, 0, 1+32+1+len(payload))
	msg = append(msg, byte(frameClientToRelayDatagram))
	msg = append(msg, remotePub[:]...)
	msg = append(msg, 0)
	msg = append(msg, payload...)

	rc.mu.Lock()
	defer rc.mu.Unlock()
	if rc.ws == nil {
		return fmt.Errorf("relay %s: ws closed", rc.url)
	}
	return rc.ws.WriteMessage(websocket.BinaryMessage, msg)
}

func (rc *irohRelayConn) sendPong(payload []byte) error {
	return rc.writeFrame(framePong, payload)
}

func (rc *irohRelayConn) connectWithBackoff() {
	backoff := 1 * time.Second
	maxBackoff := 60 * time.Second

	for {
		if err := rc.connect(); err != nil {
			log.Printf("[irohrelay] connect %s: %v", rc.url, err)
		}
		if rc.getState() == IrohRelayAlive {
			return
		}

		select {
		case <-rc.stopCh:
			return
		case <-time.After(backoff):
		}
		log.Printf("[irohrelay] reconnect %s backoff=%s", rc.url, backoff)
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func (rc *irohRelayConn) shutdown() {
	rc.setState(IrohRelayDisconnected)

	rc.mu.Lock()
	if rc.ws != nil {
		rc.ws.Close()
		rc.ws = nil
	}
	rc.mu.Unlock()
}

func (rc *irohRelayConn) setState(s IrohRelayState) {
	rc.stateMu.Lock()
	rc.state = s
	rc.stateMu.Unlock()
	log.Printf("[irohrelay] relay %s state=%s", rc.url, s)
}

func (rc *irohRelayConn) getState() IrohRelayState {
	rc.stateMu.RLock()
	defer rc.stateMu.RUnlock()
	return rc.state
}

// ───── IrohRelayPool ─────

var irohRelayPool = NewIrohRelayPool()

type IrohRelayPool struct {
	mu       sync.Mutex
	relays   map[string]*irohRelayConn
	ctx      context.Context
	cancel   context.CancelFunc
	stopCh   chan struct{}
	started  bool
	keyFile  string
	localKey ed25519.PrivateKey
	localPub [32]byte
	acceptCh chan *IrohRelayStream

	pmap   map[[32]byte]*peerStreamManager
	peerMu sync.Mutex
}

func NewIrohRelayPool() *IrohRelayPool {
	return &IrohRelayPool{
		relays: make(map[string]*irohRelayConn),
		pmap:   make(map[[32]byte]*peerStreamManager),
	}
}

func GetIrohRelayPool() *IrohRelayPool {
	return irohRelayPool
}

func (p *IrohRelayPool) Start(ctx context.Context, keyFile string) error {
	p.mu.Lock()
	if p.started {
		p.mu.Unlock()
		return nil
	}
	p.started = true
	p.keyFile = keyFile
	p.ctx, p.cancel = context.WithCancel(ctx)
	p.stopCh = make(chan struct{})
	p.acceptCh = make(chan *IrohRelayStream, 64)
	p.mu.Unlock()

	kr, err := fedkey.LoadKeyRing(keyFile, true)
	if err != nil {
		p.mu.Lock()
		p.started = false
		p.mu.Unlock()
		return fmt.Errorf("irohrelay load key: %w", err)
	}
	p.localKey = kr.BTDHTKey()
	copy(p.localPub[:], p.localKey.Public().(ed25519.PublicKey))
	log.Printf("[irohrelay] pool start nodeID=%X key=%s", p.localPub[:], keyFile)

	p.mu.Lock()
	for _, rc := range p.relays {
		go rc.start()
	}
	p.mu.Unlock()
	return nil
}

func (p *IrohRelayPool) Stop() {
	p.mu.Lock()
	if !p.started {
		p.mu.Unlock()
		return
	}
	p.started = false
	if p.cancel != nil {
		p.cancel()
	}
	close(p.stopCh)
	for _, rc := range p.relays {
		rc.stop()
	}
	p.mu.Unlock()
	log.Printf("[irohrelay] pool stopped")
}

func (p *IrohRelayPool) AddRelay(url string) error {
	rc := newRelayConn(url, p)

	p.mu.Lock()
	if _, exists := p.relays[url]; exists {
		p.mu.Unlock()
		return fmt.Errorf("irohrelay %q already added", url)
	}
	p.relays[url] = rc
	started := p.started
	p.mu.Unlock()

	if started {
		go rc.start()
	}
	log.Printf("[irohrelay] added relay %s", url)
	return nil
}

func (p *IrohRelayPool) RemoveRelay(url string) {
	p.mu.Lock()
	rc, ok := p.relays[url]
	if ok {
		rc.stop()
		delete(p.relays, url)
	}
	p.mu.Unlock()
	if ok {
		log.Printf("[irohrelay] removed relay %s", url)
	}
}

func (p *IrohRelayPool) OpenStream(ctx context.Context, remotePub [32]byte) (*IrohRelayStream, error) {
	p.peerMu.Lock()
	mgr, ok := p.pmap[remotePub]
	if !ok {
		mgr = newPeerStreamManager(p, remotePub)
		p.pmap[remotePub] = mgr
	}
	p.peerMu.Unlock()

	sid := mgr.allocID()
	s := newStream(sid, remotePub, p)

	mgr.mu.Lock()
	mgr.streams[sid] = s
	mgr.mu.Unlock()

	log.Printf("[irohrelay] stream %d: open to %X", sid, remotePub[:8])
	return s, nil
}

func (p *IrohRelayPool) AcceptStream(ctx context.Context) (*IrohRelayStream, error) {
	select {
	case s := <-p.acceptCh:
		if s == nil {
			return nil, fmt.Errorf("irohrelay: acceptCh got nil")
		}
		log.Printf("[irohrelay] stream %d: accept from %X", s.id, s.remote[:8])
		return s, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-p.stopCh:
		return nil, fmt.Errorf("irohrelay: pool stopped")
	}
}

func (p *IrohRelayPool) ListStats() []IrohRelayStats {
	p.mu.Lock()
	defer p.mu.Unlock()

	stats := make([]IrohRelayStats, 0, len(p.relays))
	for _, rc := range p.relays {
		rc.mu.Lock()
		tx := atomic.LoadInt64(&rc.txBytes)
		rx := atomic.LoadInt64(&rc.rxBytes)
		rc.mu.Unlock()

		stats = append(stats, IrohRelayStats{
			URL:       rc.url,
			State:     rc.getState(),
			Uptime:    time.Since(rc.createdAt),
			PubkeyHex: hex.EncodeToString(rc.pubkey[:]),
			TxBytes:   tx,
			RxBytes:   rx,
			FailCount: int(atomic.LoadInt32(&rc.failCount)),
		})
	}
	return stats
}

// ───── internal ─────

func (p *IrohRelayPool) sendTo(remotePub [32]byte, frame []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, rc := range p.relays {
		if rc.getState() != IrohRelayAlive {
			continue
		}
		if err := rc.sendDatagram(remotePub, frame); err != nil {
			return err
		}
		atomic.AddInt64(&rc.txBytes, int64(len(frame)))
		return nil
	}
	return fmt.Errorf("irohrelay: no alive relay to send")
}

func (p *IrohRelayPool) sendRESET(remotePub [32]byte, sid uint64) {
	frame := make([]byte, 9)
	binary.LittleEndian.PutUint64(frame, sid)
	frame[8] = flagRESET
	p.sendTo(remotePub, frame)
}

func (p *IrohRelayPool) dispatchStreamFrame(src [32]byte, sid uint64, flags byte, data []byte) {
	p.peerMu.Lock()
	mgr, ok := p.pmap[src]
	if !ok {
		mgr = newPeerStreamManager(p, src)
		p.pmap[src] = mgr
	}
	p.peerMu.Unlock()

	mgr.dispatch(sid, flags, data)
}

func (p *IrohRelayPool) removeStream(remote [32]byte, sid uint64) {
	p.peerMu.Lock()
	if mgr, ok := p.pmap[remote]; ok {
		mgr.mu.Lock()
		delete(mgr.streams, sid)
		mgr.mu.Unlock()
	}
	p.peerMu.Unlock()
}

func (p *IrohRelayPool) peerGone(pubkey [32]byte) {
	p.peerMu.Lock()
	mgr, ok := p.pmap[pubkey]
	if ok {
		delete(p.pmap, pubkey)
	}
	p.peerMu.Unlock()
	if !ok {
		return
	}

	mgr.mu.Lock()
	streams := make([]*IrohRelayStream, 0, len(mgr.streams))
	for _, s := range mgr.streams {
		streams = append(streams, s)
	}
	mgr.streams = make(map[uint64]*IrohRelayStream)
	mgr.mu.Unlock()

	for _, s := range streams {
		s.dispatch(flagRESET, nil)
	}
	log.Printf("[irohrelay] peer gone %X", pubkey[:8])
}
