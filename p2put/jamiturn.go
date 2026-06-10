package p2put

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/pion/transport/v2"
	"github.com/pion/turn/v2"
)

var turnPool = NewTurnPool()

type TurnServerConfig struct {
	Addr     string
	Protocol string
	Username string
	Password string
	Realm    string

	CredentialTTL time.Duration
	MaxBPS        int64
	MaxAllocBW    int64
}

type TurnState int

const (
	TurnDisconnected  TurnState = iota
	TurnConnecting
	TurnAuthenticating
	TurnAllocating
	TurnAlive
	TurnReconnecting
)

func (s TurnState) String() string {
	switch s {
	case TurnDisconnected:
		return "disconnected"
	case TurnConnecting:
		return "connecting"
	case TurnAuthenticating:
		return "authenticating"
	case TurnAllocating:
		return "allocating"
	case TurnAlive:
		return "alive"
	case TurnReconnecting:
		return "reconnecting"
	default:
		return "unknown"
	}
}

type TurnStats struct {
	State     TurnState
	Uptime    time.Duration
	RelayAddr string
	TxBytes   int64
	RxBytes   int64
	FailCount int
}

// tcpAlloc avoids importing pion/turn's internal/client package.
type tcpAlloc interface {
	Addr() net.Addr
	AcceptTCP() (transport.TCPConn, error)
	Close() error
}

type rateLimiter struct {
	mu       sync.Mutex
	maxBPS   int64
	tokens   float64
	lastTick time.Time
}

func newRateLimiter(maxBPS int64) *rateLimiter {
	return &rateLimiter{
		maxBPS:   maxBPS,
		tokens:   float64(maxBPS),
		lastTick: time.Now(),
	}
}

func (r *rateLimiter) AllowN(n int) bool {
	if r == nil || r.maxBPS <= 0 {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	elapsed := now.Sub(r.lastTick).Seconds()
	r.tokens += elapsed * float64(r.maxBPS)
	if r.tokens > float64(r.maxBPS) {
		r.tokens = float64(r.maxBPS)
	}
	r.lastTick = now
	if r.tokens >= float64(n) {
		r.tokens -= float64(n)
		return true
	}
	return false
}

type turnServer struct {
	config TurnServerConfig

	state   TurnState
	stateMu sync.RWMutex

	client *turn.Client
	conn   net.Conn

	alloc     tcpAlloc
	relayAddr net.Addr

	createdAt   time.Time
	credCreated time.Time

	rateLimiter *rateLimiter

	txBytes int64
	rxBytes int64

	stunFailCount int
	portFailCount int

	stopCh chan struct{}
}

func newTurnServer(cfg TurnServerConfig) *turnServer {
	return &turnServer{
		config:      cfg,
		state:       TurnDisconnected,
		createdAt:   time.Now(),
		credCreated: time.Now(),
		rateLimiter: newRateLimiter(cfg.MaxBPS),
		stopCh:      make(chan struct{}),
	}
}

// ───── TurnPool ─────

type TurnPool struct {
	mu      sync.Mutex
	servers map[string]*turnServer
	ctx     context.Context
	cancel  context.CancelFunc
}

func NewTurnPool() *TurnPool {
	return &TurnPool{
		servers: make(map[string]*turnServer),
	}
}

func (p *TurnPool) Start(ctx context.Context) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.ctx, p.cancel = context.WithCancel(ctx)
	for _, ts := range p.servers {
		ts.start()
	}
}

func (p *TurnPool) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.cancel != nil {
		p.cancel()
	}
	for _, ts := range p.servers {
		ts.stop()
	}
}

func (p *TurnPool) AddServer(cfg TurnServerConfig) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if _, ok := p.servers[cfg.Addr]; ok {
		return fmt.Errorf("turn server %q already exists", cfg.Addr)
	}
	ts := newTurnServer(cfg)
	p.servers[cfg.Addr] = ts
	if p.ctx != nil {
		ts.start()
	}
	return nil
}

func (p *TurnPool) RemoveServer(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	ts, ok := p.servers[addr]
	if ok {
		ts.stop()
		delete(p.servers, addr)
	}
}

func (p *TurnPool) ListRelayAddrs() []net.Addr {
	p.mu.Lock()
	defer p.mu.Unlock()
	var addrs []net.Addr
	for _, ts := range p.servers {
		if ts.getState() == TurnAlive && ts.relayAddr != nil {
			addrs = append(addrs, ts.relayAddr)
		}
	}
	return addrs
}

func (p *TurnPool) Status(addr string) (TurnState, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	ts, ok := p.servers[addr]
	if !ok {
		return TurnDisconnected, fmt.Errorf("turn server %q not found", addr)
	}
	return ts.getState(), nil
}

func (p *TurnPool) Stats(addr string) *TurnStats {
	p.mu.Lock()
	defer p.mu.Unlock()
	ts, ok := p.servers[addr]
	if !ok {
		return nil
	}
	return &TurnStats{
		State:     ts.getState(),
		Uptime:    time.Since(ts.createdAt),
		RelayAddr: safeString(ts.relayAddr),
		TxBytes:   ts.txBytes,
		RxBytes:   ts.rxBytes,
		FailCount: ts.stunFailCount + ts.portFailCount,
	}
}

func safeString(a net.Addr) string {
	if a == nil {
		return ""
	}
	return a.String()
}

// ───── turnServer lifecycle ─────

func (ts *turnServer) start() {
	go ts.runLoop()
}

func (ts *turnServer) stop() {
	close(ts.stopCh)
}

func (ts *turnServer) runLoop() {
	ts.connect()

	stunTick := time.NewTicker(30 * time.Second)
	defer stunTick.Stop()

	for {
		if ts.getState() != TurnAlive {
			ts.shutdown()
			ts.connectWithBackoff()
			if ts.getState() != TurnAlive {
				return
			}
			continue
		}

		select {
		case <-ts.stopCh:
			ts.shutdown()
			return
		case <-stunTick.C:
			ts.stunCheck()
		}
	}
}

func (ts *turnServer) connect() {
	ts.setState(TurnConnecting)

	done := make(chan struct{}, 1)
	var conn net.Conn
	go func() {
		var err error
		conn, err = net.DialTimeout("tcp", ts.config.Addr, 10*time.Second)
		if err != nil {
			ts.logf("dial fail: %v", err)
			ts.setState(TurnDisconnected)
		}
		done <- struct{}{}
	}()

	select {
	case <-ts.stopCh:
		return
	case <-done:
	}
	if ts.getState() != TurnConnecting {
		return
	}

	ts.logf("enabling TCP keepalive on control connection")
	if tcp, ok := conn.(*net.TCPConn); ok {
		tcp.SetKeepAlive(true)
		tcp.SetKeepAlivePeriod(15 * time.Second)
	}

	ts.setState(TurnAuthenticating)
	stunConn := turn.NewSTUNConn(conn)
	c, err := turn.NewClient(&turn.ClientConfig{
		STUNServerAddr: ts.config.Addr,
		TURNServerAddr: ts.config.Addr,
		Conn:           stunConn,
		Username:       ts.config.Username,
		Password:       ts.config.Password,
		Realm:          ts.config.Realm,
		Software:   "libp2px/888",
	})
	if err != nil {
		ts.setState(TurnDisconnected)
		ts.logf("new client fail: %v", err)
		conn.Close()
		return
	}

	if err := c.Listen(); err != nil {
		ts.setState(TurnDisconnected)
		ts.logf("listen fail: %v", err)
		c.Close()
		conn.Close()
		return
	}

	ts.setState(TurnAllocating)
	alloc, err := c.AllocateTCP()
	if err != nil {
		ts.setState(TurnDisconnected)
		ts.logf("allocate TCP fail: %v", err)
		c.Close()
		conn.Close()
		return
	}

	ts.client = c
	ts.conn = conn
	ts.alloc = alloc
	ts.relayAddr = alloc.Addr()
	ts.portCheck(ts.relayAddr.String())
	ts.stunFailCount = 0
	ts.portFailCount = 0
	ts.setState(TurnAlive)
	ts.logf("test accept: alloc=%s", ts.relayAddr)

	// STUN: 获取外网地址
	mappedAddr, err := c.SendBindingRequest()
	if err != nil {
		ts.logf("SendBindingRequest fail: %v", err)
		mappedAddr = nil
	} else {
		ts.logf("mapped address = %s", mappedAddr)
		alloc.CreatePermissions(mappedAddr)
		ts.logf("CreatePermissions for %s OK", mappedAddr)
	}

	ts.logf("probing relay: Connect to 8.8.8.8:53...")
	_, err = alloc.Connect(&net.TCPAddr{IP: net.IPv4(8, 8, 8, 8), Port: 53})
	if err != nil {
		ts.logf("relay Connect probe failed: %v (server may not support RFC 6062)", err)
	} else {
		ts.logf("relay Connect probe OK")
	}

	// Accept loop（先启动，等 ConnectionAttempt）
	go func() {
		for {
			c, err := alloc.AcceptTCP()
			if err != nil {
				return
			}
			from := c.RemoteAddr()
			ts.logf("incoming from %s (accepted)", from)
			buf := make([]byte, 64)
			n, err := c.Read(buf)
			if err == nil {
				ts.logf("received from %s: %q", from, string(buf[:n]))
				c.Write([]byte("pong"))
			}
			c.Close()
		}
	}()

	// 自测：直连自己的 relay 端口触发 AcceptTCP
	if mappedAddr != nil {
		go func() {
			rawConn, err := net.DialTimeout("tcp", ts.relayAddr.String(), 5*time.Second)
			if err != nil {
				ts.logf("self-test: dial to relay %s fail: %v", ts.relayAddr, err)
				return
			}
			defer rawConn.Close()
			ts.logf("self-test: dial to relay %s OK", ts.relayAddr)

			rawConn.Write([]byte("ping"))
			buf := make([]byte, 64)
			n, _ := rawConn.Read(buf)
			ts.logf("self-test: pong from %s: %q", ts.relayAddr, string(buf[:n]))
			if string(buf[:n]) == "pong" {
				ts.logf("self-test: SUCCESS - relay data path verified")
			} else {
				ts.logf("self-test: FAIL - expected \"pong\", got %q", string(buf[:n]))
			}
		}()
	}
}

func (ts *turnServer) connectWithBackoff() {
	backoff := 1 * time.Second
	maxBackoff := 60 * time.Second

	for {
		ts.connect()
		if ts.getState() == TurnAlive {
			return
		}

		select {
		case <-ts.stopCh:
			return
		case <-time.After(backoff):
		}

		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func (ts *turnServer) shutdown() {
	ts.setState(TurnDisconnected)
	if ts.client != nil {
		ts.client.Close()
		ts.client = nil
	}
	if ts.conn != nil {
		ts.conn.Close()
		ts.conn = nil
	}
	ts.relayAddr = nil
}

// ───── health checks ─────

func (ts *turnServer) stunCheck() {
	if ts.getState() != TurnAlive || ts.client == nil {
		return
	}
	_, err := ts.client.SendBindingRequest()
	if err != nil {
		ts.stunFailCount++
		ts.logf("STUN ping fail (%d/3): %v", ts.stunFailCount, err)
		if ts.stunFailCount >= 3 {
			ts.logf("too many STUN failures, reconnecting")
			ts.setState(TurnDisconnected)
		}
		return
	}
	ts.stunFailCount = 0
}

func (ts *turnServer) portCheck(addr string) {
	if ts.getState() != TurnAlive {
		return
	}
	log.Println("[turn:" + ts.config.Addr + "] port check starting for relay " + addr + "...")
	done := make(chan error, 1)
	go func() {
		conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
		if err == nil {
			conn.Close()
		}
		done <- err
	}()

	select {
	case <-ts.stopCh:
		return
	case err := <-done:
		if err != nil {
			ts.portFailCount++
			log.Println("[turn:" + ts.config.Addr + "] port dial fail (" + fmt.Sprint(ts.portFailCount) + "/3) for relay " + addr + ": " + err.Error())
			if ts.portFailCount >= 3 {
				log.Println("[turn:" + ts.config.Addr + "] too many port failures for relay " + addr + ", reconnecting")
				ts.setState(TurnDisconnected)
			}
			return
		}
	}
	log.Println("[turn:" + ts.config.Addr + "] port check OK for relay " + addr)
	ts.portFailCount = 0
}

// ───── helpers ─────

func (ts *turnServer) setState(s TurnState) {
	ts.stateMu.Lock()
	ts.state = s
	ts.stateMu.Unlock()
}

func (ts *turnServer) getState() TurnState {
	ts.stateMu.RLock()
	defer ts.stateMu.RUnlock()
	return ts.state
}

func (ts *turnServer) logf(format string, v ...interface{}) {
	log.Printf("[turn:%s] %s", ts.config.Addr, fmt.Sprintf(format, v...))
}
