package p2put

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/pion/logging"
	"github.com/pion/transport/v2"
	"github.com/pion/turn/v2"
)

var turnPool = NewTurnPool()

var (
	AcceptMappedAddr  *net.UDPAddr
	ConnectMappedAddr *net.UDPAddr
)

type TurnAuthInfo struct {

}

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

	alloc     transport.TCPListener
	relayAddr net.Addr

	createdAt   time.Time
	credCreated time.Time

	rateLimiter *rateLimiter

	stunFailCount int
	portFailCount int

	peer *TurnPeer

	stopCh       chan struct{}
	acceptStopCh chan struct{}
	reconnectNow chan struct{}

	resourceMu sync.Mutex
}

func newTurnServer(cfg TurnServerConfig) *turnServer {
	return &turnServer{
		config:       cfg,
		state:        TurnDisconnected,
		createdAt:    time.Now(),
		credCreated:  time.Now(),
		rateLimiter:  newRateLimiter(cfg.MaxBPS),
		stopCh:       make(chan struct{}),
		acceptStopCh: make(chan struct{}),
		reconnectNow: make(chan struct{}),
	}
}

// ───── TurnPool ─────

type TurnPool struct {
	mu      sync.Mutex
	servers map[string]*turnServer
	ctx     context.Context
	cancel  context.CancelFunc
	permips map[string]int // ip => port? // 可以外部追加
}

func NewTurnPool() *TurnPool {
	tp := &TurnPool{
		servers: make(map[string]*turnServer),
		permips: make(map[string]int),
	}
	// tp.AddPermIP("177.42.48.118")
	tp.AddPermIP("77.42.48.118")
	return tp
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
		if ts.getState() != TurnAlive {
			continue
		}
		ts.resourceMu.Lock()
		ra := ts.relayAddr
		ts.resourceMu.Unlock()
		if ra != nil {
			addrs = append(addrs, ra)
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
	ts.resourceMu.Lock()
	ra := ts.relayAddr
	ts.resourceMu.Unlock()
	return &TurnStats{
		State:     ts.getState(),
		Uptime:    time.Since(ts.createdAt),
		RelayAddr: safeString(ra),
		FailCount: ts.stunFailCount + ts.portFailCount,
	}
}

func (p *TurnPool) AddPermIP(ip string) {
	if ip == "" { return }
	if strings.Contains(ip, ":") {
		ip = strings.Split(ip, ":")[0]
	}
	if strings.Count(ip, ".") != 3 { return }
	p.permips[ip] = 1
	log.Println("added permip", ip)
}
func (p *TurnPool) RemovePermIP(ip string) {
	if ip == "" { return }
	if strings.Contains(ip, ":") {
		ip = strings.Split(ip, ":")[0]
	}
	if strings.Count(ip, ".") != 3 { return }
	delete(p.permips, ip)
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
	if err := ts.connect(); err != nil {
		log.Printf("runLoop: initial connect failed: %v\n", err)
	}

	stunTick := time.NewTicker(30 * time.Second)
	defer stunTick.Stop()

	for {
		if ts.getState() != TurnAlive {
			ts.shutdown()
			select {
			case <-ts.stopCh:
				return
			default:
			}
			ts.connectWithBackoff()
			continue
		}

		select {
		case <-ts.stopCh:
			ts.shutdown()
			return
		case <-ts.reconnectNow:
			ts.shutdown()
			continue
		case <-stunTick.C:
			ts.stunCtrlKeepMappedPort()
		}
	}
}

func (ts *turnServer) connect() error {
	ts.acceptStopCh = make(chan struct{})
	ts.reconnectNow = make(chan struct{})
	ts.setState(TurnConnecting)

	done := make(chan struct{}, 1)
	var (
		conn    net.Conn
		dialErr error
	)
	go func() {
		var tcpAddr *net.TCPAddr
		tcpAddr, dialErr = net.ResolveTCPAddr("tcp4", ts.config.Addr)
		if dialErr == nil {
			conn, dialErr = net.DialTimeout("tcp", tcpAddr.String(), 10*time.Second)
		}
		done <- struct{}{}
	}()

	select {
	case <-ts.stopCh:
		return nil
	case <-done:
	}
	if dialErr != nil {
		ts.setState(TurnDisconnected)
		return fmt.Errorf("dial fail: %w", dialErr)
	}

	log.Printf("enabling TCP keepalive on control connection\n")
	if tcp, ok := conn.(*net.TCPConn); ok {
		tcp.SetKeepAlive(true)
		tcp.SetKeepAlivePeriod(15 * time.Second)
	}

	ts.setState(TurnAuthenticating)
	stunConn := turn.NewSTUNConn(conn)
	loggerFactory := logging.NewDefaultLoggerFactory()
	loggerFactory.ScopeLevels["turnc"] = logging.LogLevelDebug
	c, err := turn.NewClient(&turn.ClientConfig{
		STUNServerAddr: ts.config.Addr,
		TURNServerAddr: ts.config.Addr,
		Conn:           stunConn,
		Username:       ts.config.Username,
		Password:       ts.config.Password,
		Realm:          ts.config.Realm,
		Software:       "libp2px/888",
		// LoggerFactory:  loggerFactory,
	})
	if err != nil {
		ts.setState(TurnDisconnected)
		conn.Close()
		return fmt.Errorf("new client fail: %w", err)
	}

	if err := c.Listen(); err != nil {
		ts.setState(TurnDisconnected)
		c.Close()
		conn.Close()
		return fmt.Errorf("listen fail: %w", err)
	}

	ts.setState(TurnAllocating)
	alloc, err := c.AllocateTCP()
	if err != nil {
		ts.setState(TurnDisconnected)
		c.Close()
		conn.Close()
		return fmt.Errorf("allocate TCP fail: %w", err)
	}

	ts.resourceMu.Lock()
	ts.client = c
	ts.conn = conn
	ts.alloc = alloc
	ts.relayAddr = alloc.Addr()
	ts.resourceMu.Unlock()

	go ts.acceptProc()
	ts.stunFailCount = 0
	ts.portFailCount = 0
	ts.setState(TurnAlive)
	log.Printf("test accept: alloc=%s\n", ts.relayAddr)

	mappedAddr, err := c.SendBindingRequest()
	if err != nil {
		ts.setState(TurnDisconnected)
		c.Close()
		conn.Close()
		alloc.Close()
		ts.resourceMu.Lock()
		ts.client = nil
		ts.conn = nil
		ts.alloc = nil
		ts.relayAddr = nil
		ts.resourceMu.Unlock()
		return fmt.Errorf("SendBindingRequest fail: %w", err)
	}
	log.Printf("mapped address = %s\n", mappedAddr)
	AcceptMappedAddr = mappedAddr.(*net.UDPAddr)
	if err := alloc.CreatePermissions(mappedAddr); err != nil {
		log.Printf("CreatePermissions for %s FAIL: %v\n", mappedAddr, err)
	} else {
		log.Printf("CreatePermissions for %s OK\n", mappedAddr)
	}

	localIP := conn.LocalAddr().(*net.TCPAddr).IP
	if err := c.CreatePermission(&net.UDPAddr{IP: localIP}); err != nil {
		log.Printf("create permission for local IP: %v\n", err)
	}
	turnPool.AddPermIP(AcceptMappedAddr.String())
	turnPool.AddPermIP(conn.LocalAddr().String())

	ts.peer = NewTurnPeer(ts.config.Addr, ts.relayAddr,
		ts.config.Username, ts.config.Password, ts.config.Realm)
	if maddr, err := ts.peer.ExchangeMapped(); err != nil {
		log.Printf("turnpeer ExchangeMapped failed: %v\n", err)
	} else {
		log.Printf("turnpeer mapped address = %s\n", maddr)
		turnPool.AddPermIP(maddr.String())
		if err := alloc.CreatePermissions(maddr); err != nil {
			log.Printf("CreatePermissions for turnpeer %s FAIL: %v\n", maddr, err)
		}
	}
	ts.peer.Start()

	log.Printf("probing relay: Connect to 8.8.8.8:53...\n")
	if err := alloc.CreatePermissions(&net.UDPAddr{IP: net.IPv4(8, 8, 8, 8)}); err != nil {
		log.Printf("CreatePermissions for 8.8.8.8 fail: %v", err)
	}
	cid, err := alloc.Connect(&net.TCPAddr{IP: net.IPv4(8, 8, 8, 8), Port: 53})
	if err != nil {
		log.Printf("relay Connect probe UDP outbound failed: %v\n", err)
	} else {
		log.Printf("relay Connect probe UDP outbound OK %v\n", cid)
		// err = alloc.BindConnection(relayConn, cid)    // ③ 把 TCP 绑定到该连接 ID
	}
	
	{ // google
		ttaddr := &net.TCPAddr{IP: net.IPv4(185, 45, 5, 35), Port: 443}
		cid, err := alloc.Connect(ttaddr)
		log.Println("probe turn TCP outbound", cid, err)
	}

	alloc.CreatePermissions(turnPool.permAddresses()...)
	c.CreatePermission(turnPool.permAddresses()...)

	return nil
}

func (tp *TurnPool) permAddresses() []net.Addr {
	addrs := []net.Addr{}
	for ip, _ := range tp.permips {
		o := net.ParseIP(ip)
		addrs=append(addrs, &net.TCPAddr{IP: o})
	}
	return addrs
}

// reallocate 只重新分配 TCP relay，保留控制连接 (conn + client)
func (ts *turnServer) reallocate() error {
	ts.resourceMu.Lock()
	if ts.client == nil {
		ts.resourceMu.Unlock()
		return fmt.Errorf("reallocate: no client")
	}
	if ts.alloc != nil {
		ts.alloc.Close()
	}
	client := ts.client
	ts.resourceMu.Unlock()

	ts.setState(TurnAllocating)

	alloc, err := client.AllocateTCP()
	if err != nil {
		ts.setState(TurnDisconnected)
		ts.resourceMu.Lock()
		ts.alloc = nil
		ts.relayAddr = nil
		ts.resourceMu.Unlock()
		close(ts.reconnectNow)
		return fmt.Errorf("reallocate AllocateTCP fail: %w", err)
	}

	select {
	case <-ts.acceptStopCh:
	default:
		close(ts.acceptStopCh)
	}

	ts.acceptStopCh = make(chan struct{})

	ts.resourceMu.Lock()
	ts.alloc = alloc
	ts.relayAddr = alloc.Addr()
	ts.resourceMu.Unlock()

	go ts.acceptProc()

	ts.stunFailCount = 0
	ts.portFailCount = 0

	mappedAddr, err := client.SendBindingRequest()
	if err != nil {
		ts.setState(TurnDisconnected)
		return fmt.Errorf("reallocate SendBindingRequest fail: %w", err)
	}
	AcceptMappedAddr = mappedAddr.(*net.UDPAddr)
	_ = alloc.CreatePermissions(mappedAddr)

	localIP := ts.conn.LocalAddr().(*net.TCPAddr).IP
	client.CreatePermission(&net.UDPAddr{IP: localIP})

	if ts.peer != nil {
		ts.peer.Stop()
	}
	ts.peer = NewTurnPeer(ts.config.Addr, ts.relayAddr,
		ts.config.Username, ts.config.Password, ts.config.Realm)
	if maddr, err := ts.peer.ExchangeMapped(); err != nil {
		log.Printf("turnpeer ExchangeMapped failed: %v\n", err)
	} else {
		alloc.CreatePermissions(maddr)
	}
	ts.peer.Start()

	log.Printf("reallocate OK: new relay %s\n", ts.relayAddr)
	turnPool.AddPermIP(mappedAddr.String())
	turnPool.AddPermIP(ts.conn.LocalAddr().String())
	alloc.CreatePermissions(turnPool.permAddresses()...)
	client.CreatePermission(turnPool.permAddresses()...)

	ts.setState(TurnAlive)
	return nil
}

func (ts *turnServer) connectWithBackoff() {
	backoff := 1 * time.Second
	maxBackoff := 60 * time.Second

	for {
		if err := ts.connect(); err != nil {
			log.Printf("connectWithBackoff: %v\n", err)
		}
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
	log.Printf("shutdown: starting\n")
	ts.setState(TurnDisconnected)

	if ts.peer != nil {
		ts.peer.Stop()
		ts.peer = nil
	}

	select {
	case <-ts.acceptStopCh:
	default:
		close(ts.acceptStopCh)
	}

	ts.resourceMu.Lock()
	if ts.alloc != nil {
		log.Printf("shutdown: closing alloc\n")
		ts.alloc.Close()
	}
	if ts.client != nil {
		log.Printf("shutdown: closing client\n")
		ts.client.Close()
	}
	if ts.conn != nil {
		log.Printf("shutdown: closing conn\n")
		ts.conn.Close()
	}
	ts.client = nil
	ts.conn = nil
	ts.alloc = nil
	ts.relayAddr = nil
	ts.resourceMu.Unlock()
	log.Printf("shutdown: done\n")
}

// ───── health checks ─────

// SendBindingRequest是无状态的匿名请求，达到空闲超时后可以直接回收，对保活没有任何用
func (ts *turnServer) stunCtrlKeepMappedPort() {
	ts.resourceMu.Lock()
	client := ts.client
	ts.resourceMu.Unlock()
	if ts.getState() != TurnAlive || client == nil {
		// return
	}
	mappedAddr, err := client.SendBindingRequest()
	if err != nil {
		ts.stunFailCount++
		log.Printf("STUN ping fail (%d/3): %v\n", ts.stunFailCount, err)
		if ts.stunFailCount >= 2 {
			log.Printf("too many STUN failures, reconnecting\n")
			ts.setState(TurnDisconnected)
		}
		return
	}
	// check with old mappedAddr
	if mappedAddr.String() != AcceptMappedAddr.String() {
		log.Println("mapped change", AcceptMappedAddr.String(), mappedAddr.String())
	}
	ts.client.CreatePermission(mappedAddr)
	ts.client.CreatePermission(turnPool.permAddresses()...)
	ts.stunFailCount = 0
}



func (ts *turnServer) handleAccept(c transport.TCPConn) {
	from := c.RemoteAddr()
	log.Printf("incoming from %s (accepted)\n", from)
	defer c.Close()

	for {
		if err := c.SetReadDeadline(time.Now().Add(30 * time.Second)); err != nil {
			log.Printf("set read deadline error: %v\n", err)
			return
		}
		buf := make([]byte, 64)
		n, err := c.Read(buf)
		if err != nil {
			log.Printf("read from %s error: %v\n", from, err)
			return
		}
		log.Printf("received from %s: %q\n", from, string(buf[:n]))
		if string(buf[:n]) != "ping" {
			log.Printf("unexpected message from %s: %q\n", from, string(buf[:n]))
			return
		}
		if _, err := c.Write([]byte("pong")); err != nil {
			log.Printf("write to %s error: %v\n", from, err)
			return
		}
	}
}

func (ts *turnServer) isAcceptFatal(err error) bool {
	return strings.Contains(err.Error(), "failed to bind connection")
}

func (ts *turnServer) acceptProc() {
	defer log.Println("acceptProc() exited", ts.relayAddr)
	lastReason := ""
	_ = lastReason
	for {
		select {
		case <-ts.acceptStopCh:
			return
		default:
		}
		ts.resourceMu.Lock()
		alloc := ts.alloc
		ts.resourceMu.Unlock()
		if alloc == nil {
			return
		}
		//log.Println("accepting...", ts.relayAddr, lastReason)
		alloc.SetDeadline(time.Now().Add(10 * time.Second))
		c, err := alloc.AcceptTCP()
		//log.Println("acceptd...", ts.relayAddr)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				lastReason = "deadlined"
				continue  // 正常超时，下一轮循环
			}
			select {
			case <-ts.acceptStopCh:
				return
			default:
			}
			if ts.isAcceptFatal(err) {
				select {
				case <-ts.acceptStopCh:
					return
				default:
				}
				log.Printf("accept fatal error: %v, reconnecting\n", err)
				ts.setState(TurnDisconnected)
				close(ts.reconnectNow)
				return
			}
			// 端口还在，但不接收新连接
			if strings.Contains(err.Error(), "connection reset by peer") {
				log.Printf("accept fatal error: %v, reconnecting\n", err)
				err = ts.reallocate()
				if err != nil {
					return
				}
				return
			}
			log.Printf("accept error: %v\n", err)
			time.Sleep(time.Second)
			continue
		}
		lastReason = ""
		go ts.handleAccept(c)
	}
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
