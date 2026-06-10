package p2put

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/pion/transport/v2"
	"github.com/pion/turn/v2"
)

type TurnPeerStats struct {
	TargetAddr string
	MappedAddr string
	FailCount  int
	LastPing   time.Time
	LastPong   time.Time
	TxBytes    int64
	RxBytes    int64
	Alive      bool
}

// TurnPeer simulates a complete peer on the connect side.
// It maintains two persistent connections:
//   - TURN server:  STUN BindingRequest every 30s (keepalive + mapped addr refresh)
//   - Relay:        "ping" every 19s, expects "pong" (keepalive + health check)
type TurnPeer struct {
	serverAddr string
	targetAddr net.Addr
	mappedAddr *net.UDPAddr

	username string
	password string
	realm    string

	// persistent TURN connection (STUN keepalive)
	turnConn   net.Conn
	turnClient *turn.Client
	turnAlloc  transport.TCPListener

	// persistent relay connection (ping/pong keepalive)
	relayConn  net.Conn
	relayMu    sync.Mutex

	mu        sync.Mutex
	failCount int
	lastPing  time.Time
	lastPong  time.Time
	txBytes   int64
	rxBytes   int64

	stopCh chan struct{}
	wg     sync.WaitGroup
}

func NewTurnPeer(serverAddr string, targetAddr net.Addr, username, password, realm string) *TurnPeer {
	return &TurnPeer{
		serverAddr: serverAddr,
		targetAddr: targetAddr,
		username:   username,
		password:   password,
		realm:      realm,
		stopCh:     make(chan struct{}),
	}
}

// ExchangeMapped establishes a persistent TCP connection to the TURN server,
// creates a STUN client, performs BindingRequest, stores the mapped address
// in the global ConnectMappedAddr, and keeps the connection alive for future
// STUN keepalive.
func (tp *TurnPeer) ExchangeMapped() (*net.UDPAddr, error) {
	conn, err := net.DialTimeout("tcp", tp.serverAddr, 10*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial TURN server: %w", err)
	}
	if tcp, ok := conn.(*net.TCPConn); ok {
		tcp.SetKeepAlive(true)
		tcp.SetKeepAlivePeriod(15 * time.Second)
	}

	stunConn := turn.NewSTUNConn(conn)
	c, err := turn.NewClient(&turn.ClientConfig{
		STUNServerAddr: tp.serverAddr,
		TURNServerAddr: tp.serverAddr,
		Conn:           stunConn,
		Username:       tp.username,
		Password:       tp.password,
		Realm:          tp.realm,
		Software:       "libp2px/888",
	})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("create STUN client: %w", err)
	}

	if err := c.Listen(); err != nil {
		c.Close()
		conn.Close()
		return nil, fmt.Errorf("STUN listen: %w", err)
	}

	mappedAddr, err := c.SendBindingRequest()
	if err != nil {
		c.Close()
		conn.Close()
		return nil, fmt.Errorf("STUN BindingRequest: %w", err)
	}

	udpAddr, ok := mappedAddr.(*net.UDPAddr)
	if !ok {
		c.Close()
		conn.Close()
		return nil, fmt.Errorf("expected *net.UDPAddr, got %T", mappedAddr)
	}

	alloc, err := c.AllocateTCP()
	if err != nil {
		c.Close()
		conn.Close()
		return nil, fmt.Errorf("AllocateTCP: %w", err)
	}
	log.Println("peer alloc", alloc.Addr().String())

	tp.turnConn = conn
	tp.turnClient = c
	tp.turnAlloc = alloc

	tp.mu.Lock()
	tp.mappedAddr = udpAddr
	tp.mu.Unlock()

	ConnectMappedAddr = udpAddr
	log.Printf("turnpeer: mapped address = %s\n", udpAddr)
	return udpAddr, nil
}

func (tp *TurnPeer) Start() {
	tp.wg.Add(1)
	go tp.runLoop()
}

func (tp *TurnPeer) Stop() {
	close(tp.stopCh)
	tp.wg.Wait()
}

// closeRelay closes the persistent relay connection.
func (tp *TurnPeer) closeRelay() {
	tp.relayMu.Lock()
	if tp.relayConn != nil {
		tp.relayConn.Close()
		tp.relayConn = nil
	}
	tp.relayMu.Unlock()
}

// closeTurn closes the persistent TURN connection.
func (tp *TurnPeer) closeTurn() {
	if tp.turnAlloc != nil {
		tp.turnAlloc.Close()
		tp.turnAlloc = nil
	}
	if tp.turnClient != nil {
		tp.turnClient.Close()
		tp.turnClient = nil
	}
	if tp.turnConn != nil {
		tp.turnConn.Close()
		tp.turnConn = nil
	}
}

func (tp *TurnPeer) runLoop() {
	defer tp.wg.Done()
	defer tp.closeRelay()
	defer tp.closeTurn()

	time.Sleep(4 * time.Second)
	tp.relayPing()

	stunTick := time.NewTicker(9 * time.Second)
	defer stunTick.Stop()
	pingTick := time.NewTicker(19 * time.Second)
	defer pingTick.Stop()

	for {
		select {
		case <-tp.stopCh:
			return
		case <-stunTick.C:
			tp.stunCtrlKeepMappedPort()
		case <-pingTick.C:
			tp.relayPing()
		}
	}
}

// SendBindingRequest是无状态的匿名请求，达到空闲超时后可以直接回收，对保活没有任何用
// stunKeepalive sends a STUN BindingRequest on the persistent TURN connection.
func (tp *TurnPeer) stunCtrlKeepMappedPort() {
	if tp.turnClient == nil {
		return
	}
	mappedAddr, err := tp.turnClient.SendBindingRequest()
	if err != nil {
		log.Printf("turnpeer: STUN keepalive fail: %v\n", err)
		// tp.closeTurn()
	}else{
		// check with old mappedAddr
		if mappedAddr.String() != ConnectMappedAddr.String() {
			log.Println("mapped change", ConnectMappedAddr.String(), mappedAddr.String())
		}
	}
}

// relayPing sends "ping" on the persistent relay connection and expects "pong".
// If the connection is dead, it tears it down; the next cycle will reconnect.
func (tp *TurnPeer) relayPing() {
	tp.relayMu.Lock()
	conn := tp.relayConn
	tp.relayMu.Unlock()

	if conn == nil {
		if err := tp.dialRelay(); err != nil {
			tp.mu.Lock()
			tp.failCount++
			tp.mu.Unlock()
			return
		}
		conn = tp.relayConn
	}

	tp.mu.Lock()
	tp.lastPing = time.Now()
	tp.mu.Unlock()

	if err := conn.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		log.Printf("turnpeer: set deadline error: %v\n", err)
		tp.closeRelay()
		return
	}

	n, err := conn.Write([]byte("ping"))
	if err != nil {
		log.Printf("turnpeer: write fail: %v\n", err)
		tp.closeRelay()
		return
	}
	tp.mu.Lock()
	tp.txBytes += int64(n)
	tp.mu.Unlock()

	buf := make([]byte, 64)
	n, err = conn.Read(buf)
	if err != nil {
		log.Printf("turnpeer: read fail: %v\n", err)
		tp.closeRelay()
		return
	}
	tp.mu.Lock()
	tp.rxBytes += int64(n)
	tp.mu.Unlock()

	if string(buf[:n]) != "pong" {
		log.Printf("turnpeer: unexpected response: %q\n", string(buf[:n]))
		tp.closeRelay()
		return
	}

	tp.mu.Lock()
	tp.failCount = 0
	tp.lastPong = time.Now()
	tp.mu.Unlock()

	log.Printf("turnpeer: ping OK via %s\n", tp.targetAddr)
}

// dialRelay establishes the persistent TCP connection to the relay address.
func (tp *TurnPeer) dialRelay() error {
	tp.relayMu.Lock()
	defer tp.relayMu.Unlock()

	if tp.relayConn != nil {
		return nil
	}

	conn, err := net.DialTimeout("tcp", tp.targetAddr.String(), 5*time.Second)
	if err != nil {
		return fmt.Errorf("dial relay: %w", err)
	}
	tp.relayConn = conn
	return nil
}

func (tp *TurnPeer) Stats() TurnPeerStats {
	tp.mu.Lock()
	defer tp.mu.Unlock()
	mappedStr := ""
	if tp.mappedAddr != nil {
		mappedStr = tp.mappedAddr.String()
	}
	return TurnPeerStats{
		TargetAddr: tp.targetAddr.String(),
		MappedAddr: mappedStr,
		FailCount:  tp.failCount,
		LastPing:   tp.lastPing,
		LastPong:   tp.lastPong,
		TxBytes:    tp.txBytes,
		RxBytes:    tp.rxBytes,
		Alive:      tp.failCount == 0,
	}
}
