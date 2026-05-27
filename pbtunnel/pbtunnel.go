package pbtunnel

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/envsh/libp2px/p2put"
	"github.com/libp2p/go-libp2p/core/network"
)

var ShouldReject func(network.Stream) bool

const tunnelProto = "tunnel/1.0"

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

func targetAddr() string {
	if tunnelTarget != "" {
		return tunnelTarget
	}
	return fmt.Sprintf("127.0.0.1:%d", tunnelPort)
}

func init() {
	p2put.MustRegisterProtocol(tunnelProto, handleTunnel)
}

func handleTunnel(s network.Stream) {
	seq := atomic.AddInt64(&Stats.ConnSeq, 1)
	start := time.Now()

	if ShouldReject != nil && ShouldReject(s) {
		s.Reset()
		return
	}

	addr := targetAddr()
	if addr == "" {
		return
	}

	conn, err := net.DialTimeout("tcp", addr, 30*time.Second)
	if err != nil {
		log.Printf("[pbtunnel] dial %s: %v", addr, err)
		return
	}

	var closeOnce sync.Once
	closeStream := func() { closeOnce.Do(func() { s.Close() }) }
	defer closeStream()
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)

	var localSent, localRecv int64

	go func() {
		defer wg.Done()
		defer cancel()
		buf := make([]byte, 32*1024)
		for {
			conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
			n, rerr := conn.Read(buf)
			if n > 0 {
				s.Write(buf[:n])
				atomic.AddInt64(&Stats.BytesSent, int64(n))
				localSent += int64(n)
			}
			if rerr != nil {
				if ne, ok := rerr.(net.Error); ok && ne.Timeout() {
					if ctx.Err() != nil {
						return
					}
					continue
				}
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		defer cancel()
		buf := make([]byte, 32*1024)
		for {
			n, rerr := s.Read(buf)
			if n > 0 {
				conn.Write(buf[:n])
				atomic.AddInt64(&Stats.BytesRecv, int64(n))
				localRecv += int64(n)
			}
			if rerr != nil {
				return
			}
		}
	}()

	go func() {
		<-ctx.Done()
		closeStream()
	}()

	wg.Wait()
	dur := time.Since(start)
	atomic.AddInt64(&Stats.Dur, dur.Nanoseconds())
	log.Printf("[pbtunnel] conn=%d closed: sent=%d recv=%d dur=%s", seq, localSent, localRecv, dur.Round(time.Millisecond))
}

func Dial(peerID string, ctx ...context.Context) (network.Stream, error) {
	var c context.Context
	if len(ctx) > 0 {
		c = ctx[0]
	} else {
		c = context.Background()
	}
	return p2put.OpenStream(c, peerID, tunnelProto)
}
