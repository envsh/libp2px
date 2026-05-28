// Package pbtunnel provides an HTTP/HTTPS forward proxy server.
//
// Example:
//
//	proxy := pbtunnel.NewHTTPProxy()
//	go proxy.ListenAndServe(":9229")   // HTTP + CONNECT
//	defer proxy.Close()
package pbtunnel

import (
	"io"
	"log"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

var proxyConnSeq int64

type HTTPProxy struct {
	server    *http.Server
	transport *http.Transport
	mu        sync.Mutex
}

func NewHTTPProxy() *HTTPProxy {
	return &HTTPProxy{
		transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:    100,
			IdleConnTimeout: 90 * time.Second,
		},
	}
}

func (p *HTTPProxy) ListenAndServe(addr string) error {
	if addr == "" {
		addr = ":9229"
	}
	s := &http.Server{Addr: addr, Handler: p}
	p.mu.Lock()
	p.server = s
	p.mu.Unlock()
	return s.ListenAndServe()
}

func (p *HTTPProxy) Close() error {
	p.mu.Lock()
	s := p.server
	p.mu.Unlock()
	if s != nil {
		return s.Close()
	}
	return nil
}

func (p *HTTPProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method == "CONNECT" {
		p.serveConnect(w, r)
	} else {
		p.serveHTTPProxy(w, r)
	}
}

func (p *HTTPProxy) serveConnect(w http.ResponseWriter, r *http.Request) {
	seq := atomic.AddInt64(&proxyConnSeq, 1)
	start := time.Now()
	target := r.Host

	targetConn, err := net.DialTimeout("tcp", target, 30*time.Second)
	if err != nil {
		log.Printf("[pbtunnel] proxy conn=%d dial %s: %v", seq, target, err)
		http.Error(w, "Bad Gateway", http.StatusBadGateway)
		return
	}

	hj, ok := w.(http.Hijacker)
	if !ok {
		targetConn.Close()
		http.Error(w, "Hijacking not supported", http.StatusInternalServerError)
		return
	}
	clientConn, _, err := hj.Hijack()
	if err != nil {
		targetConn.Close()
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	clientConn.Write([]byte("HTTP/1.1 200 Connection Established\r\n\r\n"))

	var closeClientOnce sync.Once
	closeClient := func() { closeClientOnce.Do(func() { clientConn.Close() }) }
	var closeTargetOnce sync.Once
	closeTarget := func() { closeTargetOnce.Do(func() { targetConn.Close() }) }
	defer closeClient()
	defer closeTarget()

	log.Printf("[pbtunnel] proxy conn=%d newed: target=%s", seq, target)

	var wg sync.WaitGroup
	wg.Add(2)
	var localSent, localRecv int64

	go func() {
		defer log.Println("xfer proxy <- target", seq)
		defer wg.Done()
		defer closeTarget()
		buf := make([]byte, bufSize)
		for {
			clientConn.SetReadDeadline(time.Now().Add(5 * time.Minute))
			n, rerr := clientConn.Read(buf)
			if n > 0 {
				targetConn.Write(buf[:n])
				localSent += int64(n)
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
		defer log.Println("xfer proxy -> target", seq)
		defer wg.Done()
		defer closeClient()
		buf := make([]byte, bufSize)
		for {
			targetConn.SetReadDeadline(time.Now().Add(5 * time.Minute))
			n, rerr := targetConn.Read(buf)
			if n > 0 {
				clientConn.Write(buf[:n])
				localRecv += int64(n)
			}
			if rerr != nil {
				if ne, ok := rerr.(net.Error); ok && ne.Timeout() {
					continue
				}
				return
			}
		}
	}()

	log.Println("wg.Wait() ...", seq)
	wg.Wait()
	dur := time.Since(start)
	log.Printf("[pbtunnel] proxy conn=%d closed: target=%s sent=%d recv=%d dur=%s",
		seq, target, localSent, localRecv, dur.Round(time.Millisecond))
}

func (p *HTTPProxy) serveHTTPProxy(w http.ResponseWriter, r *http.Request) {
	seq := atomic.AddInt64(&proxyConnSeq, 1)
	start := time.Now()

	outReq, err := http.NewRequest(r.Method, r.URL.String(), r.Body)
	if err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	for k, vv := range r.Header {
		for _, v := range vv {
			outReq.Header.Add(k, v)
		}
	}
	outReq.Header.Del("Proxy-Connection")

	resp, err := p.transport.RoundTrip(outReq)
	if err != nil {
		log.Printf("[pbtunnel] proxy req=%d %s %s: %v", seq, r.Method, r.Host, err)
		http.Error(w, "Bad Gateway", http.StatusBadGateway)
		return
	}
	defer resp.Body.Close()

	for k, vv := range resp.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(resp.StatusCode)
	n, _ := io.Copy(w, resp.Body)

	dur := time.Since(start)
	log.Printf("[pbtunnel] proxy req=%d %s %s: status=%d size=%d dur=%s",
		seq, r.Method, r.Host, resp.StatusCode, n, dur.Round(time.Millisecond))
}
