package pbecho

import (
	"bytes"
	"context"
	"io"
	"log"
	"time"

	"github.com/envsh/libp2px/p2put"
	"github.com/libp2p/go-libp2p/core/network"
)

var ShouldReject func(network.Stream) bool

const (
	echoProto   = "echo/1.0"
	maxEchoLen  = 64 * 1024
	readTimeout = 10 * time.Second
)

func init() {
	p2put.MustRegisterProtocol(echoProto, func(s network.Stream) {
		defer s.Close()
		defer func() { recover() }()
		log.Printf("[pbecho] from %s", s.Conn().RemotePeer().ShortString())

		if ShouldReject != nil && ShouldReject(s) {
			s.Reset()
			return
		}

		var buf bytes.Buffer
		readCh := make(chan error, 1)
		go func() {
			_, err := io.Copy(&buf, io.LimitReader(s, maxEchoLen))
			readCh <- err
		}()
		select {
		case err := <-readCh:
			if err != nil {
				log.Printf("[pbecho] read error: %v", err)
				return
			}
		case <-time.After(readTimeout):
			log.Printf("[pbecho] read timeout")
			return
		}

		if _, err := io.WriteString(s, "Re: "); err != nil {
			log.Printf("[pbecho] write prefix error: %v", err)
			return
		}
		if _, err := s.Write(buf.Bytes()); err != nil {
			log.Printf("[pbecho] write echo error: %v", err)
		}
	})
}

func Echo(peerID, msg string, ctx ...context.Context) (string, error) {
	var c context.Context
	if len(ctx) > 0 {
		c = ctx[0]
	} else {
		c = context.Background()
	}
	s, err := p2put.OpenStream(c, peerID, echoProto)
	if err != nil {
		return "", err
	}
	defer s.Close()
	if _, err := s.Write([]byte(msg)); err != nil {
		return "", err
	}
	if sc, ok := s.(interface{ CloseWrite() error }); ok {
		sc.CloseWrite()
	}
	buf, err := io.ReadAll(s)
	if err != nil {
		return "", err
	}
	return string(buf), nil
}
