package pbecho

import (
	"context"
	"io"
	"log"

	"github.com/envsh/libp2px/p2put"
	"github.com/libp2p/go-libp2p/core/network"
)

const echoProto = "echo/1.0"

func init() {
	p2put.MustRegisterProtocol(echoProto, func(s network.Stream) {
		defer s.Close()
		log.Printf("[pbecho] from %s", s.Conn().RemotePeer().ShortString())
		if _, err := io.WriteString(s, "Re: "); err != nil {
			log.Printf("[pbecho] write prefix error: %v", err)
			return
		}
		if _, err := io.Copy(s, s); err != nil {
			log.Printf("[pbecho] copy error: %v", err)
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
