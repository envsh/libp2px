package p2put

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"sync"
	"sync/atomic"
	"time"

	pubsub "github.com/libp2p/go-libp2p-pubsub"
	pb "github.com/libp2p/go-libp2p-pubsub/pb"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
)

const PubSubFwdProtocol = protocol.ID("/d2hub/pubsub/1.0")

var (
	seenMsgIDs   sync.Map // map[string]time.Time
	dedupTTL     = 10 * time.Minute
	dedupMaxSize = 2000
	dedupCount   atomic.Int32
)

func init() {
	MustRegisterProtocol("pubsub/1.0", handlePubSubFwdStream)
}

func isMsgSeen(msgID string) bool {
	_, loaded := seenMsgIDs.LoadOrStore(msgID, time.Now())
	if loaded {
		return true
	}

	cnt := dedupCount.Add(1)

	if cnt >= int32(dedupMaxSize) {
		dedupCount.Store(0)
		seenMsgIDs.Range(func(key, val any) bool {
			if time.Since(val.(time.Time)) > dedupTTL {
				seenMsgIDs.Delete(key)
			} else {
				dedupCount.Add(1)
			}
			return true
		})
	}
	return false
}

// ForwardPubSubToPeer forwards a pubsub message to a specific peer via
// /d2hub/pubsub/1.0. Requirements:
//   - peer is in PeerDB (non-expired)
//   - no direct (non-Limited) connection to peer
//   - peer's protocol cache includes PubSubFwdProtocol
//
// If protocol support is unknown (SupportsProtocols returns error), we
// attempt the stream anyway and let negotiation handle it.
func ForwardPubSubToPeer(pid peer.ID, msg *pubsub.Message) error {
	if bootres.Host == nil {
		return fmt.Errorf("host not ready")
	}

	_, ok := bootres.PeerDB.Get(pid)
	if !ok {
		return fmt.Errorf("peer %s not in PeerDB", pid.ShortString())
	}

	if PeerIsConnected(pid, true) {
		return fmt.Errorf("peer %s has direct connection", pid.ShortString())
	}

	supported, err := bootres.Host.Peerstore().SupportsProtocols(pid, PubSubFwdProtocol)
	if err == nil && len(supported) == 0 {
		return fmt.Errorf("peer %s does not support %s", pid.ShortString(), PubSubFwdProtocol)
	}

	// msg.ID 是 From+Seqno 的二进制拼接，JSON 序列化会被 \uXXXX 膨胀 4-6 倍
	// 清空再 marshal，接收方用 DefaultMsgIdFn 重建，节省带宽
	savedID := msg.ID
	msg.ID = ""
	out, err := json.Marshal(msg)
	msg.ID = savedID
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}

	ctx := network.WithAllowLimitedConn(context.Background(), "pubsub/1.0")
	s, err := bootres.Host.NewStream(ctx, pid, PubSubFwdProtocol)
	if err != nil {
		return fmt.Errorf("newstream: %w", err)
	}
	defer s.Close()

	if _, err := s.Write(out); err != nil {
		s.Reset()
		return fmt.Errorf("write: %w", err)
	}
	s.CloseWrite()
	return nil
}

func handlePubSubFwdStream(s network.Stream) {
	defer s.Close()
	pid := s.Conn().RemotePeer()

	raw, err := io.ReadAll(io.LimitReader(s, 1<<20))
	if err != nil {
		log.Printf("[pubsubfw] read from %s: %v", pid.ShortString(), err)
		s.Reset()
		return
	}

	msg := &pubsub.Message{Message: &pb.Message{}}
	if err := json.Unmarshal(raw, msg); err != nil {
		log.Printf("[pubsubfw] unmarshal from %s: %v", pid.ShortString(), err)
		return
	}
	// 发送方已清空 msg.ID（免得 JSON 膨胀），用 From+Seqno 重建
	msg.ID = pubsub.DefaultMsgIdFn(msg.Message)
	msg.ReceivedFrom = pid

	if isMsgSeen(msg.ID) {
		log.Printf("[pubsubfw] dup %s from %s", msg.ID, pid.ShortString())
		return
	}

	log.Printf("[pubsubfw] fwd %s topic=%s peer=%s", msg.ID, *msg.Topic, pid.ShortString())
	rawChan <- msg
}
