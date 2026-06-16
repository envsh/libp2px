package p2put

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
)

const (
	frameTypeInit   byte = 0
	frameTypeAccept byte = 1
	frameTypeReject byte = 2
	frameTypeData   byte = 3
	frameTypeAck    byte = 4
	frameTypeFin    byte = 5
	frameTypeDone   byte = 6
	frameTypeError  byte = 7
	frameTypeCancel byte = 8
)

const chunkSize = 65536
const ackBatch = 8
const dataTimeout = 30 * time.Second

func init() {
	MustRegisterProtocol("file/1.0", HandleFileStream)
}

type FileProgress struct {
	SessionID   string
	PeerID      peer.ID
	Name        string
	TotalBytes  int64
	DoneBytes   int64
	TotalChunks int
	DoneChunks  int
	State       string // init|transfer|checksum|done|error|cancelled
	Err         error
}

type ProgressFunc func(FileProgress)

type fileSession struct {
	mu          sync.Mutex
	id          string
	peer        peer.ID
	filename    string
	savePath    string
	size        int64
	chunkSize   int
	totalChunks int
	checksum    string
	state       string

	file   *os.File
	tmpFile *os.File
	bitmap  []uint64
	tmpPath string
	doneBytes int64

	cancel context.CancelFunc
}

var (
	sessions   sync.Map
	recvDir    string
	recvDirMu  sync.RWMutex
	recvCB     map[peer.ID]ProgressFunc
	recvCBMu   sync.Mutex
)

func SetFileRecvDir(dir string) {
	recvDirMu.Lock()
	recvDir = dir
	recvDirMu.Unlock()
}

func getRecvDir() string {
	recvDirMu.RLock()
	defer recvDirMu.RUnlock()
	return recvDir
}

func newSessionID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

func frameWrite(w io.Writer, typ byte, payload []byte) error {
	lenBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(lenBytes, uint32(len(payload)))
	if _, err := w.Write(lenBytes); err != nil {
		return fmt.Errorf("write frame len: %w", err)
	}
	if _, err := w.Write([]byte{typ}); err != nil {
		return fmt.Errorf("write frame type: %w", err)
	}
	if len(payload) > 0 {
		if _, err := w.Write(payload); err != nil {
			return fmt.Errorf("write frame payload: %w", err)
		}
	}
	return nil
}

func frameRead(r io.Reader) (byte, []byte, error) {
	lenBytes := make([]byte, 4)
	if _, err := io.ReadFull(r, lenBytes); err != nil {
		return 0, nil, fmt.Errorf("read frame len: %w", err)
	}
	plen := binary.BigEndian.Uint32(lenBytes)
	typBuf := make([]byte, 1)
	if _, err := io.ReadFull(r, typBuf); err != nil {
		return 0, nil, fmt.Errorf("read frame type: %w", err)
	}
	payload := make([]byte, plen)
	if plen > 0 {
		if _, err := io.ReadFull(r, payload); err != nil {
			return 0, nil, fmt.Errorf("read frame payload: %w", err)
		}
	}
	return typBuf[0], payload, nil
}

func bitmapHas(bitmap []uint64, seq int) bool {
	idx := seq / 64
	if idx >= len(bitmap) {
		return false
	}
	return bitmap[idx]&(1<<uint(seq%64)) != 0
}

func bitmapSet(bitmap []uint64, seq int) {
	idx := seq / 64
	if idx < len(bitmap) {
		bitmap[idx] |= 1 << uint(seq%64)
	}
}

func bitmapMaxContinuous(bitmap []uint64, limit int) int {
	for seq := 0; seq < limit; seq++ {
		if !bitmapHas(bitmap, seq) {
			return seq
		}
	}
	return limit
}

func writeJSONFrame(w io.Writer, typ byte, v interface{}) error {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal %x: %w", typ, err)
	}
	return frameWrite(w, typ, b)
}

func readJSONFrame(r io.Reader, expectedType byte, v interface{}) error {
	typ, payload, err := frameRead(r)
	if err != nil {
		return err
	}
	switch typ {
	case frameTypeError:
		var e struct{ Reason string }
		json.Unmarshal(payload, &e)
		return fmt.Errorf("remote error: %s", e.Reason)
	case frameTypeCancel:
		var e struct{ Reason string }
		json.Unmarshal(payload, &e)
		return fmt.Errorf("remote cancelled: %s", e.Reason)
	}
	if typ != expectedType {
		return fmt.Errorf("unexpected frame type %d, want %d", typ, expectedType)
	}
	if v == nil {
		return nil
	}
	return json.Unmarshal(payload, v)
}

func HandleFileStream(s network.Stream) {
	defer s.Close()

	var init struct {
		Name        string `json:"name"`
		Size        int64  `json:"size"`
		ChunkSize   int    `json:"chunk_size"`
		TotalChunks int    `json:"total_chunks"`
		Checksum    string `json:"checksum"`
		Offset      int    `json:"offset"`
	}
	if err := readJSONFrame(s, frameTypeInit, &init); err != nil {
		log.Printf("[relayfile] read INIT: %v", err)
		return
	}

	sessionID := newSessionID()
	safeName := filepath.Base(init.Name)
	saveDir := getRecvDir()
	if saveDir == "" {
		saveDir = "."
	}
	if err := os.MkdirAll(saveDir, 0755); err != nil {
		frameWrite(s, frameTypeReject, mustJSON(struct{ Reason string }{err.Error()}))
		return
	}
	finalPath := filepath.Join(saveDir, safeName)
	tmpPath := finalPath + ".relayfile.part"

	var tmpFile *os.File
	var bitmap []uint64
	var doneBytes int64
	var startSeq int

	if init.TotalChunks > 0 {
		bitmap = make([]uint64, (init.TotalChunks+63)/64)
	}

	if init.Offset > 0 {
		tmpFile, _ = os.OpenFile(tmpPath, os.O_RDWR, 0644)
		if tmpFile != nil {
			info, _ := tmpFile.Stat()
			doneBytes = info.Size()
			writtenChunks := int(doneBytes / int64(init.ChunkSize))
			if doneBytes%int64(init.ChunkSize) != 0 {
				writtenChunks++
			}
			startSeq = writtenChunks
			for i := 0; i < writtenChunks && i < init.TotalChunks; i++ {
				bitmapSet(bitmap, i)
			}
		}
	}
	if tmpFile == nil {
		var err error
		tmpFile, err = os.Create(tmpPath)
		if err != nil {
			frameWrite(s, frameTypeReject, mustJSON(struct{ Reason string }{err.Error()}))
			return
		}
	}

	resumeOffset := bitmapMaxContinuous(bitmap, init.TotalChunks)
	if resumeOffset > startSeq {
		startSeq = resumeOffset
	}
	tmpFile.Seek(int64(startSeq)*int64(init.ChunkSize), io.SeekStart)

	if err := writeJSONFrame(s, frameTypeAccept, struct {
		Offset int `json:"offset"`
	}{startSeq}); err != nil {
		tmpFile.Close()
		os.Remove(tmpPath)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	ses := &fileSession{
		id:          sessionID,
		peer:        s.Conn().RemotePeer(),
		filename:    safeName,
		savePath:    finalPath,
		size:        init.Size,
		chunkSize:   init.ChunkSize,
		totalChunks: init.TotalChunks,
		checksum:    init.Checksum,
		state:       "transfer",
		tmpFile:     tmpFile,
		bitmap:      bitmap,
		tmpPath:     tmpPath,
		doneBytes:   doneBytes,
		cancel:      cancel,
	}
	sessions.Store(sessionID, ses)

	recvCBMu.Lock()
	cb := recvCB[ses.peer]
	recvCBMu.Unlock()

	go func() {
		<-ctx.Done()
		ses.mu.Lock()
		if ses.state != "done" && ses.state != "error" {
			ses.state = "cancelled"
		}
		ses.mu.Unlock()
	}()

	receiveLoop(s, ses, sessionID, cb, startSeq)
}

func receiveLoop(s network.Stream, ses *fileSession, sid string, cb ProgressFunc, startSeq int) {
	lastAck := startSeq
	for {
		s.SetDeadline(time.Now().Add(dataTimeout))
		typ, payload, err := frameRead(s)
		if err != nil {
			cleanupSession(ses, sid, fmt.Errorf("read: %w", err))
			return
		}
		switch typ {
		case frameTypeCancel:
			cleanupSession(ses, sid, fmt.Errorf("cancelled by remote"))
			return
		case frameTypeError:
			var e struct{ Reason string }
			json.Unmarshal(payload, &e)
			cleanupSession(ses, sid, fmt.Errorf("remote error: %s", e.Reason))
			return
		case frameTypeData:
			if len(payload) < 8 {
				continue
			}
			dataSeq := int64(binary.BigEndian.Uint64(payload[:8]))
			chunkData := payload[8:]
			if !bitmapHas(ses.bitmap, int(dataSeq)) {
				if _, err := ses.tmpFile.Write(chunkData); err != nil {
					writeJSONFrame(s, frameTypeError, mustJSON(struct{ Reason string }{err.Error()}))
					cleanupSession(ses, sid, fmt.Errorf("write: %w", err))
					return
				}
				bitmapSet(ses.bitmap, int(dataSeq))
				ses.doneBytes += int64(len(chunkData))
			}
			newSeq := bitmapMaxContinuous(ses.bitmap, ses.totalChunks)
			if newSeq-lastAck >= ackBatch || newSeq >= ses.totalChunks {
				if newSeq > lastAck {
					if err := writeJSONFrame(s, frameTypeAck, struct {
						CS int `json:"cs"`
					}{newSeq - 1}); err != nil {
						cleanupSession(ses, sid, fmt.Errorf("write ack: %w", err))
						return
					}
					lastAck = newSeq
				}
			}
			if cb != nil && newSeq > startSeq {
				cb(FileProgress{
					SessionID:   sid,
					PeerID:      ses.peer,
					Name:        ses.filename,
					TotalBytes:  ses.size,
					DoneBytes:   ses.doneBytes,
					TotalChunks: ses.totalChunks,
					DoneChunks:  newSeq,
					State:       "transfer",
				})
			}
		case frameTypeFin:
			var fin struct {
				TotalChunks int    `json:"total_chunks"`
				Checksum    string `json:"checksum"`
			}
			json.Unmarshal(payload, &fin)
			if fin.Checksum != ses.checksum {
				writeJSONFrame(s, frameTypeError, mustJSON(struct{ Reason string }{"checksum mismatch"}))
				cleanupSession(ses, sid, fmt.Errorf("checksum mismatch"))
				return
			}
			ses.tmpFile.Sync()
			ses.tmpFile.Close()
			ses.tmpFile = nil

			if err := verifyFileHash(ses.tmpPath, ses.checksum); err != nil {
				writeJSONFrame(s, frameTypeError, mustJSON(struct{ Reason string }{err.Error()}))
				cleanupSession(ses, sid, err)
				return
			}
			if err := os.Rename(ses.tmpPath, ses.savePath); err != nil {
				writeJSONFrame(s, frameTypeError, mustJSON(struct{ Reason string }{err.Error()}))
				cleanupSession(ses, sid, fmt.Errorf("rename: %w", err))
				return
			}
			ses.doneBytes = ses.size
			ses.state = "done"
			if err := writeJSONFrame(s, frameTypeDone, struct {
				OK bool `json:"ok"`
			}{true}); err != nil {
				cleanupSession(ses, sid, fmt.Errorf("write done: %w", err))
				return
			}
			if cb != nil {
				cb(FileProgress{
					SessionID:   sid,
					PeerID:      ses.peer,
					Name:        ses.filename,
					TotalBytes:  ses.size,
					DoneBytes:   ses.size,
					TotalChunks: ses.totalChunks,
					DoneChunks:  ses.totalChunks,
					State:       "done",
				})
			}
			log.Printf("[relayfile] received %s from %s (%d bytes, %d chunks)",
				ses.filename, ses.peer.ShortString(), ses.size, ses.totalChunks)
			sessions.Delete(sid)
			return
		}
	}
}

func SendFile(ctx context.Context, pid peer.ID, localPath string, cb ProgressFunc) (string, error) {
	file, err := os.Open(localPath)
	if err != nil {
		return "", fmt.Errorf("open: %w", err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return "", fmt.Errorf("stat: %w", err)
	}
	fileSize := info.Size()
	totalChunks := int(fileSize / chunkSize)
	if fileSize%chunkSize != 0 {
		totalChunks++
	}

	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", fmt.Errorf("hash: %w", err)
	}
	checksum := "sha256:" + hex.EncodeToString(hash.Sum(nil))
	file.Seek(0, io.SeekStart)

	sessionID := newSessionID()

	ctx2, cancel := context.WithCancel(ctx)
	ses := &fileSession{
		id:          sessionID,
		peer:        pid,
		filename:    filepath.Base(localPath),
		size:        fileSize,
		chunkSize:   chunkSize,
		totalChunks: totalChunks,
		checksum:    checksum,
		state:       "init",
		file:        file,
		cancel:      cancel,
	}
	sessions.Store(sessionID, ses)
	defer sessions.Delete(sessionID)

	s, err := bootres.Host.NewStream(
		network.WithAllowLimitedConn(ctx2, "file/send"),
		pid, fullProtoID("file/1.0"),
	)
	if err != nil {
		cancel()
		return sessionID, fmt.Errorf("open stream: %w", err)
	}
	defer s.Close()

	offset := 0
initRetry:
	if err := writeJSONFrame(s, frameTypeInit, struct {
		Name        string `json:"name"`
		Size        int64  `json:"size"`
		ChunkSize   int    `json:"chunk_size"`
		TotalChunks int    `json:"total_chunks"`
		Checksum    string `json:"checksum"`
		Offset      int    `json:"offset"`
	}{
		Name:        filepath.Base(localPath),
		Size:        fileSize,
		ChunkSize:   chunkSize,
		TotalChunks: totalChunks,
		Checksum:    checksum,
		Offset:      offset,
	}); err != nil {
		return sessionID, fmt.Errorf("write init: %w", err)
	}

	var accept struct {
		Offset int `json:"offset"`
	}
	if err := readJSONFrame(s, frameTypeAccept, &accept); err != nil {
		return sessionID, fmt.Errorf("read accept: %w", err)
	}
	offset = accept.Offset
	file.Seek(int64(offset)*chunkSize, io.SeekStart)

	ses.state = "transfer"

	cbOnce := cb
	if cbOnce != nil {
		cbOnce(FileProgress{
			SessionID:   sessionID,
			PeerID:      pid,
			Name:        filepath.Base(localPath),
			TotalBytes:  fileSize,
			DoneBytes:   int64(offset) * chunkSize,
			TotalChunks: totalChunks,
			DoneChunks:  offset,
			State:       "transfer",
		})
	}

	seq := offset
	buf := make([]byte, chunkSize)
	for seq < totalChunks {
		select {
		case <-ctx2.Done():
			frameWrite(s, frameTypeCancel, mustJSON(struct{ Reason string }{"cancelled"}))
			return sessionID, ctx2.Err()
		default:
		}

		batchEnd := seq + ackBatch
		if batchEnd > totalChunks {
			batchEnd = totalChunks
		}
		for ; seq < batchEnd; seq++ {
			n, err := file.Read(buf)
			if err != nil && err != io.EOF {
				frameWrite(s, frameTypeCancel, mustJSON(struct{ Reason string }{"read error"}))
				return sessionID, fmt.Errorf("read file: %w", err)
			}
			if n == 0 {
				break
			}
			pay := make([]byte, 8+n)
			binary.BigEndian.PutUint64(pay[:8], uint64(seq))
			copy(pay[8:], buf[:n])
			if err := frameWrite(s, frameTypeData, pay); err != nil {
				return sessionID, fmt.Errorf("write data: %w", err)
			}
		}

		var ack struct {
			CS int `json:"cs"`
		}
		s.SetDeadline(time.Now().Add(dataTimeout))
		if err := readJSONFrame(s, frameTypeAck, &ack); err != nil {
			s.Close()
			newS, err := bootres.Host.NewStream(
				network.WithAllowLimitedConn(ctx2, "file/resume"),
				pid, fullProtoID("file/1.0"),
			)
			if err != nil {
				return sessionID, fmt.Errorf("reconnect: %w", err)
			}
			s = newS
			offset = seq
			goto initRetry
		}
		seq = ack.CS + 1
		file.Seek(int64(seq)*chunkSize, io.SeekStart)

		if cb != nil {
			cb(FileProgress{
				SessionID:   sessionID,
				PeerID:      pid,
				Name:        filepath.Base(localPath),
				TotalBytes:  fileSize,
				DoneBytes:   int64(seq) * chunkSize,
				TotalChunks: totalChunks,
				DoneChunks:  seq,
				State:       "transfer",
			})
		}
	}

	if err := writeJSONFrame(s, frameTypeFin, struct {
		TotalChunks int    `json:"total_chunks"`
		Checksum    string `json:"checksum"`
	}{
		TotalChunks: totalChunks,
		Checksum:    checksum,
	}); err != nil {
		return sessionID, fmt.Errorf("write fin: %w", err)
	}

	var done struct {
		OK bool `json:"ok"`
	}
	s.SetDeadline(time.Now().Add(dataTimeout))
	if err := readJSONFrame(s, frameTypeDone, &done); err != nil {
		return sessionID, fmt.Errorf("read done: %w", err)
	}
	ses.state = "done"
	if cb != nil {
		cb(FileProgress{
			SessionID:   sessionID,
			PeerID:      pid,
			Name:        filepath.Base(localPath),
			TotalBytes:  fileSize,
			DoneBytes:   fileSize,
			TotalChunks: totalChunks,
			DoneChunks:  totalChunks,
			State:       "done",
		})
	}
	log.Printf("[relayfile] sent %s to %s (%d bytes, %d chunks)",
		localPath, pid.ShortString(), fileSize, totalChunks)
	return sessionID, nil
}

func CancelFileSession(sessionID string) error {
	v, ok := sessions.Load(sessionID)
	if !ok {
		return fmt.Errorf("session %s not found", sessionID)
	}
	ses := v.(*fileSession)
	ses.mu.Lock()
	ses.state = "cancelled"
	ses.cancel()
	ses.mu.Unlock()
	return nil
}

func ReceiveFile(ctx context.Context, pid peer.ID, saveDir string, cb ProgressFunc) (string, error) {
	if saveDir != "" {
		SetFileRecvDir(saveDir)
	}
	recvCBMu.Lock()
	if recvCB == nil {
		recvCB = make(map[peer.ID]ProgressFunc)
	}
	if cb != nil {
		recvCB[pid] = cb
	}
	recvCBMu.Unlock()
	defer func() {
		recvCBMu.Lock()
		delete(recvCB, pid)
		recvCBMu.Unlock()
	}()

	for {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		default:
		}
		var found string
		sessions.Range(func(key, value interface{}) bool {
			ses := value.(*fileSession)
			if ses.peer == pid {
				found = key.(string)
				return false
			}
			return true
		})
		if found != "" {
			v, _ := sessions.Load(found)
			ses := v.(*fileSession)
			ses.mu.Lock()
			state := ses.state
			ses.mu.Unlock()
			if state == "done" {
				return found, nil
			}
			if state == "error" || state == "cancelled" {
				return found, fmt.Errorf("transfer %s", state)
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func mustJSON(v interface{}) []byte {
	b, _ := json.Marshal(v)
	return b
}

func cleanupSession(ses *fileSession, sid string, err error) {
	ses.mu.Lock()
	if ses.state == "done" || ses.state == "error" || ses.state == "cancelled" {
		ses.mu.Unlock()
		return
	}
	ses.state = "error"
	if ses.tmpFile != nil {
		ses.tmpFile.Close()
		ses.tmpFile = nil
	}
	if ses.tmpPath != "" {
		os.Remove(ses.tmpPath)
	}
	cb := getRecvCB(ses.peer)
	ses.mu.Unlock()

	if cb != nil {
		cb(FileProgress{
			SessionID:   sid,
			PeerID:      ses.peer,
			Name:        ses.filename,
			TotalBytes:  ses.size,
			DoneBytes:   ses.doneBytes,
			TotalChunks: ses.totalChunks,
			DoneChunks:  bitmapMaxContinuous(ses.bitmap, ses.totalChunks),
			State:       "error",
			Err:         err,
		})
	}
	sessions.Delete(sid)
	log.Printf("[relayfile] session %s error: %v", sid, err)
}

func getRecvCB(pid peer.ID) ProgressFunc {
	recvCBMu.Lock()
	defer recvCBMu.Unlock()
	if recvCB == nil {
		return nil
	}
	return recvCB[pid]
}

func verifyFileHash(path, checksum string) error {
	if len(checksum) < 7 || checksum[:7] != "sha256:" {
		return fmt.Errorf("unknown checksum format")
	}
	expected := checksum[7:]
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return err
	}
	got := hex.EncodeToString(h.Sum(nil))
	if got != expected {
		return fmt.Errorf("checksum mismatch: expected %s, got %s", expected, got)
	}
	return nil
}
