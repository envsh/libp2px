package p2put

import (
	_ "embed"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

func InstallRestHandler(path string, mux *http.ServeMux) {
	if mux == nil {
		mux = http.DefaultServeMux
	}
	myinstall := func(name string, f func(w http.ResponseWriter, r *http.Request)) {
		uri := filepath.Join(path, name)
		mux.HandleFunc(uri, f)
	}
	myinstall("board", onBoard)
	myinstall("relays", onRelays)
	myinstall("dht", onDHT)
	myinstall("conns", onConns)
	myinstall("peers", onPeers)
	myinstall("kv", onKV)
	myinstall("index", onIndex)
	myinstall("events", onEvents)
	mux.HandleFunc("/api/events", onEvents) // 模拟 toxhs 的 events 接口
	myinstall("send", onSend)
	myinstall("unsub", onUnsub)
	myinstall("store_peers", onStorePeers)
	myinstall("stable_peers", onStablePeers)
	myinstall("topics", onTopics)
	myinstall("findpeers", onFindPeers)
	myinstall("findpeer", onFindPeer)
}

func onFindPeers(w http.ResponseWriter, r *http.Request) {
	tag := r.URL.Query().Get("tag")
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	result, err := FindPeers(tag, limit)
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(w, result)
}

func onFindPeer(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	if id == "" {
		writeErr(w, http.StatusBadRequest, "missing id")
		return
	}
	result, err := FindPeer(id)
	if err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, result)
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func onBoard(w http.ResponseWriter, r *http.Request) {
	resp, err := CollectBoard()
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(w, resp)
}

func onDHT(w http.ResponseWriter, r *http.Request) {
	resp, err := CollectDHT()
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(w, resp)
}

func onRelays(w http.ResponseWriter, r *http.Request) {
	resp, err := CollectRelays()
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(w, resp)
}

func onPeers(w http.ResponseWriter, r *http.Request) {
	resp, err := CollectConns()
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(w, resp)
}

func onConns(w http.ResponseWriter, r *http.Request) {
	resp, err := CollectConns()
	if err != nil {
		writeErr(w, http.StatusServiceUnavailable, err.Error())
		return
	}
	writeJSON(w, resp)
}

func onIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write([]byte(indexHTML))
}

func onKV(w http.ResponseWriter, r *http.Request) {
	op := r.URL.Query().Get("op")
	key := r.URL.Query().Get("key")
	if key == "" {
		writeErr(w, http.StatusBadRequest, "missing key")
		return
	}

	switch r.Method {
	case http.MethodGet:
		switch op {
		case "get":
			val, err := GetKV(r.Context(), key)
			if err != nil {
				writeErr(w, http.StatusNotFound, err.Error())
				return
			}
			w.Header().Set("Content-Type", "application/octet-stream")
			w.Write(val)
		case "delete":
			if err := DelKV(r.Context(), key); err != nil {
				writeErr(w, http.StatusInternalServerError, err.Error())
				return
			}
			writeJSON(w, map[string]bool{"ok": true})
		default:
			writeErr(w, http.StatusBadRequest, "unknown op: "+op)
		}
	case http.MethodPost:
		if op != "put" {
			writeErr(w, http.StatusBadRequest, "unknown op: "+op)
			return
		}
		val, err := io.ReadAll(r.Body)
		if err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		if err := PutKV(r.Context(), key, val); err != nil {
			writeErr(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, map[string]bool{"ok": true})
	default:
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
	}
}

//go:embed index.html
var indexHTML string

func onEvents(w http.ResponseWriter, r *http.Request) {
	topicStr := r.URL.Query().Get("topic")
	var topics []string
	if topicStr != "" {
		for _, t := range strings.Split(topicStr, ",") {
			t = strings.TrimSpace(t)
			if t != "" {
				topics = append(topics, t)
				go getOrSubscribeTopic(t)
			}
		}
	}

	ch := make(chan Event, 128)
	clientsMu.Lock()
	clients[ch] = struct{}{}
	if len(topics) > 0 {
		clientTopics[ch] = topics
	}
	clientsMu.Unlock()
	defer func() {
		clientsMu.Lock()
		delete(clients, ch)
		delete(clientTopics, ch)
		clientsMu.Unlock()
		close(ch)
	}()

	var events []Event

	writeEvents := func() {
		w.Header().Set("Content-Type", "application/x-ndjson")
		for _, e := range events {
			json.NewEncoder(w).Encode(e)
		}
	}

	// Phase 0: non-blocking drain backlog
	for len(events) < 20 {
		select {
		case evt := <-ch:
			events = append(events, evt)
		default:
			goto afterDrain
		}
	}
	writeEvents()
	return

afterDrain:
	if len(events) > 0 {
		goto collectMore
	}

	// Phase 1: wait for first event, up to 30s
	select {
	case evt := <-ch:
		events = append(events, evt)
	case <-time.After(30 * time.Second):
		w.Header().Set("Content-Type", "application/x-ndjson")
		json.NewEncoder(w).Encode(map[string]string{"event": "timeout"})
		return
	case <-r.Context().Done():
		return
	}

collectMore:
	// Phase 2: collect more with 200ms idle timeout
	for len(events) < 20 {
		select {
		case evt := <-ch:
			events = append(events, evt)
		case <-time.After(200 * time.Millisecond):
			writeEvents()
			return
		case <-r.Context().Done():
			return
		}
	}
	writeEvents()
}

func onSend(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	if topic == "" {
		writeErr(w, http.StatusBadRequest, "missing topic")
		return
	}
	if r.Method != http.MethodPost {
		writeErr(w, http.StatusMethodNotAllowed, "POST only")
		return
	}
	data, err := io.ReadAll(io.LimitReader(r.Body, int64(maxPublishSize)+1))
	if err != nil {
		writeErr(w, http.StatusRequestEntityTooLarge, "payload too large")
		return
	}
	if err := PublishTopic(topic, data); err != nil {
		writeErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, map[string]bool{"ok": true})
}

func onUnsub(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	if topic == "" {
		writeErr(w, http.StatusBadRequest, "missing topic")
		return
	}
	if err := UnsubscribeTopic(topic); err != nil {
		writeErr(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSON(w, map[string]bool{"ok": true})
}

func onStorePeers(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, CollectStorePeers())
}

func onStablePeers(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, CollectStablePeers())
}

func onTopics(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, CollectTopics())
}
