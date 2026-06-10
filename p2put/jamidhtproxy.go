package p2put

import (
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const JamiUserAgent = "Jami Daemon (iOS/arm64) libp2px/888"

var (
	useJamiDHTProxy = true
	jamidhtproxy    *JamiDHTProxy
)

type JamiDHTProxy struct {
	ProxyURL string
	client   *http.Client
}

type jamidhtValue struct {
	Data string `json:"data"`
	ID   int64  `json:"id,omitempty"`
	Time int64  `json:"time,omitempty"`
}

func NewJamiDHTProxy(proxyURL string) *JamiDHTProxy {
	return &JamiDHTProxy{
		ProxyURL: proxyURL,
		client: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

func (j *JamiDHTProxy) infoHash(key string) string {
	h := sha1.Sum([]byte(key))
	return hex.EncodeToString(h[:])
}

func (j *JamiDHTProxy) Put(key string, value []byte) error {
	infohash := j.infoHash(key)
	b64data := base64.StdEncoding.EncodeToString(value)
	body, _ := json.Marshal(map[string]string{"data": b64data})

	url := fmt.Sprintf("%s/key/%s", j.ProxyURL, infohash)
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("jamidht put: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", JamiUserAgent)
	resp, err := j.client.Do(req)
	if err != nil {
		return fmt.Errorf("jamidht put: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("jamidht put: status %d: %s", resp.StatusCode, string(b))
	}
	return nil
}

func (j *JamiDHTProxy) Get(key string) ([]byte, error) {
	infohash := j.infoHash(key)
	url := fmt.Sprintf("%s/key/%s", j.ProxyURL, infohash)

	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("jamidht get: %w", err)
	}
	req.Header.Set("User-Agent", JamiUserAgent)
	resp, err := j.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("jamidht get: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return nil, fmt.Errorf("jamidht get: key not found")
	}
	if resp.StatusCode != 200 {
		b, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("jamidht get: status %d: %s", resp.StatusCode, string(b))
	}

	body, _ := io.ReadAll(resp.Body)
	var values []jamidhtValue
	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	for _, line := range lines {
		var v jamidhtValue
		if err := json.Unmarshal([]byte(line), &v); err != nil {
			continue
		}
		values = append(values, v)
	}
	if len(values) == 0 {
		return nil, fmt.Errorf("jamidht get: no values")
	}

	data, err := base64.StdEncoding.DecodeString(values[len(values)-1].Data)
	if err != nil {
		return nil, fmt.Errorf("jamidht get: decode: %w", err)
	}
	return data, nil
}
