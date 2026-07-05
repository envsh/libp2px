package p2put

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"
)

const defaultJamiProxy = "http://dhtproxy.jami.net:80"

// fallbackJamiProxy 硬编码当前解析到的 IP 作为备选。
// 部分节点上 DNS 解析更新不及时，当 dhtproxy.jami.net
// 的 A 记录变更后仍连接旧 IP 导致 dial refused，此时
// 直接使用已知可达的 IP 地址重试。
const fallbackJamiProxy = "http://141.94.96.2:80"

type JamiTrackerApi struct {
	baseURL string
	cli     *http.Client
}

func NewJamiTrackerApi(baseURL string) *JamiTrackerApi {
	return &JamiTrackerApi{
		baseURL: strings.TrimRight(baseURL, "/"),
		cli:     http.DefaultClient,
	}
}

func (api *JamiTrackerApi) hashKey(key string) string {
	h := sha1.Sum([]byte(key))
	return fmt.Sprintf("%x", h)
}

func (api *JamiTrackerApi) Provide(ctx context.Context, key string, pid peer.ID, addrs []multiaddr.Multiaddr) error {
	hash := api.hashKey(key)

	addrStrs := make([]string, len(addrs))
	for i, a := range addrs {
		addrStrs[i] = a.String()
	}

	peerInfo := FoundPeer{PeerID: pid.String(), Addrs: addrStrs}
	peerJSON, err := json.Marshal(peerInfo)
	if err != nil {
		return fmt.Errorf("marshal peer: %w", err)
	}

	dataB64 := base64.StdEncoding.EncodeToString(peerJSON)
	body := fmt.Sprintf(`{"data":"%s","id":0,"type":0}`, dataB64)

	urls := []string{api.baseURL, fallbackJamiProxy}
	var lastErr error
	for _, base := range urls {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost,
			base+"/key/"+hash, strings.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := api.cli.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		if resp.StatusCode >= 300 {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return fmt.Errorf("POST %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
		}
		resp.Body.Close()
		return nil
	}
	return lastErr
}

func (api *JamiTrackerApi) FindProviders(ctx context.Context, key string) ([]FoundPeer, error) {
	hash := api.hashKey(key)

	urls := []string{api.baseURL, fallbackJamiProxy}
	var lastErr error
	for _, base := range urls {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet,
			base+"/key/"+hash, nil)
		if err != nil {
			return nil, err
		}

		resp, err := api.cli.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		body, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}

		var out []FoundPeer
		for _, line := range strings.Split(strings.TrimSpace(string(body)), "\n") {
			if line == "" {
				continue
			}
			var val struct {
				Data string `json:"data"`
			}
			if err := json.Unmarshal([]byte(line), &val); err != nil {
				continue
			}
			if val.Data == "" {
				continue
			}
			raw, err := base64.StdEncoding.DecodeString(val.Data)
			if err != nil {
				continue
			}
			var fp FoundPeer
			if err := json.Unmarshal(raw, &fp); err != nil {
				continue
			}
			if fp.PeerID == "" {
				continue
			}
			out = append(out, fp)
		}
		return out, nil
	}
	return nil, lastErr
}

func (api *JamiTrackerApi) Ping(ctx context.Context) error {
	urls := []string{api.baseURL, fallbackJamiProxy}
	var lastErr error
	for _, base := range urls {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet,
			base+"/node/info", nil)
		if err != nil {
			return err
		}
		resp, err := api.cli.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		resp.Body.Close()
		if resp.StatusCode >= 300 {
			return fmt.Errorf("Ping: %d", resp.StatusCode)
		}
		return nil
	}
	return lastErr
}

func (api *JamiTrackerApi) Listen(ctx context.Context, topic string, cb func(FoundPeer)) error {
	hash := api.hashKey(topic)

	urls := []string{api.baseURL, fallbackJamiProxy}
	var lastErr error
	for _, base := range urls {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet,
			base+"/key/"+hash+"/listen", nil)
		if err != nil {
			return err
		}

		resp, err := api.cli.Do(req)
		if err != nil {
			lastErr = err
			continue
		}

		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			var val struct {
				Data    string `json:"data"`
				Expired *bool  `json:"expired,omitempty"`
			}
			if err := json.Unmarshal([]byte(line), &val); err != nil {
				continue
			}
			if val.Data == "" {
				continue
			}
			raw, err := base64.StdEncoding.DecodeString(val.Data)
			if err != nil {
				continue
			}
			var fp FoundPeer
			if err := json.Unmarshal(raw, &fp); err != nil {
				continue
			}
			if fp.PeerID == "" {
				continue
			}
			cb(fp)
		}
		resp.Body.Close()
		return scanner.Err()
	}
	return lastErr
}
