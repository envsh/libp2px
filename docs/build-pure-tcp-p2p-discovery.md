# 构建纯 TCP P2P 发现与通信系统

## 1. 背景

在构建一个基于 libp2p 的 P2P 通信系统时，最常见的发现方式是依赖 Kademlia DHT。DHT 使用 UDP，在公网环境下工作良好，但在实际部署中遇到几个问题：

- **UDP 被限制**：部分云环境或容器网络对 UDP 有速率限制甚至直接封锁
- **DHT 启动慢**：首次启动需要连接全局 bootstrap 节点并等待路由表就绪，通常需要 30-60 秒
- **NAT 穿透不稳定**：即使在公网节点上，DHT 的 UDP 端口也可能被防火墙拦截
- **DHT 维护开销**：周期性刷新路由表，即使没有查询需求也会产生流量

目标很明确：构建一个**纯 TCP/WebSocket** 的 P2P 通信系统，不依赖 DHT 做发现，而是通过 GossipSub、HTTP 路由查询和持久化 PeerDB 三层机制实现自包含的节点发现。

技术栈：go-libp2p v0.36.2、go-libp2p-pubsub v0.12.0、go-libp2p-kad-dht v0.26.0（仅 Client Mode）。

## 2. 整体架构

系统分为四层：

```
网络层    TCP / WebSocket / Relay / HolePunching
消息层    GossipSub (PX + FloodPublish)
发现层    Gossip ↔ HTTP closest/peers ↔ PeerDB reconnection
应用层    REST API / Event Bus / Topic PubSub
```

- **网络层**：负责底层连接建立，通过 libp2p host 管理 TCP/WebSocket 传输、Relay 连接和 Hole Punching
- **消息层**：GossipSub 负责节点间的实时消息交换，同时承担节点公告和 Peer Exchange 的角色
- **发现层**：三级发现流水线，从最快到最慢提供不同可靠性的节点发现
- **应用层**：REST API 管理面板、事件总线、Topic PubSub 消息

### 2.1 启动流程

```
Bootstrap() → 加载私钥 → 创建 libp2p host → 初始化 AutoRelay
→ 启动 DHT (Client Mode) → 创建 GossipSub → 启动 PeerGossip
→ 启动发现流水线 → 订阅 Topic
```

所有组件共享一个全局 `BootNode` 实例：

```go
type BootNode struct {
    Host      host.Host
    DHT       *dht.IpfsDHT
    PSO       *pubsub.PubSub
    PeerID    peer.ID
    AddrMgr   *AddrManager
    PubkeyHex string
    PeerDB    *PeerDB
    Discovery *routing.RoutingDiscovery
}
```

配置分为两种模式：

| 参数 | 桌面模式 | 移动模式 |
|------|----------|----------|
| AutoRelay relay 数 | 5 | 2 |
| AutoRelay 候选数 | 5 | 3 |
| 启动延迟 | 60s | 30s |
| GossipSub Heartbeat | 10s | 10s |
| 发现机制 | DHT FindPeers | HTTP + Gossip + PeerDB |

## 3. 网络层与地址管理

### 3.1 传输层

系统使用 TCP 和 WebSocket 两种传输协议，明确排除了 QUIC 和 WebRTC（基于 UDP）：

```go
h, err := libp2p.New(
    libp2p.Identity(libp2pPriv),
    libp2p.ListenAddrs(listenAddr),
    libp2p.Transport(tcp.NewTCPTransport),
    libp2p.Transport(websocket.New),
    libp2p.AddrsFactory(myAddrsFactory),
    libp2p.EnableRelay(),
    autoRelayOpt,
    libp2p.EnableHolePunching(),
)
```

### 3.2 地址管理三源模型

`AddrManager` 管理三个独立的地址来源，在 `GetAll()` 中合并：

```go
type AddrManager struct {
    mu                sync.RWMutex
    localAddrs        []multiaddr.Multiaddr
    relayCircuitAddrs []multiaddr.Multiaddr
    relayVouches      map[peer.ID]*relayVouch
}

type relayVouch struct {
    addrs      []multiaddr.Multiaddr
    expiration time.Time
}
```

- **localAddrs**：来自 `EvtLocalAddressesUpdated` 事件，每次地址变更时覆盖替换
- **relayCircuitAddrs**：同样来自地址变更事件中的 `Removed` 列表，记录已移除的中继地址
- **relayVouches**：由 `watchStaticRelays` 通过 `client.Reserve()` 获取，keyed by relay peer ID，带过期时间

`GetAll()` 合并三源地址，同时惰性清理过期 vouches 和空 circuit：

```go
func (am *AddrManager) GetAll() []multiaddr.Multiaddr {
    am.mu.RLock()
    out := make([]multiaddr.Multiaddr, 0,
        len(am.localAddrs)+len(am.relayCircuitAddrs)+len(am.relayVouches))
    out = append(out, am.localAddrs...)
    for _, v := range am.relayVouches {
        if time.Now().After(v.expiration) { continue }
        out = append(out, v.addrs...)
    }
    am.mu.RUnlock()
    return out
}
```

### 3.3 中继管理

系统使用 4 个静态 Relay 节点，`watchStaticRelays` 以双循环维护中继连接：

```go
go func() {
    // Ping 循环：30s 间隔
    for range pingTicker.C {
        for _, r := range relays {
            if !connected { continue }
            <-ping.Ping(ctx, h, r.ID)
        }
    }
}()

// 主循环：60s 间隔重新 Reserve
for range ticker.C {
    for _, r := range relays {
        if !connected {
            res, err := client.Reserve(ctx, h, r)
            bootres.AddrMgr.SetRelayVouch(r.ID, res.Addrs, res.Expiration)
        }
    }
}
```

AutoRelay 配置差异：

| 参数 | 桌面 | 移动 |
|------|------|------|
| `WithNumRelays` | 5 | 2 |
| `WithMinCandidates` | 5 | 3 |
| `WithBootDelay` | 60s | 30s |
| `WithBackoff` | 3min | 3min |

### 3.4 地址过滤

`myAddrsFactory` 过滤掉回环地址，保留中继地址。`IsGoodPeerAddr` 在连接前过滤目标节点：

- 仅保留 `/tcp/4001` 或 `/tcp/443` 端口
- 仅接收公网 IPv4 地址
- 跳过 bootstrap 节点

```go
func IsGoodPeerAddr(info peer.AddrInfo) bool {
    if isBootstrapPeer(info.ID) { return false }
    for _, a := range info.Addrs {
        s := a.String()
        if !strings.HasSuffix(s, "/tcp/4001") &&
           !strings.HasSuffix(s, "/tcp/443") {
            continue
        }
        if strings.Contains(s, "ip4") && !strings.Contains(s, ":") {
            return true
        }
    }
    return false
}
```

## 4. 核心发现：三级流水线

发现系统设计了三级机制，从最快到最慢依次尝试，任意一层失效仍有后备。

### 第一级：GossipSub 实时发现（最快，<1 min）

GossipSub 不仅承担消息传输，还是最主要的节点发现通道。

初始化参数：

```go
pso, err := pubsub.NewGossipSub(ctx, h,
    pubsub.WithPeerExchange(true),        // 启用 PX
    pubsub.WithFloodPublish(true),         // 即使不在 mesh 中也能发布
    pubsub.WithDirectPeers(staticRelays), // 中继作为直接节点
    pubsub.WithGossipSubParams(myGossipSubParams()),
)
```

`PeerGossip` 结构负责在 GossipSub 话题上公告和发现节点：

```go
type PeerGossip struct {
    host      host.Host
    ps        *pubsub.PubSub
    topic     *pubsub.Topic
    sub       *pubsub.Subscription
    db        *PeerDB
    topicName string
    seq       uint64
    addrs     atomic.Value
}
```

**公告格式**：

```go
type PeerAnnounce struct {
    PeerID string   `json:"peer_id"`
    Addrs  []string `json:"addrs"`
    Seq    uint64   `json:"seq"`
    TS     int64    `json:"ts"`
}
```

**发布循环**（60s 间隔）：

```go
func (g *PeerGossip) pubLoop(ctx context.Context) {
    for {
        select {
        case <-time.After(publishInterval):
            addrs := g.addrs.Load().(*peerAddrs)
            ann := PeerAnnounce{
                PeerID: g.host.ID().String(),
                Addrs:  addrStrings(addrs.current),
                Seq:    atomic.AddUint64(&g.seq, 1),
                TS:     time.Now().UnixMilli(),
            }
            data, _ := json.Marshal(ann)
            g.topic.Publish(ctx, data)
        case <-ctx.Done():
            return
        }
    }
}
```

**订阅循环**：收到公告后解析、跳过自身、计算地址差异、更新 PeerDB。

地址变更监听器 `onEvent` 监听 `EvtLocalAddressesUpdated` 事件，以原子操作保存当前地址：

```go
func (g *PeerGossip) onEvent(e event.EvtLocalAddressesUpdated) {
    g.addrs.Store(&peerAddrs{
        current: e.Current,
        removed: e.Removed,
    })
}
```

GossipSub 参数（桌面模式默认）：

| 参数 | 值 |
|------|-----|
| D（mesh 目标度） | 3 |
| Dlo（mesh 下限） | 2 |
| Dhi（mesh 上限） | 6 |
| Dlazy（懒散布数） | 6 |
| GossipFactor | 0.25 |
| HeartbeatInterval | 10s |
| HistoryLength | 5 |
| HistoryGossip | 3 |

### 第二级：HTTP Tracker 随机探测（1-2s/次）

GossipSub 可能无法发现所有节点（例如新加入的节点尚未建立 Gossip 连接），此时通过 HTTP Tracker 的 `closest/peers` 端点进行随机探测。

```go
func (api *IpfsHttpTrackerApi) FindClosestPeers(ctx context.Context, key string) ([]FoundPeer, error) {
    req, err := http.NewRequestWithContext(ctx, http.MethodGet,
        api.baseURL+"/routing/v1/dht/closest/peers/"+key, nil)
    resp, err := api.cli.Do(req)
    var pr dhtPeersResponse
    json.NewDecoder(resp.Body).Decode(&pr)
    for _, p := range pr.Peers {
        out = append(out, FoundPeer{PeerID: p.ID, Addrs: p.Addrs})
    }
    return out, nil
}
```

使用**随机键**进行查询（避免缓存效应）：

```go
randomKey := fmt.Sprintf("r-%d", time.Now().UnixNano())
cid := StringToCID(randomKey)
peers, err := api.FindClosestPeers(ctx, cid)
```

发现循环逻辑：

1. 首次间隔 20s（`discoverInterval/3`），后续 60s
2. 解析返回的 peer ID 和地址
3. 跳过自身和已连接的节点
4. 调用 `IsGoodPeerAddr` 预过滤（仅保留公网 TCP 地址）
5. 连接上限 20 个
6. 连接后调用 `pushToConnected` 向该节点推送自身地址
7. 连接间间隔 5s，每 3 个节点跳过 1 个

```go
for n, p := range peers {
    if n%3 == 1 { time.Sleep(1 * time.Second); continue }
    pid, _ := peer.Decode(p.PeerID)
    if pid == myID { continue }
    if bootres.Host.Network().Connectedness(pid) == network.Connected { continue }
    if len(bootres.Host.Network().Conns()) > 20 { break }
    info := peer.AddrInfo{ID: pid, Addrs: maddrs}
    if !IsGoodPeerAddr(info) { continue }
    bootres.Host.Connect(ctx, info)
    pushToConnected(ctx, bootres.Host, pid, bootres.AddrMgr.GetAll())
    time.Sleep(5 * time.Second)
}
```

### 第三级：PeerDB 重连（15s/次）

PeerDB 中记录了历史上通过 GossipSub 发现过的所有节点。当新节点上线或网络波动时，通过 PeerDB 重连可以快速恢复连接。

```go
func reconnectFromPeerDB(ctx context.Context) {
    topicPeers := make(map[peer.ID]struct{})
    for _, pid := range bootres.PSO.ListPeers(currConfig.HubName) {
        topicPeers[pid] = struct{}{}
    }
    records := bootres.PeerDB.List()
    for _, r := range records {
        if _, in := topicPeers[r.PeerID]; in { continue }
        if bootres.Host.Network().Connectedness(r.PeerID) != network.NotConnected { continue }
        if len(bootres.Host.Network().Conns()) > 20 { break }
        info := peer.AddrInfo{ID: r.PeerID, Addrs: r.Addrs}
        cctx := network.WithAllowLimitedConn(ctx, "peerdb-reconnect")
        bootres.Host.Connect(cctx, info)
        <-ping.Ping(ctx, bootres.Host, r.PeerID)
        time.Sleep(5 * time.Second)
    }
}
```

关键逻辑：

- 跳过已在 GossipSub 话题中活跃的节点
- 跳过已连接的节点
- `WithAllowLimitedConn` 允许通过 Relay 建立受限连接
- 连接成功后执行 Ping 验证可达性
- 连接间间隔 5s，每次最多处理到 20 个连接

### 桌面模式：DHT 发现

桌面模式下（非移动）使用 Kademlia DHT 作为主要发现手段，通过 `routing.RoutingDiscovery.FindPeers()` 查询 `HubName` 话题相关的节点。

DHT 配置为 Client Mode，不提供服务，仅用于查询：

```go
kadDHT, err := dht.New(ctx, h,
    dht.Mode(dht.ModeClient),
    dht.BootstrapPeers(bootstrapInfos...),
    dht.Concurrency(3),
    dht.RoutingTableRefreshPeriod(5 * time.Minute),
)
```

桌面与移动模式在发现策略上的核心差异：

| | 桌面 | 移动 |
|---|---|---|
| 主要发现 | DHT FindPeers | HTTP closest/peers |
| 辅助发现 | HTTP providers | GossipSub + PeerDB |
| 连接策略 | connfixer（分批连接 + 直连升级） | 直接连接 |
| 核心发现间隔 | 3s 扫描 + 120s 完整循环 | 20s/60s 循环 |

## 5. PeerDB 持久化

PeerDB 是一个内存中的节点记录存储，带 TTL 过期清理，用于解决 GossipSub 消息不可靠导致的节点信息丢失。

```go
type PeerRecord struct {
    PeerID peer.ID
    Addrs  []multiaddr.Multiaddr
    SeenAt time.Time
}

type PeerDB struct {
    mu    sync.RWMutex
    peers map[peer.ID]*PeerRecord
    ttl   time.Duration
}
```

API 设计：

```go
func NewPeerDB(ttl time.Duration) *PeerDB

func (db *PeerDB) Update(pid peer.ID, addrs []multiaddr.Multiaddr)
func (db *PeerDB) Get(pid peer.ID) *PeerRecord
func (db *PeerDB) List() []*PeerRecord

func (db *PeerDB) cleanup(ctx context.Context)
```

- TTL 设为 30 分钟，超出时间的记录在 `List()` 和 `Get()` 中跳过
- 后台清理 goroutine 每 5 分钟运行一次，移除过期记录
- 写入来源：GossipSub 订阅循环中收到的 `PeerAnnounce`
- 读取来源：`reconnectFromPeerDB` 重连循环

```go
func (db *PeerDB) cleanup(ctx context.Context) {
    ticker := time.NewTicker(5 * time.Minute)
    for {
        select {
        case <-ticker.C:
            db.mu.Lock()
            now := time.Now()
            for id, r := range db.peers {
                if now.Sub(r.SeenAt) > db.ttl {
                    delete(db.peers, id)
                }
            }
            db.mu.Unlock()
        case <-ctx.Done():
            return
        }
    }
}
```

## 6. HTTP Tracker 封装

HTTP Tracker 作为辅助发现手段，封装了 Delegated Routing V1 HTTP API 的三个 GET 端点。

### 6.1 API 封装

```go
type IpfsHttpTrackerApi struct {
    baseURL string
    cli     *http.Client
}
```

四个方法：

| 方法 | HTTP | 端点 | 状态 |
|------|------|------|------|
| `FindProviders` | GET | `/routing/v1/providers/{cid}` | ✅ 可用 |
| `FindClosestPeers` | GET | `/routing/v1/dht/closest/peers/{key}` | ✅ 可用 |
| `FindPeers` | GET | `/routing/v1/peers/{peer-id}` | ✅ 可用 |
| `Provide` | PUT | `/routing/v1/providers/` | ❌ 不可用 |

### 6.2 GET 响应格式（IPIP-417）

GET providers 响应为扁平 JSON 格式：

```json
{
  "Providers": [
    {
      "ID": "12D3KooW...",
      "Addrs": ["/ip4/1.2.3.4/tcp/4001"],
      "Schema": "peer"
    }
  ]
}
```

`/dht/closest/peers` 和 `/peers` 端点返回类似结构，字段名从 `Providers` 变为 `Peers`。

### 6.3 PUT 写入不可用

`PUT /routing/v1/providers/` 端点是一个历史遗留实验性端点（[IPIP-526](https://github.com/ipfs/specs/pull/526)），从未被 HTTP Routing V1 规范正式标准化。

实际验证结果：所有公开 Tracker 均返回 500：

| Tracker | PUT 结果 |
|---------|----------|
| `delegated-ipfs.dev` | 500: "routing: operation or key not supported" |
| `routing.lol` | 500 |
| `peers.pleb.bot` | 500 |

这是由于规范化历程中写操作被反复搁置：

1. **IPIP-337**（2022）：定义 Delegated Routing HTTP API，仅规定了 GET 类端点，有意排除了写操作
2. **未文档化的 PUT 端点**（2022）：为 index-provider/IPNI 集成引入的遗留端点，仅接受已签名的 Bitswap 记录
3. **IPIP-378**（2023）：尝试用 POST 替代 PUT 重新设计写操作，最终关闭为"历史文物"
4. **IPIP-526**（2025）：将遗留 PUT 端点正式文档化为"历史 IPIP"，**不改变任何现有规范**

### 6.4 广告代码的归档

`AdvertiseHTTP` 函数尝试以 50s 间隔通过 PUT 向 Tracker 注册当前节点为 `HubName` 的 provider，但鉴于所有公开 Tracker 均不支持写入，该函数已归档为遗留代码。

PUT 请求构造格式（IPIP-526 已签名 Bitswap）：

```json
{
  "Providers": [{
    "Schema": "bitswap",
    "Protocol": "transport-bitswap",
    "Signature": "mbase64...",
    "Payload": {
      "Keys": ["bafkrei..."],
      "Timestamp": 1700000000000,
      "AdvisoryTTL": 172800000000000,
      "ID": "12D3KooW...",
      "Addrs": ["/ip4/.../tcp/4001"]
    }
  }]
}
```

## 7. 踩坑实录

### 7.1 `cleanPeerstore` 误删自身私钥

**问题**：`BootNode` 启动后 5 分钟，`[advertise] no private key` 日志出现。

**根因**：`cleanPeerstore()` 每 5 分钟遍历 peerstore 中的所有 peer，清理掉不活跃（`Connectedness != Connected`）且非 bootstrap 的节点。但 peerstore 中的 `Peers()` 包含 host 自身（`makeSwarm` 通过 `AddPrivKey` 将私钥存入），而 `Connectedness(hostID)` 返回 `NotConnected`（不与自己"连接"），导致 `RemovePeer` 删掉了 `sks[hostID]`。

```
cleanPeerstore 执行路径：
ps.Peers()  → hostID 在其中（AddPrivKey 写了 sks[hostID]）
isBootstrapPeer(hostID) → false
Connectedness(hostID) → NotConnected（无自连接）
RemovePeer(hostID) → delete(mkb.sks, hostID) → 私钥丢失
```

**修复**：在 `cleanPeerstore` 中跳过 host 自身：

```go
// skip self — host ID is in Peers() (via AddPrivKey) but Connectedness returns NotConnected
if pid == bootres.Host.ID() || isBootstrapPeer(pid) {
    continue
}
```

### 7.2 GossipSub over Relay 需要 Vendor Patch

`go-libp2p-pubsub v0.12.0` 在处理 `network.Limited` 连接时存在 bug，导致通过 Relay 传输的 GossipSub 消息无法正确路由。

需要的补丁（`p2p/host/pubsub/pubsub.go`）：

1. **`handlePendingPeers`**：对 `Limited` 连接不跳过，允许通过 Relay 加入 Gossip mesh
2. **`handleNewPeer`**：注册 `Limited` 连接的协议支持
3. **`handleDeadPeers`**：正确处理 `Limited` 断连事件

要求通信双方都打补丁才能正常工作。

### 7.3 公共 Relay 的 128KB 限制

公共中继节点通常实施速率限制：每条中继电路 128KB / 2 分钟。当 GossipSub 消息量较大时可能触发该限制。

建议对策：

- 优先尝试直连，中继仅作为兜底
- 大消息拆分为多个小消息分开发送
- 中继电路分离，不同消息走不同电路

### 7.4 libp2p 地址格式导致的 YAML 解析问题

在 initialDiscoveryV6 阶段，HTTP Tracker 返回的 multiaddr 字符串可能包含 `ip4` 和 `tcp` 等格式，但字段名映射容易出错。使用 IPIP-417 扁平格式（`ID` 和 `Addrs` 直接在 provider 层级）避免嵌套解析问题。

## 8. 性能数据

### 网络带宽

| 场景 | 值 |
|------|-----|
| P2P 稳态带宽 | ~1 KB/s |
| P2P 峰值带宽 | ~3 KB/s |
| GossipSub + yamux 背景流量 | ~53 B/s |
| 基础 TCP 连接开销 | ~1 KB/连接 |

### 发现延迟

| 机制 | 正常情况 | 偶发情况 |
|------|----------|----------|
| GossipSub 发现 | <1 min | 2-3 min |
| HTTP closest/peers 单次查询 | 1-2 s | 3-5 s |
| PeerDB 重连优化 | 15s 内恢复 | 依赖记录有效 |

### 周期性任务

| 任务 | 间隔 |
|------|------|
| PeerGossip 发布 | 60s |
| HTTP closest/peers 查询 | 20s（首次）/ 60s（后续）|
| PeerDB 重连 | 15s |
| PeerDB 清理 | 5 min |
| 中继 Ping | 30s |
| 中继重新 Reserve | 60s |
| 地址推送 | 30s |
| Peerstore 持久化保存 | 5 min |

### 移动端能耗估算

| 事件 | 能耗 | 频率 | 分摊 |
|------|------|------|------|
| 无线电尾随 | ~200 mJ/次 | 每 ~12s | ~16 mJ/min |
| GossipSub heartbeat | ~10 mJ/次 | 每 60s | ~10 mJ/min |
| FindPeers 突发 | ~500 mJ/次 | 每 300s | ~1.6 mJ/min |
| **合计** | | | **~28 mJ/min** |

## 9. 总结

### 关键设计决策

1. **弃用 DHT 作为主要发现机制**：DHT 的 UDP 依赖和启动延迟在 NAT 环境中不可靠，改用 GossipSub + HTTP Tracker 的组合方案
2. **三层发现流水线**：GossipSub（实时）→ HTTP closest/peers（扫描）→ PeerDB（持久化），任意一层失效仍有后备
3. **纯 TCP 传输栈**：排除 QUIC/WebRTC 等 UDP 传输，降低网络复杂度和对 UDP 的依赖
4. **移动与桌面双模式**：移动端减少中继数量、加快发现频率、关闭 DHT 以降低功耗

### 适用场景

- NAT 内部的私有集群，节点数在数十到数百规模
- 对 UDP 有限制的网络环境（容器、云环境、企业网络）
- 需要快速发现和重连，但对全局 DHT 不敏感的场景
- 移动端或功耗受限设备

### 已知限制

- HTTP Tracker 的 PUT 写入在所有公共端点上均不可用，如需写入需要自建 Tracker 服务
- GossipSub over Relay 需要双方都打 vendor 补丁
- Public Relay 有 128KB/2min 的速率限制

### 替代方案展望

如果未来需要更可靠的 HTTP 路由写入，可以：

- 自建 boxo delegate server 并启用 ProvideBitswap
- 使用 IPFS Cluster 等私有 DHT 方案
- 等待未来 IPIP 对写操作的正式标准化
