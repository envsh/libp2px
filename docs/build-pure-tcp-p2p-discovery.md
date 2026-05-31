# 构建纯 TCP P2P 发现与通信系统

## 1. 背景

在构建一个基于 libp2p 的 P2P 通信系统时，最常见的发现方式是依赖 Kademlia DHT。DHT 使用 UDP，在公网环境下工作良好，但在实际部署中遇到几个问题：

- **UDP 被限制**：部分云环境或容器网络对 UDP 有速率限制甚至直接封锁
- **DHT 启动慢**：首次启动需要连接全局 bootstrap 节点并等待路由表就绪，通常需要 30-60 秒
- **NAT 穿透不稳定**：即使在公网节点上，DHT 的 UDP 端口也可能被防火墙拦截
- **DHT 维护开销**：周期性刷新路由表，即使没有查询需求也会产生流量

目标很明确：构建一个**纯 TCP/WebSocket** 的 P2P 通信系统，不依赖 DHT 做发现，而是通过 `/d2hub/push/1.0` 推拉协议、HTTP 路由查询和持久化 PeerDB 三层机制实现自包含的节点发现。

技术栈：go-libp2p v0.36.2、go-libp2p-pubsub v0.12.0、go-libp2p-kad-dht v0.26.0（仅 Client Mode）。

### 1.1 核心挑战：全 NAT 场景下的节点发现

本系统的关键部署场景是**全 NAT 环境**——集群中所有节点都位于 NAT 之后，没有任何一个节点拥有公网 IP。所有节点之间的连接必须通过公共中继（Relay）建立，每条连接都是 `network.Limited`（受限连接）。

典型拓扑：

```
     公共 Relay (1.2.3.4:4001)
       /    |    |    |    \
      A     B    C    D    E    F
     NAT   NAT  NAT  NAT  NAT  NAT
```

6 个节点全在 NAT 内部，各自通过同一个公共 Relay 建立中继电路。节点 A 到 B 的通信路径是 `A → Relay → B`，而不是 `A ↔ B` 直连。在这个拓扑下，节点发现的难点在于：

1. **没有广播通道**：没有公网 IP 就无法被"主动找到"，新节点必须通过某种方式"自报家门"
2. **所有连接都是 Limited**：libp2p 的许多子系统（包括 GossipSub mesh）默认跳过 Limited 连接
3. **Relay 带宽有限**：公共 Relay 通常有 128KB/2min 的速率限制，不能用作持续广播通道

在改用 `/d2hub/push/1.0` 协议之前，我们尝试过基于 GossipSub 的 `PeerGossip` 方案，但在全 NAT 拓扑下暴露了根本性问题：

| 问题 | 原因 |
|------|------|
| **GossipSub 跳过 Limited 连接** | `go-libp2p-pubsub` 默认忽略 `network.Limited`，公告不会通过 Relay 传播 |
| **vendor patch 不可行** | 即使自己打补丁，也要求通信双方都打，对新加入的节点不兼容 |
| **Mesh 无法维持** | Limited 连接不稳定，Gossip mesh 在心跳间隔内频繁分裂和重建 |
| **冷启动慢** | 新节点加入 mesh 需要 30-60s 收敛，且无法保证收敛到所有节点 |

`/d2hub/push/1.0` 协议的设计目标就是解决上述问题：用**点对点 stream 请求/响应**替代**多播 mesh**，每个 Limited 连接上独立完成 peer 交换，不依赖补丁，不依赖 mesh。

## 2. 整体架构

系统分为四层：

```
网络层    TCP / WebSocket / Relay / HolePunching
消息层    GossipSub (应用层 Pub/Sub)
发现层    /d2hub/push/1.0 ↔ HTTP closest/peers ↔ PeerDB reconnection
应用层    REST API / Event Bus / Topic PubSub
```

- **网络层**：负责底层连接建立，通过 libp2p host 管理 TCP/WebSocket 传输、Relay 连接和 Hole Punching
- **消息层**：GossipSub 仅负责应用层 Pub/Sub（类似 Reddit），**不参与节点发现**
- **发现层**：三级发现流水线，从最快到最慢提供不同可靠性的节点发现
- **应用层**：REST API 管理面板、事件总线、Topic PubSub 消息

### 2.1 启动流程

```
Bootstrap() → 加载私钥 → 创建 libp2p host → 初始化 AutoRelay
→ 创建 RelayPool → 启动 relayPoolManager(K=3)
→ 创建 PeerDB → 创建 GossipSub → 订阅 HubName
→ 启动发现流水线
```

所有组件共享一个全局 `BootNode` 实例：

```go
type BootNode struct {
    Host      host.Host
    PeerID    peer.ID
    AddrMgr   *AddrManager
    PubkeyHex string
    BootTime  time.Duration
    RelayPool *RelayPool
    PeerDB    *PeerDB
    Bwc       metrics.Reporter
}
```

- `RelayPool`：双阶段中继池（probation/main），EMA 打分，circuit breaker
- `AddrMgr`：三源地址管理器（local/relayCircuit/relayVouch）
- `PeerDB`：节点记录存储，TTL 600 分钟，push + ping 双续期
- `Bwc`：带宽统计

启动流程细节：

1. `Bootstrap` 创建 libp2p host，配置 AutoRelay，初始化 `RelayPool` 并填入 6 个静态中继
2. `relayPoolManager(ctx, h, rp, 3)` 以 60s 间隔管理 K=3 个中继：首次 SelectN(3)→connect→reserve，后续只做 reserve 续约 + 断开 circuitOpen 的 relay
3. PeerDB 持久化 + `reconnectFromPeerDB` 以 15s 间隔扫描 PeerDB，重连不在 topic 中的节点
4. `/d2hub/push/1.0` 协议通过 `myEventSuber` 事件驱动（EvtPeerIdentificationCompleted 触发 push），30s 定时全量推送
5. HTTP Tracker 作为辅助发现，60s 查询最远节点

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

中继管理由 `RelayPool` + `relayPoolManager` 两层构成。

#### RelayPool — 双阶段评分池

`RelayPool` 是一个带评分的双阶段中继池（probation → main），核心指标：

| 维度 | 权重 | 说明 |
|------|------|------|
| `Success` | 0.35 | 成功率，EMA 衰减（α=0.3），rate limit 衰减更急（α=0.5） |
| `Latency` | 0.20 | RTT 评分，指数衰减 |
| `DataLimit` | 0.15 | 中继数据量上限评分 |
| `DurationLimit` | 0.15 | 中继时长上限评分 |
| `TTL` | 0.10 | 预约剩余时间评分 |
| `Uptime` | 0.05 | 连续在线时间评分 |

选择策略：90% 概率按分数加权轮盘选（roulette wheel），10% 概率随机探索。

故障处理：

| 错误类型 | 影响 | 触发条件 |
|----------|------|---------|
| `errOK` | 提升分数，第 1 次 promotion → main | RecordResult(pid, nil) |
| `errRateLimited` | 分数衰减 + rateLimitHits++ | network.ErrReset / RESERVATION_REFUSED |
| `errFailed` | 分数衰减 + consecutiveFails++ | 其他错误 |
| 3 次连续失败 | main → probation 降级 | 仅在 main 中触发 |
| 5 次连续失败 | circuit breaker 打开 | 任意 tier 触发 |

保护机制：

- **Probation**：FIFO 淘汰，上限 10 个
- **Main**：3 轮保护（最近 5min 活跃、top-20% 分数、top-10 在线时间）+ protected map
- 静态中继通过 `Protect()` 标记为永久保护，不会被 main 淘汰

容量控制：`highWater=50 / lowWater=40`，超限时批量 prune，淘汰配额对半分配到 probation（FIFO）和 main（保护轮 + 最低分），如果 main 淘汰不够则 spill-back 到 probation。

#### relayPoolManager — 常驻管理协程

```
relayPoolManager(ctx, h, rp, 3)  // K=3

─ 首次启动 ──────────────────────────────────
  SelectN(3) → [A, B, C]
  AddManaged(A, B, C)
  connect A + reserve A → SetRelayVouch + SetReservationTTL + RecordResult
  connect B + reserve B → ...
  connect C + reserve C → ...

─ 每 60s ──────────────────────────────────
  for pid in ListManaged():  // 仅遍历 A, B, C
    if NotConnected → RemoveManaged(continue)
    if circuitOpen   → ClosePeer + RemoveManaged(continue)
    正常 → reserve + SetRelayVouch + SetReservationTTL + RecordResult(pid, nil)
    reserve 失败 → RecordResult(pid, err)

  if len(ListManaged()) < 3:
    need = 3 - len
    SelectN(need) → AddManaged → connect + reserve
```

关键行为：

- **只管理列表中的 3 个**，不轮询全量 pool
- **不主动踢人**（避免震荡），只移除断连或熔断的 relay，再补充新人
- 新人替补后**不设** `ConnManager.Protect`（静态中继在初始化时已 Protect）
- 所有 connect/reserve 结果都反馈到 `RecordResult`，驱动 score/circuit breaker 更新

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

### 第一级：`/d2hub/push/1.0` 推拉协议（最快，秒级）

在全 NAT 拓扑下，每个节点都只有中继电路：

```
     ┌───────────────── Relay ─────────────────┐
     │   /d2hub/push/1.0  stream               │
     │   A───→Relay───→B   A───→Relay───→C    │
     │   A───→Relay───→D   A───→Relay───→E    │
     │   A───→Relay───→F                        │
     └──────────────────────────────────────────┘

  A 向 B 发起 push:
    1. A → Relay → B:  "我 A 的地址是 /ip4/.../tcp/4001"
    2. B → Relay → A:  "我 PeerDB 里有 C, D, E, F，给你"
    3. A 收到后 Update PeerDB(C, D, E, F)

  关键: A 和 B 之间是 Limited 连接，但 push stream 不受影响
      每条 Limited 连接上都可以独立完成双向 peer 交换
```

每个节点通过中继连接彼此。节点 A 可以同时向 B、C、D、E、F 发起 push 交换，每个方向都是独立的 stream 请求/响应。不像 GossipSub 需要 mesh 拓扑才能广播消息，push 协议的发现范围等于**节点已建立的中继连接数**——连接了 K 个节点，就能通过 push 获知这 K 个节点的 PeerDB 内容。

当节点 A 通过事件（`EvtPeerIdentificationCompleted`）或 HTTP Tracker 发现节点 B 后，立即或周期性地向 B 推送自身地址。

**协议格式**（JSON over libp2p stream）：

```json
// 请求（A → B）：A 推送自身地址
{"peers":[{"id":"12D3KooW...","addrs":["/ip4/.../tcp/4001"]}],"ts":1700000000000}

// 响应（B → A）：B 返回 PeerDB 中所有已知节点
{"peers":[{"id":"12D3KooW...","addrs":["/ip4/.../tcp/4001"]},...],"ts":1700000000001}
```

**核心逻辑**：

```
请求方 A:
  PushToPeer(B):
    1. 用 AllowLimitedConn 打开 /d2hub/push/1.0 stream
    2. 写入 A 的地址 → CloseWrite
    3. 读取 B 的响应 → 逐个 Update PeerDB
    4. 返回

处理方 B:
  HandlePushStream:
    1. 读取 A 的请求 → Update PeerDB(A)
    2. 调取 PeerDB.List() → 组装响应
    3. 写回 → Close
```

**触发方式**：

| 触发点 | 频率 | 说明 |
|--------|------|------|
| `myEventSuber` | 事件驱动 | 识别到支持 push/1.0 的新 peer 后立即 push |
| `pushMyAddrsToPeers` | 30s 定时 | 向所有已连接 peer 推送，接收方用响应更新 PeerDB |
| HTTP Tracker 发现后 | 每次发现 | `DiscoveryV6` 中成功连接后调用 `PushToPeer` |

**Limited 连接适配**：全程使用 `AllowLimitedConn`，即使只有中继连接也能正常交换 peer 信息。

**为什么比 PeerGossip 更适合全 Limited 场景**：

| 维度 | PeerGossip（旧） | `/d2hub/push/1.0`（当前） |
|------|-----------------|--------------------------|
| 传输方式 | GossipSub mesh（多播） | 点对点 stream（单播） |
| Limited 连接 | 需 vendor patch（双方打补丁） | 原生支持 `AllowLimitedConn` |
| mesh 维护 | 需心跳维护 mesh 拓扑 | 无 mesh，连上就推 |
| 连接数 | mesh 内全连接 | 只连 K=3 个中继 + push 目标 |
| 故障传导 | 一个节点断连可能导致 mesh 分裂 | 断连只影响该节点自身 |
| 冷启动 | 需等待 mesh 收敛（30-60s） | 连接成功后立即 push，秒级发现 |

在全 NAT 场景下，所有节点都只有 Limited 连接（中继电路），GossipSub 的 mesh 拓扑无法正常工作——`network.Limited` 连接在 libp2p-pubsub 中默认被跳过，需要双方打 vendor 补丁才能加入 mesh。而 push 协议只需一条 stream 即可完成双向 peer 交换，不依赖 mesh，不依赖补丁。

**PeerGossip 失败路径模拟**（6 个全 NAT 节点，无 vendor patch）：

```
时间 t=0:  A 上线 → 通过 Relay 连接到 B, C
           A 在 GossipSub topic 上发布 PeerAnnounce
           B, C 的 pubsub 收到吗?
           → 不: network.Limited 被 pubsub.handleNewPeer 跳过
           → A 的公告停留在 libp2p 层，pubsub 层看不到

时间 t=10s: GossipSub heartbeat
            A 试图加入 mesh → 发现 Limited 连接被拒绝
            A 的 mesh 大小为 0，公告无人转发

时间 t=60s: D 上线 → 连接 Relay → 谁发现了 D?
            → HTTP Tracker 可能返回 D，但 D 与 A/B/C 之间没有 Gossip mesh
            → D 的 PeerAnnounce 同样无人收到
            → 发现完全依赖 HTTP Tracker 的 60s 轮询，GossipSub 发现失效

时间 t=∞:  6 个节点全部在线，但 GossipSub mesh 为 0
           没有任何 PeerAnnounce 被传递
           节点发现的唯一通道是 HTTP Tracker 的 closest/peers
```

**push 协议的对应路径**：

```
时间 t=0:  A 上线 → 通过 Relay 连接到 B, C
           myEventSuber 检测到新连接 → PushToPeer(B), PushToPeer(C)
           B 收到 A 的地址 → Update PeerDB(A)
           B 返回 PeerDB 内容 → A 获得 B 知道的其他节点
           C 同理 → A 在 5s 内获知了 B 和 C 已知的所有节点

时间 t=10s: A 的 30s 定时 push → 再次推送给 B, C
            B, C 响应 → 增量更新 PeerDB

时间 t=60s: D 上线 → 连接 Relay
            A/B/C 的 15s reconnectFromPeerDB 扫描到 D
            → connect(D) → PushToPeer(D) → D 加入全部 PeerDB
            D 被 3 个节点同时发现，收敛时间 < 15s
```

关键区别：push 协议把**一个有限广播问题**（GossipSub mesh 在全 NAT 下不可用）转化成了**多个单播交换问题**（每个 Limited 连接上独立交换）。只要节点至少有一条中继连接到达集群中的任意节点，push 就能工作。而 GossipSub 需要至少一个完整的 mesh 拓扑才能广播消息，在全 NAT 下这个前提就不成立。

**保护**：1MB 读取上限、10s 超时、goroutine 隔离防阻塞。

### 第二级：HTTP Tracker 随机探测（1-2s/次）

当 push 协议尚未覆盖所有节点时（例如新节点刚上线），通过 HTTP Tracker 的 `closest/peers` 端点进行随机探测。

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

1. 首次间隔 20s，后续 60s
2. 解析返回的 peer ID 和地址
3. 跳过自身和已连接的节点
4. 调用 `IsGoodPeerAddr` 预过滤（仅保留公网 TCP 地址）
5. 连接上限 20 个
6. 连接成功后调用 `PushToPeer` 触发 push 交换
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
    PushToPeer(ctx, pid)  // push 交换，双方互写 PeerDB
    time.Sleep(5 * time.Second)
}
```

### 第三级：PeerDB 重连（15s/次）

PeerDB 中记录了历史上通过 push 协议或 HTTP Tracker 发现过的所有节点。当网络波动导致断连时，通过 PeerDB 重连可以快速恢复连接。

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
        res := <-ping.Ping(ctx, bootres.Host, r.PeerID)
        if res.Error == nil {
            bootres.PeerDB.Update(r.PeerID, r.Addrs)  // 续期 SeenAt
        }
        time.Sleep(5 * time.Second)
    }
}
```

关键逻辑：

- `ListPeers` 来自 GossipSub topic（`getOrSubscribeTopic(HubName)`），已在 topic 中的节点跳过
- `WithAllowLimitedConn` 允许通过 Relay 建立受限连接
- 连接成功后执行 Ping 验证可达性，成功则更新 PeerDB.**续期** SeenAt（600 分钟 TTL 重新计时）
- 连接间间隔 5s，每次最多处理到 20 个连接
- 15s 触发一次

### 桌面模式：DHT 发现

桌面模式下（非移动）使用 Kademlia DHT 作为辅助发现手段，通过 `routing.RoutingDiscovery.FindPeers()` 查询 `HubName` 话题相关的节点。

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
| 辅助发现 | push + PeerDB | push + PeerDB |
| 连接策略 | connfixer（分批连接 + 直连升级） | 直接连接 |
| 核心发现间隔 | 3s 扫描 + 120s 完整循环 | 20s/60s 循环 |

## 5. PeerDB 持久化

PeerDB 是一个内存中的节点记录存储，带 TTL 过期清理，负责记录所有被发现的节点地址。

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
func (db *PeerDB) Get(pid peer.ID) (*PeerRecord, bool)
func (db *PeerDB) List() []PeerRecord

func (db *PeerDB) cleanup(ctx context.Context)
```

- **TTL 设为 600 分钟**（10 小时），超出时间的记录在 `List()` 和 `Get()` 中跳过
- 后台清理 goroutine 每 5 分钟运行一次，移除过期记录
- 写入来源：
  - push 协议接收端：`HandlePushStream` 收到请求后 Update 发起方
  - push 协议发起端：`PushToPeer` 收到响应后 Update 响应方
  - `reconnectFromPeerDB`：ping 成功后更新 SeenAt 续期
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

**双续期保障**：TTL 虽长，但如果节点一直在线，有两种机制持续续期：

1. **push 交换**：30s 定时 push 到每个已连接 peer，对方收到后 Update 发起方（相当于每 30s 续一期）
2. **PeerDB 重连**：`reconnectFromPeerDB` 连接不在 topic 中的节点，成功后 ping 并 Update，续期 SeenAt

即使 push 偶尔失败，15s 后的重连周期也会接上续期，不会让活跃节点过期。断连超过 600 分钟的节点自动清理，避免 PeerDB 无限膨胀。

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

### 7.2 GossipSub over Relay 需要 Vendor Patch（已解决：改用 push 协议）

旧方案使用 `PeerGossip` 基于 GossipSub 进行节点发现，但 `go-libp2p-pubsub v0.12.0` 在处理 `network.Limited` 连接时存在 bug，导致通过 Relay 传输的 GossipSub 消息无法正确路由，且要求通信双方都打补丁才能正常工作。

已通过 `/d2hub/push/1.0` 协议替代 PeerGossip：push 协议是简单的 stream 请求/响应模型，原生支持 `AllowLimitedConn`，不需要任何 vendor 补丁，即使所有节点都是 Limited 连接也能正常工作。

### 7.3 公共 Relay 的 128KB 限制

公共中继节点通常实施速率限制：每条中继电路 128KB / 2 分钟。当 GossipSub 应用层消息量较大时可能触发该限制。

两条缓解措施：

1. **中继池容量控制**：`RelayPool` 高水位 50、低水位 40，超过则按分数淘汰，不会在单个中继上堆积过多流量
2. **Managed 中继**：`relayPoolManager` 只维护 3 个活跃中继的中继电路，将 reserve/health 流量控制在 3 个中继上，避免全池的 reserve renew 风暴

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
| `/d2hub/push/1.0` 推拉发现 | 秒级 (~5s) | 依赖连接就绪 |
| HTTP closest/peers 单次查询 | 1-2 s | 3-5 s |
| PeerDB 重连优化 | 15s 内恢复 | 依赖记录有效 |

### 周期性任务

| 任务 | 间隔 |
|------|------|
| `/d2hub/push/1.0` 推送 | 30s 定时 + 事件驱动 |
| HTTP closest/peers 查询 | 20s（首次）/ 60s（后续）|
| PeerDB 重连 | 15s |
| PeerDB 清理 | 5 min |
| relayPoolManager reserve 续约 | 60s（仅 managed 3 个）|
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

1. **弃用 DHT 作为主要发现机制**：DHT 的 UDP 依赖和启动延迟在 NAT 环境中不可靠，改用 push 协议 + HTTP Tracker 的组合方案
2. **三层发现流水线**：`/d2hub/push/1.0`（秒级推拉）→ HTTP closest/peers（定时扫描）→ PeerDB 重连（断线恢复），任意一层失效仍有后备
3. **`/d2hub/push/1.0` 替代 PeerGossip**：push 协议通过单播 stream 交换 peer 信息，原生支持 Limited 连接，无需 vendor 补丁；在全 NAT 场景下即使所有节点都是 Limited 连接也能正常运作
4. **纯 TCP 传输栈**：排除 QUIC/WebRTC 等 UDP 传输，降低网络复杂度和对 UDP 的依赖
5. **移动与桌面双模式**：移动端减少中继数量、加快发现频率、关闭 DHT 以降低功耗

### 关键发现

在全 NAT 场景下，`/d2hub/push/1.0` 协议的发现收敛时间远优于 PeerGossip（GossipSub）：

| 场景 | PeerGossip（旧） | `/d2hub/push/1.0` |
|------|-----------------|-------------------|
| **单节点首次加入** | 无法收敛（Limited 被跳过） | ~5s（事件驱动 + PushToPeer） |
| **全集群重启（6 节点）** | 无法收敛（mesh=0） | <15s（30s 定时 push + 15s reconnectFromPeerDB） |
| **新节点上线，已有 K 个节点在线** | 无法收敛 | <5s（K 个节点的事件驱动 push） |
| **节点断连后恢复** | 取决于 mesh 重建时间（30-60s） | <5s（30s 定时 push 覆盖） |
| **全节点覆盖（6/6 互知）** | 永远达不到（mesh 为 0） | ~30s（30s 定时 push 完成全部交换） |

核心结论：在全 NAT 拓扑下，PeerGossip 的**发现能力为零**（GossipSub mesh 无法在 Limited 连接上建立），而 push 协议的**发现能力等于节点已建立的中继连接数**。每一条 Limited 连接都是一条独立的 peer 交换通道，节点的连接数越多，发现覆盖率越高。

### 适用场景

- NAT 内部的私有集群，节点数在数十到数百规模
- 对 UDP 有限制的网络环境（容器、云环境、企业网络）
- 需要快速发现和重连，但对全局 DHT 不敏感的场景
- 移动端或功耗受限设备

### 已知限制

- HTTP Tracker 的 PUT 写入在所有公共端点上均不可用，如需写入需要自建 Tracker 服务
- Public Relay 有 128KB/2min 的速率限制
- `push/1.0` 协议目前只支持推送自身地址，不支持增量推送或批量同步

### 替代方案展望

如果未来需要更可靠的 HTTP 路由写入，可以：

- 自建 boxo delegate server 并启用 ProvideBitswap
- 使用 IPFS Cluster 等私有 DHT 方案
- 等待未来 IPIP 对写操作的正式标准化
