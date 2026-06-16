# RelayFile — 分块流式文件传输设计

## 1. 背景

在基于 libp2p Circuit Relay v2 的全 NAT 网络中，节点之间只能通过中继建立 `network.Limited` 连接。需要一种**低复杂度、不引入新依赖**的文件传输方案，支持：

- 50MB~1GB 级文件在**两个已知节点**之间传输
- Circuit Relay 断连后**自动续传**（最多丢一个 ACK 窗口）
- 进度反馈给调用方
- 并发传输互不干扰

### 为什么不选 bitswap

| | RelayFile（本方案） | bitswap |
|---|---|---|
| 设计目标 | 两个已知节点间的管道传输 | 内容寻址的多节点块交换 |
| 依赖增量 | **0**（boxo 已有但不需引入） | boxo + unixfs + blockstore + merkledag |
| 二进制增量 | ~0 | ~3-5MB（bitswap + unixfs 层） |
| 每块 RTT | 1（DATA→ACK） | 2-3（HAVE→WANT→BLOCK） |
| 断线续传 | ACK 窗口边界续传 | session 无续传语义 |
| 额外消息 | 无 | wantlist 广播/cancel/have/dont_have |
| 概念负担 | 帧类型+序列号 | CID/MerkleDAG/UnixFS/wantlist/session |

bitswap 的复杂度都花在**内容发现**（"谁有这个块？"）上，两个已知节点间传整文件时 95% 的功能用不上。

---

## 2. 协议设计

### Protocol ID

`/d2hub/file/1.0`

遵循现有命名惯例（`/d2hub/push/1.0`、`/d2hub/pubsub/1.0`）。

### 帧格式

统一 TLV（Type-Length-Value），控制帧和数据帧共用同一帧结构：

```
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                       Payload Length                          |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|   Frame Type   |           Payload (variable) ...
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
```

- **Payload Length**：4 字节大端 uint32，表示 Type 之后 payload 的字节数（不包含自身和 Type）
- **Frame Type**：1 字节
- **Payload**：根据类型不同，控制帧为 JSON，DATA 帧为二进制

### 帧类型

| 值 | 名称 | 方向 | Payload 格式 | 说明 |
|----|------|------|-------------|------|
| 0x00 | `INIT` | → | JSON | 发送方宣告文件信息 |
| 0x01 | `ACCEPT` | ← | JSON | 接收方确认，含 resume offset |
| 0x02 | `REJECT` | ← | JSON | 接收方拒绝（磁盘满等） |
| 0x03 | `DATA` | → | Binary: `[8 bytes seq BE] [chunk bytes]` | 文件块数据 |
| 0x04 | `ACK` | ← | JSON | 累计确认，声明最高连续 seq |
| 0x05 | `FIN` | → | JSON | 发送方宣告传输结束 |
| 0x06 | `DONE` | ← | JSON | 接收方确认校验通过 |
| 0x07 | `ERROR` | ⇄ | JSON | 任意阶段出错 |
| 0x08 | `CANCEL` | ⇄ | JSON | 任意阶段取消 |

### INIT Payload

```json
{
    "name": "ubuntu-22.04.iso",
    "size": 50000000,
    "chunk_size": 65536,
    "total_chunks": 763,
    "checksum": "sha256:e3b0c44298fc1c149afbf4c8996fb924...",
    "offset": 0
}
```

- `name`：目标文件名（接收方使用 `filepath.Base` 防止路径遍历）
- `size`：文件总字节数
- `chunk_size`：每块字节数，固定 65536（64KiB）
- `total_chunks`：总块数，`ceil(size / chunk_size)`
- `checksum`：格式 `sha256:<hex>`，发送方预先计算整个文件的 SHA-256
- `offset`：断线续传时非零，表示发送方要从第 offset 块开始发

### ACCEPT Payload

```json
{
    "offset": 0
}
```

- `offset`：接收方声明已连续收到 0..offset-1 块，发送方从 offset 开始发送
- 首次传输 `offset=0`；断线续传时接收方根据本地 bitmap 计算最大连续 offset

### ACK Payload

```json
{
    "cs": 15
}
```

- `cs`（ContinuousSeq）：接收方已连续收到 0..cs 全部块（不含空洞）。发送方收到 ACK(cs=15) 后确认 0..15 块已安全抵达。

### FIN / DONE / ERROR / CANCEL Payload

```json
// FIN
{
    "total_chunks": 763,
    "checksum": "sha256:e3b0c44298fc1c149afbf4c8996fb924..."
}

// DONE
{
    "ok": true
}

// ERROR
{
    "reason": "disk full"
}

// CANCEL
{
    "reason": "user cancelled"
}
```

---

## 3. 会话生命周期

```
SENDER                              RECEIVER
  │                                    │
  │── INIT ───────────────────────────►│
  │                                    │  校验 name/size/checksum
  │                                    │  检查磁盘空间
  │◄── ACCEPT ────────────────────────│
  │                                    │
  │   ── DATA(seq=0..7) (pipeline) ──►│  写入 temp 文件 + bitmap
  │◄── ACK(cs=7) ─────────────────────│
  │   ── DATA(seq=8..15) ────────────►│
  │◄── ACK(cs=15) ────────────────────│
  │   ...                              │  ...
  │                                    │
  │   ── DATA(seq=760..762) ─────────►│  最后一批（不满 8 块）
  │◄── ACK(cs=762) ───────────────────│
  │                                    │
  │── FIN ────────────────────────────►│  校验整个文件 checksum
  │◄── DONE ──────────────────────────│  重命名 .part → name
  │                                    │
```

### 批量 ACK 策略

每 **8 块**（64KiB × 8 = 512KiB）确认一次，管道化发送：

```
SENDER 发送窗口：
  [0][1][2][3][4][5][6][7]  ← 流水线发出，不等确认

RECEIVER 处理：
  逐个接收，写入 temp 文件，更新 bitmap
  当 seq % 8 == 7 时：检查 0..seq 全部连续 → 发 ACK(cs=seq)
  如果不连续（有空洞）→ 找出最大连续值 cs → 发 ACK(cs)

SENDER 收到 ACK(cs=seq)：
  丢弃 seq 之前已发数据缓存（如果有）
  继续下发 seq+1..seq+8
```

**效率**：50MB = 800 块，ACK 次数 = ceil(800/8) = 100 次。相比单块 ACK 节省 700 次 RTT。

**安全性**：ACK 只确认**连续区间**，空洞之后的块即使到达也不确认，避免批量 ACK 的核心 bug。

### 断线续传

```
Context: Sender 已发 0..31，收到 ACK(cs=15)，但 ACK(cs=31) 还没到
         此时 relay 断连

重连后:
  1. Sender 打开新的 stream
  2. INIT {offset: 16}  ← 从上次确认的连续点之后开始
  3. Receiver 检查本地 temp 文件:
     - 有 seq 16..31，但 seq=16 可能不完整 → bitmap 重校验
     - 实际连续收到 0..23 → ACCEPT {offset: 24}
  4. Sender 从 seq=24 开始发 DATA
```

---

## 4. 会话状态管理

### FileSession

```go
type fileSession struct {
    mu          sync.Mutex
    id          string            // UUID，全局唯一
    peer        peer.ID
    filename    string            // 最终文件名
    savePath    string            // 完整保存路径
    size        int64
    chunkSize   int
    totalChunks int
    checksum    string            // "sha256:<hex>"
    state       string            // init|transfer|checksum|done|error|cancelled

    // 发送方
    file        *os.File          // 源文件句柄
    curSeq      int               // 下一个要发的 seq

    // 接收方
    tmpFile     *os.File          // .part 临时文件
    bitmap      []uint64          // 位图，标记已收块（seq/64 字）
    tmpPath     string            // .part 路径
    doneBytes   int64             // 已写入字节数

    cancel      context.CancelFunc
}
```

### 全局管理器

```go
var (
    sessions   sync.Map   // sessionID → *fileSession
    recvDir    string     // 接收目录，全局设定
)
```

以 sessionID（UUID）为 key，允许多个 peer 间并发传输，同一 peer 也可同时传输多个文件。

---

## 5. API 设计

### 配置

```go
// SetFileRecvDir 设置文件接收目录。
// 目录不存在时会自动创建。
func SetFileRecvDir(dir string)
```

### 发送

```go
// SendFile 向指定 peer 发送文件，阻塞到完成或出错。
// ctx 超时或取消会发送 CANCEL 帧并清理。
// cb 在每个 ACK 批次到达时触发。
// 返回 sessionID 供后续 CancelFileSession 使用。
func SendFile(ctx context.Context, pid peer.ID, localPath string, cb ProgressFunc) (sessionID string, err error)
```

### 接收

```go
// ReceiveFile 等待指定 peer 发来的文件传输，阻塞到完成或出错。
// 传输由发送方 SendFile 主动发起；ReceiveFile 注册回调后等待匹配 peer 的 session 完成。
// 如果 saveDir 非空则覆盖 SetFileRecvDir 设定的目录。
// 返回 receiver 端 sessionID（与发送方不同）或错误。
func ReceiveFile(ctx context.Context, pid peer.ID, saveDir string, cb ProgressFunc) (sessionID string, err error)
```

### 控制

```go
// CancelFileSession 取消指定 session 的传输。
func CancelFileSession(sessionID string) error
```

### 进度回调

```go
type FileProgress struct {
    SessionID   string    // 会话唯一标识
    PeerID      peer.ID   // 对方 peer ID
    Name        string    // 文件名
    TotalBytes  int64     // 文件总大小
    DoneBytes   int64     // 已确认字节数
    TotalChunks int       // 总块数
    DoneChunks  int       // 已确认块数
    State       string    // 当前状态: init|transfer|checksum|done|error|cancelled
    Err         error     // 仅在 State=error 时非 nil
}

type ProgressFunc func(FileProgress)
```

---

## 6. 差错处理

| 阶段 | 错误 | 处理方式 |
|------|------|----------|
| INIT | 目标文件已存在 | `ACCEPT`（覆盖）/ `REJECT`（用户决策） |
| INIT | `name` 含路径遍历 | `filepath.Base()` 清洗后拼接 |
| INIT | 接收目录不可写 | `REJECT` |
| INIT | 磁盘空间不足 | 预估 `size - offset`，不足则 `REJECT` |
| INIT | stream 超时/断连 | Sender 返回 error |
| INIT | 对方不支持协议 | `NewStream` 返回 "protocol not supported" |
| DATA | Receiver 写入失败 | 发 `ERROR`，双方清理 |
| DATA | Sender 读源文件失败 | 发 `CANCEL`，双方清理 |
| DATA | 块号不连续 | Receiver 标记 bitmap，`ACK` 仅确认连续区间 |
| DATA | 重复块 | bitmap 检测，跳过写入 |
| DATA | 连接断连 | 双方清理 session，下次 INIT offset 续传 |
| ACK | 超时未收到 | Sender 发 `CANCEL`，关闭 stream |
| FIN | checksum 不匹配 | 发 `ERROR`，删除 temp 文件 |
| FIN | 缺块（total_chunks > 收到的连续块数） | bitmap 检测，发 `ERROR` |
| DONE | 重命名 .part 失败（被占用） | 发 `ERROR` |
| 任意 | context 取消 | 发 `CANCEL`，清理 temp 文件 |
| 任意 | 同时收到 `CANCEL` | 无害，一方 close stream 后另一方读失败 |

### 超时

| 超时 | 值 | 触发 |
|------|----|------|
| DATA 帧间隔超时 | 30s | Sender 等 ACK / Receiver 等 DATA |
| 总传输超时 | 由调用方 ctx 控制 | `context.DeadlineExceeded` |
| stream 空闲超时 | 由 libp2p 的 yamux keepalive 控制 | ~30s |

---

## 7. 安全

### 路径遍历

```go
safeName := filepath.Base(init.Name)
fullPath := filepath.Join(saveDir, safeName)
```

接收方始终使用 `filepath.Base()` 清洗文件名，确保 `../../etc/passwd` 等恶意文件名不会逃逸到指定目录之外。

### Temp 文件策略

- 接收阶段写入 `<safeName>.relayfile.part`（与目标文件同目录）
- 成功完成 FIN+DONE 后 `os.Rename(.part, safeName)` 原子重命名
- 失败/取消时删除 `.part` 文件

---

## 8. 并发模型

```
INCOMING STREAM (libp2p goroutine pool)
    │
    ▼
HandleFileStream(s network.Stream)
    │
    ├─ frameRead(INIT)
    ├─ 创建 fileSession, store in sync.Map
    ├─ frameWrite(ACCEPT)
    ├─ 进入接收循环 (goroutine):
    │      frameRead(DATA) → write + bitmap → frameWrite(ACK)
    │      frameRead(FIN) → checksum → frameWrite(DONE)
    │      出错 → frameWrite(ERROR/CANCEL) → cleanup
    └─ defer: 从 sync.Map 删除 session, close stream

OUTGOING CALL (调用方 goroutine)
    │
    ▼
SendFile(ctx, pid, path, cb)
    │
    ├─ OpenStream → frameWrite(INIT)
    ├─ frameRead(ACCEPT)
    ├─ 进入发送循环:
    │      read file → frameWrite(DATA × 8)
    │      frameRead(ACK) → cb()
    │      repeat
    ├─ frameWrite(FIN) → frameRead(DONE) → cb(done)
    └─ 出错 → frameWrite(CANCEL) → cleanup
```

- libp2p 为每个 incoming stream 自动分配 goroutine
- 每个 session 独占一个 stream，互不干扰
- `sync.Map` 管理全局 session 状态
- 同一 peer 可同时发送/接收多个文件

---

## 9. 实现文件

### 新增：`p2put/relayfile.go`

全部代码在此一个文件中：

| 段 | 估算行数 | 内容 |
|----|---------|------|
| 帧读写 | 40 | `frameWrite` / `frameRead` |
| INIT 发送 | 15 | 计算 checksum + 构造 JSON |
| 发送循环 | 40 | 流水线 8 块 + 等 ACK |
| 接收循环 | 40 | bitmap + 写入 + 批量 ACK |
| FIN 校验 | 20 | SHA-256 对比 |
| session 管理 | 30 | sync.Map + cancel |
| API 函数 | 40 | SendFile / ReceiveFile / CancelFileSession |
| handler | 20 | HandleFileStream + init() |
| **合计** | **~250** | |

### 无需修改的文件

- `libp2p_bs.go` — `replayProtocols` 自动重放 `init()` 注册的 handler
- `user_protocols.go` — 自动支持
- 其他所有已有文件

### 兼容性

- 无第三方二进制依赖
- 无 build tag 要求
- 无 CGO
- 与 Android/iOS 编译兼容
