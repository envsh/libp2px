# pbtunnel — tunnel/1.0 协议

## 架构

```
client (A)          server (B)          target TCP
   │                    │                   │
   │── OpenStream() ──→ │                   │
   │                    │── TCP dial ──────→ │
   │                    │←──── connected ─── │
   │  ◄═════ manual buf.copy ═══►  ◄══ manual buf.copy ═══► │
```

## 数据流向与统计

| 方向 | 循环 | 统计 |
|---|---|---|
| conn → s | `conn.Read → s.Write` | `Stats.BytesSent` |
| s → conn | `s.Read → conn.Write` | `Stats.BytesRecv` |

## 退出协调

三个 goroutine：

- goroutine1: `conn → s`（TCP 侧 5min 读超时）
- goroutine2: `s → conn`（p2p 侧无超时）
- goroutine3: `<-ctx.Done()` → `s.Close()`（sync.Once 防重复 panic）

任意方向 error → `cancel()` → goroutine3 唤醒 → `s.Close()` → 另一方向 Read 返回 EOF → 退出

## 防泄漏矩阵

| 场景 | 防护 |
|---|---|
| TCP 半开无数据 | `SetReadDeadline(5min)` + `ctx.Err()` 检查 |
| p2p 对端断开 | `s.Read` 返回 EOF |
| TCP 端断开 | `conn.Read` 返回 err |
| 重复 `s.Close()` | `sync.Once` |
| goroutine 永久阻塞 | `cancel()` 双向通知 |

## 配置

```go
func SetPort(port int)       // 默认 9229，拼为 127.0.0.1:port
func SetTarget(addr string)  // 完整目标地址，优先级高于 SetPort
```

## API

```go
func Dial(peerID string, ctx ...context.Context) (network.Stream, error)

// 全局统计
Stats.BytesSent
Stats.BytesRecv
Stats.ConnSeq   // 单调递增连接序列号
```

## 日志

```
[pbtunnel] conn=7 closed: sent=10240 recv=5120
```
