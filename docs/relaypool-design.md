# RelayPool 中继池设计

## 1. 背景

在基于 libp2p 的纯 TCP 系统中，所有节点间连接都依赖 Relay（中继）：
- 节点可能处于完全 NAT 环境，无法建立直连
- 所有连接都是 `Limited` 类型，经由 relay 转发
- 有一组静态 Relay 节点（如 4 个），但它们的质量可能不均：
  - 部分 relay 可能不可用或超时
  - 部分 relay 可能触发限流（`RESERVATION_REFUSED` / `network.ErrReset`）
  - 部分 relay 的连接延迟和带宽差异大

需要一个**中继管理池**来自动维护、评估和选择最优的中继节点。

### 设计目标

- **动态评估**：根据连接成功率、延迟、限流反馈等信息动态调整每个 relay 的分数
- **容错自动恢复**：不可用的 relay 自动降级/熔断，恢复后重新加入
- **最优选择**：每次需要中继时，以概率加权方式选中最优节点
- **容量控制**：限制池大小，防止无限增长

---

## 2. 两阶段池架构

RelayPool 使用 **SLRU（Segmented Least Recently Used）风格**的两阶段架构：

```
 ┌──────────────────────────────────────────────────┐
 │                   RelayPool                      │
 │  ┌────────────────────┐  ┌────────────────────┐  │
 │  │   Probation（观察）  │→→│    Main（主池）      │  │
 │  │    FIFO 淘汰        │  │ 分数+保护轮淘汰     │  │
 │  │    初始 success=0.70│  │  promotion↑demotion↓│  │
 │  └────────────────────┘  └────────────────────┘  │
 │                                                  │
 │  容量: total ≤ highWater(50) → prune → lowWater(40)│
 └──────────────────────────────────────────────────┘
```

### Probation（观察池）

新加入的 relay 进入观察池：
- 初始分数 `successScore = 0.70`
- 采用 FIFO（先进先出）淘汰策略，按 `connectedSince` 排序
- 成功一次（`errOK`）后提升（promotion）到主池
- **保护标记（`protected` map）不阻止 probation 淘汰**

### Main（主池）

经过验证的 relay 位于主池：
- 采用分数 + 多轮保护的淘汰策略
- 连续 3 次失败降级（demotion）回观察池
- **保护标记和三轮保护（见第 7 节）阻止 main 淘汰**

### 容量控制

```
highWater = 50   ← 触发电位
lowWater  = 40   ← 回落目标
```

当 `total > highWater` 时触发 `prune()`：
- 计算 `need = total - lowWater`
- `need/2` 从淘汰观察池（FIFO）
- `need - need/2` 从主池淘汰（保护轮 + 最低分）
- 如果某一阶段 items 不足，差额由对方补齐
- 如果主池保护轮阻止了全部淘汰，剩余配额 **spill 回观察池**（再按 FIFO 淘汰）

---

## 3. 核心数据结构

### RelayItem

```go
type RelayItem struct {
    PeerID             peer.ID
    Addr               multiaddr.Multiaddr
    inMain             bool           // 是否在主池
    successScore       float64        // EMA 平滑后的成功分数 [0-1]
    avgRTT             time.Duration  // 平均往返时间
    limitDuration      time.Duration  // 中继限制时长
    limitData          int64          // 中继限制数据量
    rateLimitHits      int            // 限流命中次数
    consecutiveFails   int            // 连续失败次数（>=3 demote, >=5 熔断）
    circuitOpen        bool           // 熔断器是否打开
    reservationExpires time.Time      // 预约过期时间
    connectedSince     time.Time      // 连接起始时间（用于 uptime 计算）
    lastResult         time.Time      // 最后结果时间（用于活跃度保护）
}
```

### RelayPool

```go
type RelayPool struct {
    mu        sync.RWMutex
    items     map[peer.ID]*RelayItem
    config    WeightConfig       // 6 维权重配置
    protected map[peer.ID]bool   // 手动保护标记
    lowWater  int
    highWater int
}
```

### WeightConfig（六维权重）

```go
type WeightConfig struct {
    Success        float64  // 默认 0.35
    Latency        float64  // 默认 0.20
    DataLimit      float64  // 默认 0.15
    DurationLimit  float64  // 默认 0.15
    TTL            float64  // 默认 0.10
    Uptime         float64  // 默认 0.05
}
```

---

## 4. 添加（Add）与淘汰（prune）流程

### Add 流程

```
Add(ai string)
  ├─ parse multiaddr → peer.ID
  ├─ 重复 pid → 直接返回
  ├─ 创建 RelayItem：successScore=0.70, connectedSince=now
  ├─ state: probation
  └─ 调用 prune()
```

Add 不再执行单条淘汰，而是始终添加后交给 `prune()` 批量处理。

### prune 流程

```
prune()
  ├─ len(items) <= highWater → return（未超过水位线）
  ├─ need = len(items) - lowWater
  ├─ evictFromProbation = need/2
  ├─ evictFromMain = need - evictFromProbation
  │
  ├─ 阶段不足调整
  │   ├─ evictFromProbation > len(probationPids)
  │   │   → evictFromMain += 差额, evictFromProbation = len(probationPids)
  │   └─ evictFromMain > len(mainPids)
  │       → evictFromProbation += 差额, evictFromMain = len(mainPids)
  │
  ├─ Step 1: 淘汰观察池（FIFO by connectedSince）
  │   └─ sort probationPids → 删除前 evictFromProbation 个
  │
  ├─ Step 2: 淘汰主池（保护轮 + 最低分）
  │   ├─ Round 1: lastResult < 5min → protected
  │   ├─ Round 2: top-20% by score → protected
  │   ├─ Round 3: top-10 by connectedSince (oldest) → protected
  │   └─ 剩余按 score 升序淘汰
  │
  └─ Step 3: spill-back
      └─ 如果 evictFromMain > 0（主池保护轮阻止了淘汰）
          → 剩余配额 spill 回观察池，按 FIFO 继续淘汰
```

### 伪代码

```go
func (p *RelayPool) prune() {
    p.mu.Lock()
    defer p.mu.Unlock()

    if len(p.items) <= p.highWater {
        return
    }
    need := len(p.items) - p.lowWater

    // 拆分淘汰配额
    evictFromProbation := need / 2
    evictFromMain := need - evictFromProbation

    // 阶段不足调整
    probationPids, mainPids := p.splitTiers()
    if evictFromProbation > len(probationPids) { /* shift to main */ }
    if evictFromMain > len(mainPids) { /* shift to probation */ }

    // 淘汰观察池（FIFO）
    sort.Slice(probationPids, byConnectedSince)
    for _, pid := range probationPids {
        if evictFromProbation <= 0 { break }
        delete(p.items, pid); evictFromProbation--
    }

    // 淘汰主池（保护轮）
    candidates := p.filterProtected(mainPids) // 3 rounds
    sort.Slice(candidates, byScoreAsc)
    for _, kv := range candidates {
        if evictFromMain <= 0 { break }
        delete(p.items, kv.pid); evictFromMain--
    }

    // spill 回观察池
    if evictFromMain > 0 {
        // repeat FIFO eviction from remaining probation
    }
}
```

---

## 5. 分数计算

### EMA（指数移动平均）

成功和失败的分数更新使用 EMA，平滑波动：

```go
func ema(old, newVal, alpha float64) float64 {
    return alpha*newVal + (1-alpha)*old
}
```

| 事件 | α | 公式 |
|------|---|------|
| errOK | 0.3 | `score = 0.3×1 + 0.7×old` |
| errFailed | 0.3 | `score = 0.3×0 + 0.7×old` |
| errRateLimited | 0.5 | `score = 0.5×0 + 0.5×old`（更高衰减） |

### 总分计算

```go
func (p *RelayPool) calcScore(item *RelayItem) float64 {
    if item.circuitOpen { return 0 }
    s := config.Success  * item.successScore
    s += config.Latency  * calcLatency(item.avgRTT)
    s += config.DataLimit * calcDataLimit(item.limitData)
    s += config.DurationLimit * calcDurationLimit(item.limitDuration)
    s += config.TTL       * calcTTL(item.reservationExpires)
    s += config.Uptime    * calcUptime(item.connectedSince)
    return s
}
```

### 各维度标准化函数

| 维度 | 函数 | 逻辑 |
|------|------|------|
| Latency | `calcLatency(rtt)` | rtt=0 → 0.5; <50ms → 1.0; >2000ms → 0.1; 线性插值 |
| DataLimit | `calcDataLimit(data)` | data=0 → 0.5; min(data/128KB, 1.0) |
| DurationLimit | `calcDurationLimit(dur)` | dur=0 → 0.5; min(dur/2min, 1.0) |
| TTL | `calcTTL(expires)` | 剩余<0 → 0; >10min → 1.0; 线性归一化 |
| Uptime | `calcUptime(since)` | >5min → 1.0; <10s → 0.3; 0.3~1.0 线性 |

---

## 6. 选择算法（Select）

采用 **epsilon-greedy + 轮盘赌** 混合策略：

```
Select()
  ├─ 收集所有 circuitOpen=false 的 items
  ├─ 空 → return nil
  ├─ rand(0,1) < 0.1
  │   └─ 均匀随机选一个（探索）
  └─ 否则
      ├─ 计算总分 totalScore
      ├─ totalScore == 0 → 均匀随机
      └─ 否则轮盘赌（分数越高选中概率越大）
```

---

## 7. 提升（Promotion）与降级（Demotion）

```
RecordResult(pid, err)
  ├─ errOK
  │   ├─ successScore = EMA(successScore, 1, 0.3)
  │   ├─ consecutiveFails = 0
  │   ├─ circuitOpen = false
  │   └─ if !inMain → inMain = true (promotion)
  │
  ├─ errRateLimited
  │   ├─ successScore = EMA(successScore, 0, 0.5)
  │   └─ rateLimitHits++（不 demote）
  │
  └─ errFailed
      ├─ successScore = EMA(successScore, 0, 0.3)
      ├─ consecutiveFails++
      ├─ if inMain && consecutiveFails >= 3
      │   → inMain = false, successScore = 0.70 (demotion)
      └─ if consecutiveFails >= 5
          → circuitOpen = true (circuit breaker)
```

### Promotion 条件

- 位于观察池（`inMain=false`）
- `RecordResult` 收到 `errOK`

### Demotion 条件

- 位于主池（`inMain=true`）
- 连续 3 次 `errFailed`

注意：demotion 后 `consecutiveFails` **不重置**，而是继续计数。这是为了熔断器能基于完整的失败序列触发。

### 重新提升

demotion 后如果收到 `errOK`，可以重新提升回主池。

---

## 8. 熔断器（Circuit Breaker）

```
连续失败计数:
  0 → 1 → 2 → 3 → 4 → 5 (circuitOpen)
  ↑           (demote)     │
  └──── errOK 重置 ────────┘
```

- 阈值：5 次连续失败（`consecutiveFails >= 5`）
- 触发后：`circuitOpen = true`，`calcScore` 返回 0，`Select` 跳过
- 恢复：任意一次 `errOK` 将 `circuitOpen` 重置为 `false`

---

## 9. 保护轮（Main 淘汰保护）

当 `prune()` 需要从主池淘汰时，依次执行三轮保护：

### Round 1：最近活跃保护

```go
if time.Since(item.lastResult) < 5*time.Minute {
    continue  // protected
}
```

在过去 5 分钟内有结果记录的 relay 受保护。

### Round 2：高分保护

```go
topK := ceil(len(candidates) * 0.20)
sort by score desc
candidates = candidates[topK:]  // top-20% 移除
```

分数最高的前 20%（小数上取整）受保护。

### Round 3：长连接保护

```go
uptimeK := min(10, len(candidates))
sort by connectedSince asc
candidates = candidates[uptimeK:]  // oldest uptimeK 移除
```

连接时间最长的前 10 个 relay 受保护。

### 最终淘汰

三轮保护后剩余的候选者，按分数**升序**排列，淘汰最低分者直至满足配额。

### 边界处理

- 如果某个保护轮的保护数量 ≥ candidates 总数，所有 candidates 被保护（`candidates = nil`）
- 如果所有 candidates 都被保护，main 淘汰返回 0 条，剩余配额**spill 回观察池**按 FIFO 继续淘汰

---

## 10. 错误分类（classifyError）

```go
func classifyError(err error) errType {
    if err == nil {
        return errOK
    }
    if errors.Is(err, network.ErrReset) {
        return errRateLimited
    }
    var re *client.ReservationError
    if errors.As(err, &re) && re.Status == RESERVATION_REFUSED {
        return errRateLimited
    }
    return errFailed
}
```

| 错误 | 分类 | 行为 |
|------|------|------|
| `nil` | `errOK` | 加分，可能 promotion |
| `network.ErrReset` | `errRateLimited` | 高分衰减(α=0.5)，不 demote |
| `ReservationError{Status: RESERVATION_REFUSED}` | `errRateLimited` | 同上 |
| 其他错误（超时、连接失败等） | `errFailed` | 常规衰减，可能 demote/熔断 |

---

## 11. 主动健康检查（StartHealthCheck）

```go
StartHealthCheck(ctx, interval)
  ├─ 每 interval（如 30s）执行
  ├─ 遍历 items
  │   ├─ 跳过 circuitOpen
  │   └─ 跳过 disconnected
  └─ 对每个 target
      ├─ ping(ctx, host, target)
      ├─ 成功 → 更新 avgRTT, successScore, 重置 consecutiveFails
      └─ 失败 → RecordResult(pid, error)
```

被动记录（`RecordResult`）和主动健康检查配合使用，覆盖全面。

---

## 12. 辅助操作

| 方法 | 功能 |
|------|------|
| `Remove(pid)` | 从池中移除并清理 protected 标记 |
| `Protect(pid)` | 加入手动保护 map（仅防止 main 淘汰） |
| `Unprotect(pid)` | 移除手动保护 |
| `SetWeights(w)` | 批量更新 6 维权重 |
| `SetWeight(factor, value)` | 更新单个维度权重 |
| `SetRelayLimits(pid, dur, data)` | 更新 relay 限制数据 |
| `SetReservationTTL(pid, expires)` | 更新预约过期时间 |
| `Stats()` | 返回总览统计 |

---

## 13. 测试覆盖

46 个测试用例，9 大分类，全部通过：

| 分类 | 数量 | 覆盖点 |
|------|------|--------|
| Add | 6 | 正常/重复/无效/满池不拒/超水位prune/保护存活 |
| Promotion | 4 | 观察→主/满时prune/未知pid/已主再OK |
| Demotion | 5 | 3次降/5次熔断/观察不降/限流不降/重升 |
| Select | 3 | 空/正常/全熔断nil |
| Prune容量 | 5 | 低于/等于水位/观察FIFO/main可淘汰/阶段不足补齐/spill |
| Prune保护 | 4 | 活跃保护/高分保护/protected map/长连接保护 |
| Prune替代(原evictOne) | 3 | 观察FIFO/main最低分/全保护main不损 |
| 增删改 | 3 | Remove/Protect/Unprotect |
| 统计/边界 | 4 | 空统计/熔断统计/SetWeight/SetWeights/默认权重 |
| 扩展方法 | 4 | SetRelayLimits/SetReservationTTL（含未知pid） |
| 并发 | 2 | Add+Select并发/RecordResult+Select+Stats并发 |

---

## 14. 设计权衡

### 为什么选用 SLRU 双阶段而非单一优先级队列？

- 新加入的中继需要"试用期"验证可靠性
- FIFO 淘汰观察池限制低质量中继的停留时间
- 主池的多维评估提供更精细的排序

### 为什么保护轮只适用于主池？

观察池的定位是"待验证"，保护标记只阻止主池淘汰。这确保新中继必须证明自己才能进入主池。

### 为什么 demotion 不重置 consecutiveFails？

熔断器需要基于完整的连续失败序列触发。如果 demotion 时重置计数，需要 3（demotion）+ 5（熔断）= 8 次失败才能触发熔断，不符合"5 次连续失败即熔断"的设计。

### 为什么 prune 使用 split 而非单一排序淘汰？

split 确保两个阶段都有淘汰压力：观察池避免堆积过期中继，主池持续优胜劣汰。同时 spill-back 机制防止保护轮完全阻塞淘汰流程。
