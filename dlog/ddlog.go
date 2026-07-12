package dlog

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/bep/debounce"
)

// ─── DDLog — debounced dedup logger ────────────────────────────────────────
//
// 设计模式: debounce — 延迟执行 + 去重合并
//
// 核心语义 (单一导出方法 Printf):
//
//   Printf("msg")  ──  1s 空闲 ──→ 输出 "15:04:05 file.go:123: msg"
//               │                   或 "msg [x3]" (同消息合并)
//               │
//               └── 同 key 1s 内 ──→ count++, 重置 1s 定时器
//                                    (隐式取消上一次 pending)
//
//   Printf("不同 key")  ──→ 立即 flush 旧 batch, 开始新 batch
//
// 竞态防护 (gen 代际计数器):
//   timer fire 时进入 flush(gen, entry):
//     gen != d.gen  → 该 timer 已被后续调用取代, 静默返回
//     d.cur != entry → 该 entry 已被手工 flush, 静默返回
//
// 选型理由: bep/debounce 而非 romdo/go-debounce
//   bep/debounce 的 add(f) 内部用 time.AfterFunc 创建独立定时器,
//   每个 f 在创建时捕获闭包值, 不存在共享 fn 变量问题.
//   旧定时器发车时始终执行旧 f (含旧 gen), gen 检查可靠.
//   romdo/go-debounce 的 NewMutable 共享 fn 变量, 旧定时器回调可能
//   读到最新 fn, 导致 gen 检查失效.
// ────────────────────────────────────────────────────────────────────────────

const debounceDelay = time.Second

type ddEntry struct {
	firstFull string      // 完整日志行 (时间戳 + 文件:行 + 消息)
	count     int         // 去重计数
	firstAt   time.Time   // 首次到达时间
}

type DDLogger struct {
	mu     sync.Mutex
	curKey string        // 当前 batch 的 key (file:line:msg)
	cur    *ddEntry      // 当前 pending entry
	gen    uint64        // 代际计数器, 每次启动/重置 timer 时递增
	trig   func(func())  // debounce trigger (New 的返回值)
}

// struct has Mutex, must pointer
var DDLog = newDDLogger()

func newDDLogger() *DDLogger {
	return &DDLogger{trig: debounce.New(debounceDelay)}
}

// Printf 是 DDLog 的唯一入口.
// 同 key 在 1s 窗口内: 合并计数, 重置定时器 (隐式取消旧 pending).
// 不同 key: 立即 flush 旧 batch, 开始新 batch.
func (d *DDLogger) Printf(format string, v ...any) {
	msg := fmt.Sprintf(format, v...)
	_, file, line, ok := runtime.Caller(1)
	if !ok {
		file = "???"
		line = 0
	}
	if log.Flags()&log.Lshortfile != 0 {
		file = filepath.Base(file)
	}

	now := time.Now()
	ts := now.Format("15:04:05")
	key := fmt.Sprintf("%s:%d: %s", file, line, msg)
	full := fmt.Sprintf("%s %s:%d: %s", ts, file, line, msg)

	d.mu.Lock()

	if key == d.curKey && now.Sub(d.cur.firstAt) < debounceDelay {
		d.cur.count++
		d.gen++
		gen := d.gen
		entry := d.cur
		d.trig(func() { d.flush(gen, entry) })
		d.mu.Unlock()
		return
	}

	if d.cur != nil {
		d.printEntry(d.cur)
	}

	d.gen++
	entry := &ddEntry{
		firstFull: full,
		count:     1,
		firstAt:   now,
	}
	d.cur = entry
	d.curKey = key
	gen := d.gen
	d.trig(func() { d.flush(gen, entry) })
	d.mu.Unlock()
}

// ExitFlush 只在进程退出前调用（defer），不要在业务逻辑中使用。
func (d *DDLogger) ExitFlush() {
	d.mu.Lock()
	if d.cur != nil {
		d.printEntry(d.cur)
		d.cur = nil
		d.curKey = ""
	}
	d.mu.Unlock()
}

func (d *DDLogger) flush(gen uint64, entry *ddEntry) {
	d.mu.Lock()
	if gen != d.gen || d.cur != entry {
		d.mu.Unlock()
		return
	}
	d.printEntry(entry)
	d.cur = nil
	d.curKey = ""
	d.mu.Unlock()
}

func (d *DDLogger) printEntry(entry *ddEntry) {
	if entry.count > 1 {
		fmt.Fprintf(os.Stderr, "%s [x%d]\n", entry.firstFull, entry.count)
	} else {
		fmt.Fprintln(os.Stderr, entry.firstFull)
	}
}
