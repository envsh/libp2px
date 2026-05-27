# pbexec — shexec/1.0 协议

## 协议流程

```
client → Write("command") → CloseWrite() → Read JSON response
server → LimitReader(4KB) → ReadAll → sh -c "cmd" (3s) → JSON result → Close()
```

## Shell 检测

顺序 `exec.LookPath`: `bash` → `sh` → `/bin/sh`

## 安全防护

| 阶段 | 措施 | 值 |
|---|---|---|
| 命令输入 | `LimitReader` + 读超时 | 4KB / 10s |
| 命令执行 | `exec.CommandContext` + goroutine + select | 3s timeout |
| 输出截断 | stdout/stderr 截断 | 1KB |
| 资源回收 | `cmd.Run()` goroutine 二次超时 | 2s 收割 |
| panic | `recover()` | handler 级 |

## 数据模型

```go
type ExecResult struct {
    Stdout   string `json:"stdout"`
    Stderr   string `json:"stderr"`
    ExitCode int    `json:"exit_code"`
    Error    string `json:"error,omitempty"`
}
```

## API

```go
func Exec(peerID, command string, ctx ...context.Context) (*ExecResult, error)
```
