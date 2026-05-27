# pbecho — echo/1.0 协议

## 协议流程

```
client → Write("msg") → CloseWrite() → ReadAll()
server → ReadAll() → Write("Re: " + data) → Close()
```

## 安全防护

| 措施 | 值 |
|---|---|
| 输入大小限制 | `LimitReader(64KB)` |
| 读超时 | `time.After(10s)` |
| panic 恢复 | `recover()` |

## API

```go
func Echo(peerID, msg string, ctx ...context.Context) (string, error)
```
