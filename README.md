# minirpc — 从零理解 RPC 的教学项目

> 📋 **实现 agent 请先读 [CONTEXT.md](./CONTEXT.md)** —— 那是完整的工程上下文、规格、时序图、验收标准。
> 本 README 只做导航,实现后由 agent 补充运行说明。

## 这个项目是干嘛的

通过**从零写一个极简 RPC 框架 + 三个 demo**,搞懂三件事:

1. **远程函数调用** —— `client.Add(2,3)` 怎么调到另一个进程的函数
2. **多路复用** —— 为什么一条 TCP 上能并发跑多个请求,互不阻塞
3. **流式 RPC + 竞态** —— server 持续推送时,"RPC 返回"和"流消费完"为什么会不同步

## 灵感来源

来自一次真实的 containerd bug 排查(PR #13625)。
排查中发现的"多路复用让 RPC 响应与 progress 流异步"现象,是本项目的核心教学点 —— Demo 3 会把它**还原成可复现、可修复的代码**。

## 目录结构

```
minirpc/
├── CONTEXT.md              # 实现规格(实现前必读)
├── README.md               # 本文件
├── go.mod                  # 零外部依赖,只用标准库
├── minirpc/                # 框架库
│   ├── frame.go            # 帧编解码(长度前缀 + StreamID + Type + payload)
│   ├── codec.go            # JSON 序列化(Request/Response/Encode/Decode)
│   ├── conn.go             # 多路复用连接:一条 TCP 上按 StreamID 分发到多个逻辑流
│   ├── stream.go           # 逻辑流抽象(Send/Recv/Close,EOF 语义)
│   ├── server.go           # server:注册 service + 分派 handler + 回 RESPONSE
│   ├── client.go           # client:stub + Call(unary)/ Stream(server-streaming)
│   ├── frame_test.go       # 帧编解码单测(含字节流 partial read)
│   ├── codec_test.go       # 序列化单测
│   └── conn_test.go        # 多路复用分派单测(net.Pipe,含 -race)
└── cmd/
    ├── unary/              # Demo 1: 远程函数调用入门
    ├── multiplex/          # Demo 2: 多路复用可视化 ⭐ 核心
    └── stream/             # Demo 3: 流式 RPC + 竞态复现 ⭐ 对应真实 bug
```

## 运行

前置:Go 1.23+(只用标准库,无依赖要装)。在项目根目录:

```bash
# Demo 1 —— 远程函数调用入门
go run ./cmd/unary

# Demo 2 —— 多路复用可视化(一条 TCP 上 3 个并发慢请求,看完成顺序)
go run ./cmd/multiplex

# Demo 3 —— 流式 RPC + 竞态复现
go run ./cmd/stream --mode buggy -count 20    # 会偶发漏收 progress
go run ./cmd/stream --mode fixed  -count 20    # 全部正确

# 跑单测(含 race detector)
go test -race ./minirpc/
```

## 三个 demo 看什么

### Demo 1:`cmd/unary` —— 远程函数调用入门

跑完会看到 `Add(2,3) = 5`,日志展示完整链路:

```
[client] 调用 MathService.Add(2, 3) —— 这一行的背后:
         1) 参数 [2,3] 被 JSON 序列化
         2) 包成 Request{method, args}
         3) 序列化为 payload,塞进 Frame{StreamID=1, Type=REQUEST}
         4) WriteFrame 写进 TCP(带长度前缀)
         5) 阻塞等 RESPONSE(同一个 StreamID=1)
[server] 收到请求 stream#1: Add(2, 3) — 开始计算
[server] 计算完成,准备回 RESPONSE: 5
[client] ✅ Add(2, 3) = 5
```

### Demo 2:`cmd/multiplex` —— 多路复用可视化 ⭐

**时序图**(实际跑出来对应):

```
client 进程                              server 进程
─────────────                            ─────────────
一条 net.Conn ───────────────────────────────────────

T+0.000s  发起 task A (RPC payload: duration=1000ms) ───►   [stream#X] sleep 1s
T+0.000s  发起 task B (RPC payload: duration=2000ms) ───►   [stream#Y] sleep 2s
T+0.000s  发起 task C (RPC payload: duration=500ms)  ───►   [stream#Z] sleep .5s
                                           (3 个 server goroutine 并发)
T+0.5s                              ◄───  stream#Z RESPONSE   ← 先回来!不是发起顺序
T+1.0s                              ◄───  stream#X RESPONSE
T+2.0s                              ◄───  stream#Y RESPONSE   ← 最后

关键观察:三条响应到达顺序 = 完成顺序,不是发起顺序。
         全程只用一条 TCP。这就是多路复用。
         task A/B/C 只是本地日志标签,不进入 RPC payload;
         真正在 TCP 字节流里分发 frame 的是协议层 StreamID。
```

实际日志样例:

```
T+0.102s  client  拨号 127.0.0.1:52386 —— 全程只用这一条 TCP 连接
T+0.104s  client  发起 task A (RPC 只发送 duration=1000ms)
T+0.104s  client  发起 task B (RPC 只发送 duration=2000ms)
T+0.104s  client  发起 task C (RPC 只发送 duration=500ms)
T+0.106s  server  收到 stream#3 的 SlowOp(duration=1000ms)
T+0.106s  server  收到 stream#5 的 SlowOp(duration=2000ms)
T+0.106s  server  收到 stream#1 的 SlowOp(duration=500ms)
T+0.608s  client  收到 task C 响应   ← ★ 最先完成!不是发起顺序 —— 多路复用的证据
T+1.108s  client  收到 task A 响应   ← 第 2 个完成(按耗时,不按发起顺序)
T+2.108s  client  收到 task B 响应   ← 第 3 个完成(按耗时,不按发起顺序)
```

注意:并发 goroutine 谁先真正调用 `client.Call` 不固定,所以示例里的
`stream#1/#3/#5` 和 task A/B/C 的对应关系每次运行都可能不同。这正是要
强调的边界:业务标签不是 StreamID;Conn 读循环只看 frame header 里的 StreamID。

### Demo 3:`cmd/stream` —— 流式 RPC + 竞态复现 ⭐ 对应真实 bug

模仿 containerd 的 `Import`:一次操作产出两部分 —— 一个最终结果(RPC 返回)+ 一串 progress(33% → 66% → done)。两部分走**两条独立的逻辑流**(因为多路复用允许),共用一条 TCP。

**时序图**(对应 containerd PR #13625):

```
client 进程                                          server 进程
─────────────                                        ─────────────
Stream A (unary RPC): Call("Import","img")
Stream B (server-stream): 收 progress

T=0  发 [id=A]REQUEST(Import)                  ───►  开始 import
     起 goroutine:                                   (干活中)
       loop { streamB.Recv() ──阻塞── 等 id=B 的帧 }

     主流程: client.Call 阻塞等 id=A 的 RESPONSE ──┐
                                                     │
T=.1s                                         ◄───  发 [id=B]DATA(progress 33%)
T=.2s                                         ◄───  发 [id=B]DATA(progress 66%)
T=.3s                                         ◄───  发 [id=B]DATA(done)
T=.3s                                         ◄───  发 [id=B]CLOSE(EOF)
T=.3s                                         ◄───  发 [id=A]RESPONSE(ok)  ← RPC 完成

     ┌── 主流程 client.Call 收到 id=A 响应,解阻塞!
     │
     │  ⚠️ 此刻 id=B 的 DATA(done) 可能还在队列里
     │     没被 progress goroutine Recv 掉!
     │
  (--mode buggy):
     主流程立刻读 progressCount → 可能是 1/2(漏了 done) ❌ 偶发
  (--mode fixed):
     主流程等 <-done(goroutine 读到 CLOSE 后再唤醒)→ progressCount=3 ✓ 稳定
```

**两个模式的唯一代码差异**(在 `cmd/stream/main.go::runOnce` 里,用注释明确标出):

```go
if mode == "fixed" {
    // ✅ FIXED:先 join progress goroutine,确保它读到 server 发来的
    // TypeClose 并退出后,再读 progressCount。
    <-done
} else {
    // ❌ BUGGY:不 join,直接读。还原"真实但漏了 join"的代码结构 ——
    // 竞态自然发生:有时 goroutine 还没 Recv 完最后一条,我们就读了计数。
    _ = done
}
```

**典型输出**:

```
=== Demo 3: 流式 RPC + 竞态复现 (mode=buggy, count=20) ===
  ⚠️  round 10: progressCount = 0 (期望 3) —— 漏收了!
  ⚠️  round 7:  progressCount = 2 (期望 3) —— 漏收了!
模式 buggy,跑了 20 轮:
  progressCount == 3(正确): 16
  progressCount <  3(漏收): 4
  ✅ 成功复现竞态!buggy 模式下确实会偶发漏收 progress。

=== Demo 3: 流式 RPC + 竞态复现 (mode=fixed, count=20) ===
模式 fixed,跑了 20 轮:
  progressCount == 3(正确): 20
  progressCount <  3(漏收): 0
  ✅ fixed 模式下全部正确收到 3 条 progress,join 修复有效。
```

## 框架 API 速查

```go
// server 端
srv := minirpc.NewServer()
srv.Register("MathService.Add", func(stream *minirpc.Stream, args json.RawMessage) (any, error) {
    var nums []int
    json.Unmarshal(args, &nums)
    return nums[0] + nums[1], nil
})
srv.Serve(listener)

// client 端(unary)
client, _ := minirpc.Dial(addr)
var result int
client.Call("MathService.Add", &result, 2, 3)

// client 端(stream)—— 拿到 stream 后自己 Recv() 消费,直到 io.EOF
stream, _ := client.Stream("ImportService.Import", "img", progressStreamID)
for {
    f, err := stream.Recv()
    if err == io.EOF { break }
    // handle f
}
```

## 协议

帧格式(长度前缀切消息边界,因为 TCP 是字节流):

```
┌──────────────┬───────────┬──────────┬───────────────┐
│  Length      │ StreamID  │ Type     │ Payload       │
│  (4 bytes,   │ (4 bytes, │ (1 byte) │ (Length-5     │
│   big-endian)│  BE)      │          │  bytes)       │
└──────────────┴───────────┴──────────┴───────────────┘
```

Type 枚举:`1=REQUEST` / `2=RESPONSE` / `3=DATA` / `4=CLOSE(EOF)` / `5=ERROR`。Payload 是 JSON。

详细规格见 [CONTEXT.md 第 3 节](./CONTEXT.md#3-协议规格-protocol-spec)。

## 与真实 containerd/ttrpc 的对应

| minirpc 概念 | containerd/ttrpc 对应 |
|---|---|
| Frame (长度+streamid+type) | ttrpc 的 message 帧 |
| Conn 的 stream-id 分发 | ttrpc stream multiplexing |
| Stream.Send/Recv | streaming.Stream 接口 |
| client.Call 阻塞等响应 | proxyTransferrer 调 `p.client.Transfer` |
| Demo 3 的 progress goroutine | proxy 的 progress 消费 goroutine |
| Demo 3 的竞态 + join 修复 | PR #13625 的 done channel + 等待 |

demo 里学到的机制 = 真实生产 RPC 的机制,只是生产版多了 protobuf、tls、更完善的错误处理等"工程外壳"。

## 验收结果(对应 CONTEXT.md 第 5 节)

| # | 标准 | 结果 |
|---|---|---|
| 1 | `go build ./...` 通过,零外部依赖 | ✅ PASS |
| 2 | `go vet ./...` 通过 | ✅ PASS |
| 3 | Demo 1 跑通,打印 `Add(2,3)=5` 且日志展示完整链路 | ✅ PASS |
| 4 | Demo 2 三请求交错完成,0.5s 先回,标注"共用 1 条 TCP" | ✅ PASS |
| 5 | Demo 3 buggy `-count=20` 至少出现一次 `<3`;fixed `-count=20` 全部 `==3`;两模式唯一差异是 join | ✅ PASS |
| 6 | 包结构清晰,每文件顶部有 doc comment 说明角色 | ✅ PASS |
| 7 | 时序图在 README(Demo 2、Demo 3) | ✅ PASS(见上) |

附:`go test -race ./minirpc/` 也全绿 —— 多路复用的并发分派经过了 race detector 验证。

## 给派发 agent 的话

- 规格全在 `CONTEXT.md`,**严格按它的验收标准实现**。
- 技术栈:Go 标准库,**零依赖**。
- 核心原则:**教学优先**(清晰 > 简洁 > 性能),注释讲 why。
- 三个 demo 必须能 `go run ./cmd/xxx` 直接跑。
