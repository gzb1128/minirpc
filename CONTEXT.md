# minirpc — RPC 框架学习项目工程上下文

> 本文件是派发给实现 agent 的 **brief(上下文)**。请先完整读完本文件再动手写代码。
> 目标不是"能跑",而是"让作者通过读代码 + 跑 demo,真正理解 RPC 的内部机制"。
> 所以**代码要为教学服务**:清晰 > 简洁 > 性能。必要的注释解释"为什么这样设计"。

---

## 0. 项目使命 (Mission)

从零构建一个**极简但完整**的 RPC 框架 `minirpc`,并配一个可运行的 demo。
通过读代码 + 跑 demo,让作者搞懂三件事:

1. **远程过程调用**:为什么 client 端写 `client.Add(2,3)` 能调用到 server 进程里的函数?
   → 帧、序列化、stub(代理对象)。
2. **多路复用 (Multiplexing)**:为什么一条 TCP 上能同时跑多个并发请求,互不阻塞?
   → stream-id 分发、每流独立队列。
3. **流式 RPC (Streaming)**:为什么 server 能持续推送(像 containerd 的 progress),client 边收边处理?
   → server-streaming、`Recv()` 阻塞读、EOF 语义。

**真实灵感来源**:这个项目的所有设计取舍,都映射 containerd 的 `ttrpc`。
作者在排查一个真实 bug 时(containerd PR #13625)发现了"多路复用导致 RPC 响应与 progress 流异步"的现象,
本项目就是把那个现象的底层机制还原出来,让人能亲手复现、亲手修复。

---

## 1. 学习者画像与背景 (Context)

### 作者已有的认知(不要重复教这些)
- 知道 TCP 是字节流,要按序读
- 知道进程间内存隔离,不能直接调函数
- 已经理解了"多路复用 = 一条 TCP 上多个独立逻辑流,用 stream-id 区分"的概念
- 理解了"RPC 响应流与 progress 流是多路复用的两条独立流,所以 RPC 返回 ≠ progress 消费完"

### 作者想搞懂的(本项目要回答的)
- 这些机制**在代码里具体长什么样**?
- "client.Add(2,3)" 这行代码,框架在背后做了哪几步?
- 多路复用的"分发器"代码怎么写?
- 怎么用代码**亲眼看到**"两个并发请求在一条 TCP 上交错完成"?
- 怎么用代码**亲手复现**那个竞态(RPC 返回时 progress 还没收完),然后理解为什么要 join?

### 技术栈
- **语言:Go**(作者在用 Go 做容器运行时相关工作,且 Go 的 goroutine + channel 天然适合演示多路复用)
- **传输:TCP**(`net.Listen` / `net.Dial`),先不用 Unix socket,保持通用
- **序列化:手工 JSON 或二进制长度前缀**(不要引入 protobuf,增加复杂度。教学优先)
- **零外部依赖**(只用标准库)

---

## 2. 要构建的东西 (Deliverables)

### 2.1 框架库 `minirpc`
一个最小 RPC 框架,放在 `minirpc/` 包下。需要提供的 API:

```
minirpc/
├── frame.go      # 帧的编解码(长度前缀 + stream-id + 类型 + payload)
├── codec.go      # 序列化(JSON):把函数参数/返回值 ↔ 字节
├── conn.go       # 多路复用连接:一条 TCP 上按 stream-id 分发到多个逻辑流
├── server.go     # server 端:注册 service、读帧、分派到 handler、回写响应
├── client.go     # client 端:stub 生成、发请求、阻塞等响应(unary)/ 持续 Recv(stream)
└── stream.go     # 逻辑流抽象:Send/Recv/Close,每条流有独立队列
```

### 2.2 Demo 程序
放在 `cmd/` 下,三个可独立运行的 demo,每个聚焦一个概念:

#### Demo 1: `cmd/unary/` — 远程函数调用入门
- server 注册一个 `MathService`,提供 `Add(a, b int) (int, error)`
- client 调用 `client.Call("MathService.Add", 2, 3)` 拿到 `5`
- **教学点**:展示"调用远程函数"的完整链路:序列化 → 帧化 → 发送 → server 解析 → 执行 → 回响应 → client 反序列化 → 返回。打印每一步的日志。

#### Demo 2: `cmd/multiplex/` — 多路复用可视化 ⭐ 核心
- client 在**一条 TCP 连接**上,**并发**发起 3 个慢请求(比如 `SlowOp(duration)`),每个 server 端 sleep 不同时长(1s / 2s / 0.5s)
- Demo 日志可以用 `task A/B/C` 表示发起顺序,但这些只是本地展示标签,不要作为 RPC 入参;真正把 frame map 回逻辑流的是协议层 `StreamID`
- 三个请求必须**交错完成**(0.5s 的先回来,不是按发起顺序)
- **必须输出一张时序日志**,形如:
  ```
  T+0.000s  client  发起 task A (RPC 只发送 duration=1000ms)
  T+0.000s  client  发起 task B (RPC 只发送 duration=2000ms)
  T+0.000s  client  发起 task C (RPC 只发送 duration=500ms)
  T+0.501s  client  收到 task C 响应   ← 不是按顺序!证明多路复用
  T+1.001s  client  收到 task A 响应
  T+2.001s  client  收到 task B 响应
  ```
- **教学点**:证明"一条 TCP,多个并发逻辑流,各自独立完成"。这是回答"为什么 RPC 响应能比 progress 流先到"的活证据。

#### Demo 3: `cmd/stream/` — 流式 RPC + 竞态复现 ⭐ 对应真实 bug
- server 提供 `Import(name) returns stream of Progress`(server-streaming)
- server 干活时持续推送进度:`33% → 66% → done`,最后发 RPC 完成响应
- client 有一个**收 progress 的 goroutine**(`stream.Recv()` 循环)+ 一个**等 RPC 响应的主流程**(`client.Call`)
- **关键教学点:复现 containerd PR #13625 的竞态**
  - Demo 要提供两个模式:
    - `--buggy`:主流程 RPC 返回后**立即**读 progress 计数 → 间歇性读到不全(因为 goroutine 还没 Recv 完)
    - `--fixed`:主流程 RPC 返回后**等待 progress goroutine 读到 CLOSE 并退出**(join)再读 → 稳定读到全部
  - 跑 `--buggy` 多次(`-count=20`)能看到偶发少收事件;`--fixed` 稳定全收
- **教学点**:这是把抽象的"多路复用导致异步"变成可复现、可修复的代码。作者亲历过这个 bug 的排查,这里让它"落地"。

---

## 3. 协议规格 (Protocol Spec)

### 3.1 帧格式 (Frame)

每条消息 = 一个帧。帧格式(教学用,简单为上):

```
┌──────────────┬───────────┬──────────┬───────────────┐
│  Length      │ StreamID  │ Type     │ Payload       │
│  (4 bytes,   │ (4 bytes, │ (1 byte) │ (Length-5     │
│   big-endian)│  BE)      │          │  bytes)       │
└──────────────┴───────────┴──────────┴───────────────┘
```

- **Length**:整个帧剩余部分长度(StreamID + Type + Payload),不含 Length 自身。用来切消息边界。
- **StreamID**:多路复用的钥匙。同一个请求/响应/流事件共享一个 StreamID。
- **Type**:帧类型,枚举:
  - `1 = REQUEST`:client→server,一次方法调用请求
  - `2 = RESPONSE`:server→client,unary 调用的最终返回值(含 error)
  - `3 = DATA`:流中的一条数据(双向皆可,本项目主要 server→client 推 progress)
  - `4 = CLOSE / EOF`:流结束信号(对应 `io.EOF`)
  - `5 = ERROR`:流或调用过程中的错误
- **Payload**:JSON 序列化的结构(见下)。

### 3.2 Payload 结构 (JSON,教学用)

```go
// REQUEST 的 payload
type Request struct {
    Method string          `json:"method"`   // e.g. "MathService.Add"
    Args   json.RawMessage `json:"args"`     // 方法参数,JSON 编码
}

// RESPONSE 的 payload
type Response struct {
    Result json.RawMessage `json:"result,omitempty"`
    Err    string          `json:"err,omitempty"`  // 空串 = 无错
}

// DATA (流事件) 的 payload,本项目用 progress 演示
type Progress struct {
    Event string `json:"event"`   // "progress" / "done"
    Value int    `json:"value"`   // 百分比
}
```

### 3.3 多路复用规则

- client 为每个并发调用生成一个**唯一递增的 StreamID**(1, 2, 3, ...)。
- server 收到 REQUEST(StreamID=X)→ 执行 → 把响应/流数据都用 **同一个 StreamID=X** 回来。
- 双方各维护一个 `map[StreamID]chan Frame`,读循环把帧按 StreamID 投递到对应 channel。
- 一条 TCP 连接 = 一个读循环 goroutine,负责解帧 + 分发。

---

## 4. 时序图 (必须能跑出来对应)

### 4.1 Demo 2 多路复用时序(一条 TCP,3 个并发请求)

```
client 进程                              server 进程
─────────────                            ─────────────
一条 net.Conn ───────────────────────────────────────

T=0  task A 发 [stream=X]REQUEST(SlowOp,1000ms) ───►  stream=X sleep 1s
T=0  task B 发 [stream=Y]REQUEST(SlowOp,2000ms) ───►  stream=Y sleep 2s
T=0  task C 发 [stream=Z]REQUEST(SlowOp,500ms)  ───►  stream=Z sleep .5s
                                          (3 个 server goroutine 并发)
T=.5s                              ◄───  发 [stream=Z]RESPONSE     ← 先回来!
     stream=Z 的 channel 收到 → 唤醒等 stream=Z 的 client goroutine
T=1s                               ◄───  发 [stream=X]RESPONSE
T=2s                               ◄───  发 [stream=Y]RESPONSE     ← 最后

关键观察:三条响应到达顺序 = 完成顺序,不是发起顺序。
         全程只用一条 TCP。这就是多路复用。
         task A/B/C 是 demo 的本地展示标签,和协议里的 StreamID 没有因果关系。
         并发调度下 task 与具体 stream 数字的对应关系也可能每次不同。
```

### 4.2 Demo 3 流式 + 竞态时序(对应 containerd bug)

```
client 进程                                          server 进程
─────────────                                        ─────────────
Stream A (unary RPC): Call("Import","img")
Stream B (server-stream): 收 progress

T=0  发 [id=A]REQUEST(Import)                  ───►  开始 import
     起 goroutine:                                   (干活中)
       loop { streamB.Recv() ──阻塞── 等 id=B 的帧
              dispatch 给用户回调 }

     主流程: client.Call 阻塞等 id=A 的 RESPONSE ──┐
                                                     │
T=.1s                                         ◄───  发 [id=B]DATA(progress 33%)
     id=B channel 收到 → goroutine Recv 到 → 回调   │
T=.2s                                         ◄───  发 [id=B]DATA(progress 66%)
T=.3s                                         ◄───  发 [id=B]DATA(done)
T=.3s                                         ◄───  发 [id=B]CLOSE(EOF)
T=.3s                                         ◄───  发 [id=A]RESPONSE(ok)  ← RPC 完成

     ┌── 主流程 client.Call 收到 id=A 响应,解阻塞!
     │
     │  ⚠️ 此刻 id=B 的 DATA(done) 可能还在 channel 里
     │     没被 goroutine Recv 掉!
     │
  (--buggy 模式):
     主流程立刻读 progressCount → 可能是 2(漏了 done) ❌ 偶发
  (--fixed 模式):
     主流程等 <-done(goroutine 读到 CLOSE 后再唤醒)→ progressCount=3 ✓ 稳定
```

**这张时序图就是 containerd PR #13625 的本质**。Demo 3 要让作者能跑出这两种模式的差异。

---

## 5. 验收标准 (Acceptance Criteria)

实现完成后,以下必须为真(作者会逐条验证):

1. **`go build ./...` 在项目根通过**,零外部依赖。
2. **`go vet ./...` 通过**。
3. **Demo 1** 跑通:client 打印 `Add(2,3) = 5`,且日志展示完整链路(序列化→帧→发送→执行→回包→反序列化)。
4. **Demo 2** 跑通:
   - 三个并发请求在一条 TCP 上交错完成。
   - 时序日志显示 **0.5s 的请求先于 1s 的回来**(证明多路复用,非串行)。
   - 日志明确标注"共用 1 条 TCP 连接"。
5. **Demo 3** 跑通:
   - `--buggy` 模式跑 `-count=20`,**至少出现一次** progress 计数 < 3(复现竞态)。
   - `--fixed` 模式跑 `-count=20`,**全部** progress 计数 == 3。
   - 两个模式的**唯一代码差异**是"主流程是否等待 progress goroutine"(用注释标出 diff 位置)。
6. 代码有清晰的包结构和注释,**每个文件顶部有 doc comment 说明它在框架里的角色**(对应第 2.1 节)。
7. 时序图(Demo 2、Demo 3)要么在 README 里,要么在运行日志里能直观看到。

---

## 6. 实现顺序建议(给 agent 的提示)

按依赖关系,建议顺序:
1. `frame.go` + `codec.go`(最底层,可独立单测)
2. `conn.go`(多路复用核心,单测:模拟一条 conn 塞两个 stream 的帧,验证分发到不同 channel)
3. `stream.go`(基于 conn 的逻辑流,Send/Recv/Close)
4. `server.go`(注册 service,读帧分派)
5. `client.go`(stub,Call 阻塞 / Stream 返回可 Recv 的流)
6. Demo 1 → 2 → 3(每个 demo 验证一层概念)

每一步都先写小单测再写实现。Demo 3 的 buggy/fixed 双模式是高潮,前面所有代码都为它服务。

---

## 7. 技术约束与教学原则

- **零依赖**:只用 Go 标准库(`net`, `encoding/json`, `sync`, `io`, `fmt`, `time`, `context`)。
- **教学优先**:遇到"工业级做法"和"易读做法"冲突,选易读。例如:
  - 用 JSON 不用 protobuf
  - 用长度前缀不用 protobuf varint
  - 错误处理够用就行,不用搞完整的 status code 体系
- **不要过度抽象**:不要搞 plugin/中间件/拦截器这些。一个 service 注册表 + 一个分派循环就够。
- **注释解释 why,不是 what**:`// 读 4 字节长度` 是 what(废话);`// 先读长度再读 payload,因为 TCP 是字节流,没有消息边界,必须自己切` 是 why(有价值)。
- **buggy 模式要真实**:Demo 3 的 `--buggy` 不是故意 sleep 制造假竞态,而是**还原真实代码结构**(主流程不等 goroutine 就返回),让竞态自然发生。

---

## 8. 参考映射 (Mapping to real containerd)

实现时可以在代码注释里点出与 containerd/ttrpc 的对应关系,帮助作者把 demo 和真实世界连起来:

| minirpc 概念 | containerd/ttrpc 对应 | 真实文件 |
|---|---|---|
| Frame (长度+streamid+type) | ttrpc 的 message 帧 | `ttrpc/message.go` |
| Conn 的 stream-id 分发 | ttrpc stream multiplexing | `ttrpc/shim.go` |
| Stream.Send/Recv | streaming.Stream 接口 | `core/streaming/streaming.go` |
| client.Call 阻塞等响应 | proxyTransferrer 调 `p.client.Transfer` | `core/transfer/proxy/transfer.go` |
| Demo 3 的 progress goroutine | proxy 的 progress 消费 goroutine | 同上 |
| Demo 3 的竞态 + join 修复 | PR #13625 的 done channel + 等待 | 同上 |
| server 注册 service | ttrpc RegisterService | `api/services/transfer/v1/transfer_ttrpc.pb.go` |

这些映射让作者明白:**demo 里学到的机制 = 真实生产 RPC 的机制,只是生产版多了 protobuf、tls、更完善的错误处理等"工程外壳"。**

---

## 9. 给实现 agent 的最后指令

1. 先读完本文件,确认理解学习目标(教学 > 工程)。
2. 按第 6 节顺序实现,每步配单测。
3. 三个 demo 都要能 `go run ./cmd/xxx` 直接跑。
4. README 里放运行说明 + 时序图(Demo 2 和 3 的预期输出)。
5. 完成后自检第 5 节的所有验收标准,把验证结果写进 README 末尾。
6. 代码风格:清晰、注释讲 why、不炫技。
