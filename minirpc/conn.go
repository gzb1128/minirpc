package minirpc

import (
	"io"
	"sync"
)

// conn.go 是"多路复用"的心脏。
//
// 多路复用要解决的问题:
// 一条 TCP 连接上可能同时有 N 个并发请求/流在飞。
// server 处理完哪个就把哪个的响应写回来,顺序完全乱。
// 那 client 怎么知道"这一帧是给哪个请求的"?
//
// 答案就是 StreamID + 分发器:
//   - 每个逻辑流(Stream)开一条自己的入站队列(下面的 streamRecv)。
//   - 一个读循环 goroutine 不停从 TCP 读帧,看帧的 StreamID,
//     把帧投递进对应流的队列。
//   - 每个流的 Recv() 阻塞读自己的队列 —— 各流互不阻塞。
//
// 所以"一条 TCP、多个并发逻辑流、各自独立完成"这件事,
// 在代码里就是:1 个读循环 + N 个队列。
//
// 对应 containerd/ttrpc 的 stream multiplexing。生产版多了流量控制、
// 半关闭、stream 生命周期管理等,但"按 stream-id 分发"的核心机制是一样的。

// perStreamChanCap 是每条流入站 channel 的缓冲大小。
//
// 为什么要有缓冲?读循环从 TCP 读帧很快,但流的 Recv() 用户可能还没调。
// 如果 channel 无缓冲,读循环会被阻塞,拖慢所有其他流 —— 这正是要避免的。
// 给一点缓冲,让读循环尽快把帧丢下就走,保持分发顺畅。
const perStreamChanCap = 8

// NewStreamHandler 是 Conn 在读循环里"遇到一个还没注册过的 StreamID"时调用的回调。
//
// 为什么需要它?因为 client / server 的"流创建时机"是对称但相反的:
//   - client 侧:client 知道自己要发哪个 StreamID(自己生成),所以"先 Register 再 Send"。
//   - server 侧:server 不知道哪个 StreamID 会来,只能等第一帧到了才知道。
//
// 所以 Conn 提供两种模式:
//  1. 主动模式:调用方先 Register(id),再收发。
//  2. 被动模式:设置 OnNewStream 回调,读循环遇到未知 StreamID 的帧时,
//     先回调(由回调内部 Register),再把第一帧投递给新建的流。
//
// server 用被动模式,client 用主动模式。
type NewStreamHandler func(stream *Stream)

// streamRecv 是一条流的入站队列 + 关闭状态。
//
// 为什么不直接用 chan *Frame,还要包一层?因为这里藏着一个并发陷阱:
//
//	读循环拿到帧想 ch <- f 投递;
//	流的用户调用 Close() 想 close(ch) 让阻塞中的 Recv 返回 EOF。
//	如果读循环在"已 close"的 channel 上 send → panic: send on closed channel。
//
// 解决方法:**永远不 close 这个 channel**。改用一个 closed 标志位 + done channel
// 来表达"流已结束"。读循环在 send 前用 select 同时监听 done,流一关就停止投递。
// channel 本身只在整个 Conn 关闭时(closeAll)才被 close —— 那时读循环已经停了,
// 不会再 send,安全。
type streamRecv struct {
	ch chan *Frame

	// done 在流 Close 时被关闭。两个用途:
	//   1. 唤醒阻塞在 Recv 上的调用方(返回 EOF)。
	//   2. 让读循环的 select 在"流已关"时停止往里投递(见投递处注释)。
	done chan struct{}

	closeOnce sync.Once
}

func newStreamRecv() *streamRecv {
	return &streamRecv{
		ch:   make(chan *Frame, perStreamChanCap),
		done: make(chan struct{}),
	}
}

// close 标记这条流入站侧已结束(幂等)。不 close ch,理由见上。
func (r *streamRecv) close() {
	r.closeOnce.Do(func() { close(r.done) })
}

// Conn 是一条多路复用的连接,包裹了一条底层 net.Conn(用 io.ReadWriter 表达更通用)。
type Conn struct {
	rw io.ReadWriter

	mu       sync.Mutex
	closed   bool
	streams  map[uint32]*streamRecv // StreamID → 该流的入站队列
	writeMu  sync.Mutex             // 序列化所有写操作,见 Send
	doneOnce sync.Once
	done     chan struct{} // 连接结束时关闭,供 goroutine 退出

	// onNewStream 被动模式回调(server 用)。为 nil 时遇到未知 StreamID 直接丢弃。
	// 注意:回调在"读循环 goroutine"里被同步调用,所以回调里绝不能阻塞读循环。
	// server 的实现是:在回调里 Register + 起 handler goroutine,立刻返回。
	onNewStream NewStreamHandler
}

// NewConn 包裹一条底层连接,并立刻启动读循环。
//
// 一条 Conn = 一个读循环 goroutine。读循环的生命周期和 Conn 一致。
func NewConn(rw io.ReadWriter) *Conn {
	c := &Conn{
		rw:      rw,
		streams: make(map[uint32]*streamRecv),
		done:    make(chan struct{}),
	}
	go c.readLoop()
	return c
}

// OnNewStream 设置"被动模式"回调,供 server 使用。
// 一条连接只在最开始设置一次;读循环遇到未知 StreamID 的帧时会调用它。
//
// 注意:这里用 c.mu 保护写,readLoop 里读也用同一把锁 —— 因为 NewConn 已经起
// 了 readLoop(它会读 onNewStream),所以"设置回调"和"读循环读回调"之间
// 必须有 happens-before,否则 race detector 会报数据竞争。
// 典型用法是 NewConn 之后立刻 OnNewStream(在本项目 server.handleConn 里就是这样),
// 但代码上不能假设这个顺序,所以加锁保证安全。
func (c *Conn) OnNewStream(h NewStreamHandler) {
	c.mu.Lock()
	c.onNewStream = h
	c.mu.Unlock()
}

// Register 为 StreamID 开通一条入站队列,返回它(供 newStream 构造 Stream)。
//
// 调用时机:
//   - client 侧:发起一个新调用/流之前,先 Register,确保服务端的响应一来就有地方落。
//   - server 侧:收到一个未知的 StreamID 的 REQUEST 时,在 onNewStream 回调里 Register。
//
// 已存在同 StreamID 会报错(重复注册通常是 bug)。
func (c *Conn) Register(id uint32) (*streamRecv, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return nil, ErrConnClosed
	}
	if _, exists := c.streams[id]; exists {
		return nil, ErrStreamExists
	}
	recv := newStreamRecv()
	c.streams[id] = recv
	return recv, nil
}

// OpenStream 是 client 侧的便利方法:Register + 用返回的队列构造一个 Stream。
// 它自动把 StreamID 和 conn 绑定,调用方拿到 Stream 后直接 Send/Recv。
func (c *Conn) OpenStream(id uint32) (*Stream, error) {
	recv, err := c.Register(id)
	if err != nil {
		return nil, err
	}
	return newStream(id, c, recv), nil
}

// Unregister 把一条流标记为"入站侧结束"并从 map 摘掉。
//
// 注意:它**不 close** recv.ch(避免读循环 send-on-closed panic),只 close recv.done。
// 之后读循环看到 done 已关,不会再往这个队列投递帧;流的 Recv 看到 done 后
// drain 完剩余帧就返回 EOF。channel 真正被 close 只发生在 closeAll(整个连接结束)。
func (c *Conn) Unregister(id uint32) {
	c.mu.Lock()
	recv, ok := c.streams[id]
	if ok {
		delete(c.streams, id)
	}
	c.mu.Unlock()
	if ok {
		recv.close()
	}
}

// Send 把一帧写到底层连接。
//
// 为什么要加锁(写不能并发)?TCP 是字节流,如果两个 goroutine 同时写,
// 两帧的字节会交错,对端解出来全是乱码。所以必须"一帧 = 一次完整的连续写",
// 用一把锁串起来。读侧不需要加锁,因为只有一个读循环在读。
func (c *Conn) Send(f *Frame) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return WriteFrame(c.rw, f)
}

// readLoop 是多路复用的分派核心:不断读帧,按 StreamID 投递。
//
// 它是整个项目最值得读的代码。这里藏着"为什么 0.5s 的请求能比 1s 的先回来"
// 的全部秘密:不同 StreamID 的帧去不同队列,各自被各自的等待者取走,没有先来后到。
func (c *Conn) readLoop() {
	defer c.closeAll()

	for {
		f, err := ReadFrame(c.rw)
		if err != nil {
			// 连接断开 / 出错 → 读循环退出。常见错误:io.EOF。
			return
		}

		c.mu.Lock()
		recv, ok := c.streams[f.StreamID]
		h := c.onNewStream // 在锁内读,和 OnNewStream 的写配对,避免数据竞争
		c.mu.Unlock()

		if !ok {
			// 没人注册过这个 StreamID 的流。
			// 如果设了 onNewStream(server 模式),交给回调去创建流;否则直接丢弃。
			if h != nil {
				c.handleNewStream(f)
			}
			continue
		}

		// 投递。这里有个并发细节:recv 可能在我们 send 前被 Unregister。
		// 用 select 同时监听 recv.done,如果流已关就放弃这一帧(避免阻塞/panic)。
		// ch 本身永远不会被 close(除非整个连接 closeAll,那时读循环已停),
		// 所以这里不存在 send-on-closed-channel 的风险。
		select {
		case recv.ch <- f:
			// 投递成功
		case <-recv.done:
			// 流已被关闭,丢弃这一帧。常见场景:client 已经 Cancel/Close,
			// server 后续推送的 DATA 来晚了 —— 直接丢,合理。
		}
	}
}

// handleNewStream 处理"server 侧第一次见到某 StreamID"的情况。
//
// 流程:Register 一个新队列 → 造一个 Stream → 起 goroutine 调用户回调 →
// 把"触发这一切的第一帧"投递进队列(否则该帧就丢了,handler 永远 Recv 不到)。
//
// 注意整个函数不能阻塞读循环太久:回调必须自己开 goroutine 干活,
// 而第一帧的投递是有缓冲 channel,直接 send 不会阻塞(此时正好空)。
func (c *Conn) handleNewStream(first *Frame) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	if _, exists := c.streams[first.StreamID]; exists {
		// 竞态:另一个 goroutine 刚注册过,那就走普通路径
		c.mu.Unlock()
		return
	}
	recv := newStreamRecv()
	c.streams[first.StreamID] = recv
	h := c.onNewStream
	c.mu.Unlock()

	stream := newStream(first.StreamID, c, recv)
	// 调用 onNewStream 回调。注意:**这里不在这里 go**,而是把"起 goroutine"的
	// 责任交给回调本身 —— 这样调用方(server)可以在起 goroutine 之前做必要的
	// 计数(比如 handlerWG.Add),避免"goroutine 启动了但还没 Add"的关闭竞态。
	// 读循环不能被回调阻塞太久,所以回调必须立刻返回(典型实现:回调里 Add + go)。
	h(stream)
	// 把第一帧投回去,handler 的第一个 Recv() 就能拿到它。
	recv.ch <- first
}

// closeAll 在读循环退出后清理所有流,让所有 Recv() 不再永久阻塞。
//
// 此时读循环已经停止(本函数就是它 defer 调的),不会再有新的 send,
// 所以在这里 close 各流的 channel 是安全的 —— Recv 那边读到 ok=false 即返回 EOF。
func (c *Conn) closeAll() {
	c.doneOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		for id, recv := range c.streams {
			recv.close()
			close(recv.ch)
			delete(c.streams, id)
		}
		c.mu.Unlock()
		close(c.done)
	})
}

// Done 返回一个 channel,在连接结束(读循环退出)时被关闭。
// 调用方可以 <-conn.Done() 等连接彻底关闭。
func (c *Conn) Done() <-chan struct{} { return c.done }

// ErrConnClosed 在连接已关闭时注册/发送会返回。
var ErrConnClosed = io.ErrClosedPipe // 复用标准库的语义化错误

// ErrStreamExists 表示同一个 StreamID 被重复注册。
var ErrStreamExists = errStreamExists{}

type errStreamExists struct{}

func (errStreamExists) Error() string { return "minirpc: stream already exists" }
