package minirpc

import (
	"fmt"
	"io"
	"sync"
)

// stream.go 是"逻辑流"的抽象。
//
// 一条 TCP 物理连接被 conn.go 多路复用成很多条逻辑流,
// 每条流用一个 StreamID 标识。Stream 就是其中一条逻辑流的高层 API:
//
//   - Send(type, payload):往外发一帧(标上自己的 StreamID)。
//   - CloseSend():发一个 TypeClose,表示"我的发送方向到此为止"——协议帧,
//     不影响本地接收(这是 client-streaming / bidi 的半关闭)。
//   - Recv():阻塞读入站的一帧;收到对端的 TypeClose 时返回 io.EOF(远端说完了)。
//   - Close():本地结束这条流、注销接收队列(我不再听了),释放底层资源。
//
// 为什么把 Recv 设计成"收到 CLOSE 返回 io.EOF"?
// 因为这正是 Go 标准库 io.Reader 的习惯 —— 调用方写 for 循环一直 Recv,
// 遇到 io.EOF 自然退出,代码模式统一漂亮。containerd streaming 也是这么做的。
type Stream struct {
	id   uint32
	conn *Conn
	recv *streamRecv // 入站队列 + done(conn.go 注释里有为什么包一层)

	mu         sync.Mutex
	closed     bool // 本地 Close 已调
	sendClosed bool // CloseSend 已调(发过远端半关闭帧)
}

// newStream 不对外暴露构造,统一通过 client/server 创建,保证 id 和 conn 配对。
func newStream(id uint32, conn *Conn, recv *streamRecv) *Stream {
	return &Stream{id: id, conn: conn, recv: recv}
}

// NewStreamFromChan 让外部用"已 Register 的队列 + 指定 StreamID"构造一条 Stream。
//
// 什么时候需要它?client 侧某些高级场景(比如 Demo 3:要预先开一条流并把
// StreamID 当参数传给 server)需要自己控制 StreamID,而不是走 client.Call 的
// 自动分配。此时调用方自己 Conn.Register(id) 拿到队列,再用本函数造 Stream。
func NewStreamFromChan(id uint32, conn *Conn, recv *streamRecv) *Stream {
	return newStream(id, conn, recv)
}

// ID 返回这条流的 StreamID,打日志/调试时有用。
func (s *Stream) ID() uint32 { return s.id }

// Conn 返回这条流所在的底层多路复用连接。
//
// 为什么暴露它?因为 server 端的 handler 有时需要"往**别的** StreamID 写帧"
// (典型场景:server-streaming RPC 里,主调用的 RESPONSE 在一条流上,
// 但 progress 数据要推到另一条流上 —— Demo 3 就是这个结构)。
// 暴露 Conn 让 handler 能往任意 StreamID 发帧,这是教学上更贴近真实场景的做法。
// 对应 ttrpc 里 handler 通过 context 拿到 connection。
func (s *Stream) Conn() *Conn { return s.conn }

// Send 沿这条流发一帧出去。type/payload 由调用方决定。
// StreamID 自动填成自己的,保证多路复用正确。
func (s *Stream) Send(t FrameType, payload []byte) error {
	return s.conn.Send(&Frame{
		StreamID: s.id,
		Type:     t,
		Payload:  payload,
	})
}

// CloseSend 表示"我这条流的发送方向到此为止":发一个 TypeClose 帧给对端。
// 幂等:多次调用只发一次帧(对应 HTTP/2 END_STREAM 只发一次的约定)。
//
// 注意它和 Close 完全是两件事,别混:
//
//   - CloseSend:协议帧。告诉对端"这条流的入站方向 EOF 了",对端 Recv 把排在
//     它前面的 DATA 全部读完后,会在这帧上返回 io.EOF。本地的接收队列**不受
//     影响** —— 调完 CloseSend 仍可以继续 Recv(比如等 server 的 RESPONSE)。
//   - Close:本地动作。注销本流的接收队列(recv.done),表示"我不再读了"。
//     它不发任何协议帧。
//
// 为什么要分这两个?这对应 HTTP/2 / ttrpc 的"半关闭":client-streaming /
// bidi 场景里,client 要先把一串 DATA 推完、告诉 server"我说完了",但还想
// 继续读 server 的回包。CloseSend 就是那个"我说完了"的信号;本地 Close 要等
// 真的不需要这条流了才调(它会把整条流拆掉)。
func (s *Stream) CloseSend() error {
	s.mu.Lock()
	if s.sendClosed {
		s.mu.Unlock()
		return nil
	}
	s.sendClosed = true
	s.mu.Unlock()
	return s.Send(TypeClose, nil)
}

// Recv 阻塞读这条流的下一帧。
//
// 返回值:
//   - 收到普通帧 → 返回 (frame, nil)
//   - 收到 TypeClose → 返回 (nil, io.EOF) —— 流的正常结束信号
//   - 收到 TypeError → 返回 (nil, err) —— 流的异常结束
//   - 流已 Close / 连接断 → 返回 (nil, io.EOF 或其它)
//
// io.EOF 这个约定让用户可以这样写消费循环:
//
//	for {
//	    f, err := stream.Recv()
//	    if err == io.EOF { break }
//	    if err != nil { ... }
//	    handle(f)
//	}
//
// ── 实现细节(踩过坑,很重要)─────────────────────────────────────────
// 这里有两个不同的"结束"语义,不能混在一起:
//
//   - 对端结束它的发送方向:它发一个 TypeClose 帧(对端用 CloseSend 发,
//     或框架在 RESPONSE 之后统一发)。TypeClose 和前面的 DATA 同在 recv.ch
//     里排队,所以 Recv 会自然按 FIFO 先读完 DATA,再在 TypeClose 上返回
//     io.EOF —— 这是"远端 EOF"。
//   - 本地调用 Close():它会关闭 recv.done,表示调用方已经放弃这条流、不再
//     想读。此时 Recv 应该尊重本地关闭,尽快返回 io.EOF,不要再 drain 队列
//     —— 这是"本地取消"。
//
// 另一个细节:当整个 Conn 关闭时,closeAll 会 close(recv.ch),此时
// `f, ok := <-ch` 的 ok 变成 false,handleFrame 会返回 io.EOF。
// 所以 Conn 级别的关闭也能让 Recv 正常返回,不死锁。
func (s *Stream) Recv() (*Frame, error) {
	select {
	case <-s.recv.done:
		return nil, io.EOF
	default:
	}

	select {
	case <-s.recv.done:
		return nil, io.EOF
	case f, ok := <-s.recv.ch:
		return s.handleFrame(f, ok)
	}
}

// handleFrame 把 Recv 的几个分支(队列拿到帧)共用逻辑提出来。
func (s *Stream) handleFrame(f *Frame, ok bool) (*Frame, error) {
	if !ok {
		// channel 被关闭(连接级 closeAll)→ 视为流结束。
		return nil, io.EOF
	}
	switch f.Type {
	case TypeClose:
		// server 显式发的"流结束"信号,转成 io.EOF 语义。
		return nil, io.EOF
	case TypeError:
		return nil, streamErr(f)
	default:
		return f, nil
	}
}

// Close 关闭这条流,从 conn 注销入站队列。
// 幂等:多次调用安全。Close 后再有 Recv 会立即返回 io.EOF。
func (s *Stream) Close() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	s.mu.Unlock()

	s.conn.Unregister(s.id)
	return nil
}

// streamErr 把一个 TypeERROR 帧解成 error。
//
// payload 约定是一个 JSON 编码的 Response,取它的 Err 字段。
// 如果 decode 失败 / Err 为空(对端发了一个畸形错误帧),不要用一个无信息的
// "stream error" 吞掉它 —— 把原始 payload(截断)附上,方便排查。
func streamErr(f *Frame) error {
	var resp Response
	if Decode(f.Payload, &resp) == nil && resp.Err != "" {
		return streamError{msg: resp.Err}
	}
	raw := string(f.Payload)
	if len(raw) > 128 {
		raw = raw[:128] + "...(truncated)"
	}
	return streamError{msg: fmt.Sprintf("stream error (raw=%q)", raw)}
}

type streamError struct{ msg string }

func (e streamError) Error() string { return "minirpc stream error: " + e.msg }
