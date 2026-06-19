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
//   - Recv():阻塞读入站的一帧;收到 TypeClose 时返回 io.EOF(流结束的标志)。
//   - Close():结束这条流,释放底层资源。
//
// 为什么把 Recv 设计成"收到 CLOSE 返回 io.EOF"?
// 因为这正是 Go 标准库 io.Reader 的习惯 —— 调用方写 for 循环一直 Recv,
// 遇到 io.EOF 自然退出,代码模式统一漂亮。containerd streaming 也是这么做的。
type Stream struct {
	id   uint32
	conn *Conn
	recv *streamRecv // 入站队列 + done(conn.go 注释里有为什么包一层)

	mu     sync.Mutex
	closed bool
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
// 关键难点:Close() 会关闭 recv.done,而 Go 的 select 在多个 case 同时
// ready 时是**随机**选一个。如果"队列里有帧" 和 "done 已关闭"同时为真,
// select 可能直接走 done 分支返回 io.EOF,把队列里还没读的帧**丢掉**。
//
// 这对 Demo 3 是致命的:fixed 模式靠的就是"主流程 Close 后等 goroutine
// 把队列里所有帧 Recv 完再退出",如果 Recv 在 done 已关时跳过队列里的帧,
// join 也救不了。
//
// 所以 Recv 必须保证:**只要队列里还有帧,就一定先消费完,再考虑 EOF**。
// 下面用一个 for 循环 + "done 仅作为唤醒信号" 来实现这个保证。
//
// 另一个细节:当整个 Conn 关闭时,closeAll 会 close(recv.ch),此时
// `f, ok := <-ch` 的 ok 变成 false,handleFrame 会返回 io.EOF。
// 所以 Conn 级别的关闭也能让 Recv 正常返回,不死锁。
func (s *Stream) Recv() (*Frame, error) {
	for {
		// 优先非阻塞读队列。有帧就直接处理,绝不让 done 抢跑。
		select {
		case f, ok := <-s.recv.ch:
			return s.handleFrame(f, ok)
		default:
		}

		// 队列暂时没有帧,阻塞等。
		// 两个 case:队列来了帧 / done 被关(Close 或连接级 closeAll 触发)。
		select {
		case f, ok := <-s.recv.ch:
			return s.handleFrame(f, ok)
		case <-s.recv.done:
			// done 被关了 —— 但此时队列可能"几乎同时"又有帧到达
			// (因为 Close 和 server 最后一条 DATA 是并发的)。
			// 所以这里不能直接 return EOF,而是回到 for 循环顶部,
			// 再做一次非阻塞读。如果队列里还有帧,会被处理掉;
			// 如果队列真空了,下一次非阻塞读走 default → 再次进阻塞 select →
			// done 仍关 → 再回顶部 …… 这样会空转吗?
			// 不会:连接级关闭会 close(ch),那 non-blocking 读会拿到 ok=false。
			// 而单流 Close(Unregister)只 close(done) 不 close(ch),所以如果
			// ch 真的空且不会再有帧,我们需要另一个信号。
			// 解决:Unregister 已经把流从 map 摘掉,读循环不会再投递。
			// 此时如果 ch 空,Recvr 该返回 EOF —— 我们用下面的 drain 检查:
			// done 已关 → 流已被 Unregister(或连接关),不可能再投递新帧,
			// 所以"ch 当前空"就等价于"永远不会再来帧",可以安全返回 EOF。
			select {
			case f, ok := <-s.recv.ch:
				// 最后一刻又有帧(连接级 closeAll 之前投进来的),处理掉
				return s.handleFrame(f, ok)
			default:
				return nil, io.EOF
			}
		}
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
