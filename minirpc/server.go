package minirpc

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
)

// server.go 把 frame/codec/conn/stream 组装成一个能用的 server。
//
// server 做三件事:
//  1. 监听 TCP(net.Listen),每来一条连接,起一个连接级 goroutine。
//  2. 在每条连接上跑一个 Conn(自带读循环),并把每个收到的 REQUEST 分派给
//     注册表里对应的 handler。
//  3. handler 的返回值序列化成 RESPONSE(或流场景里持续 Send DATA + CLOSE),
//     通过 Conn 写回 client。
//
// 注册表非常简单:map[method]Handler,没搞任何中间件/拦截器(教学优先)。
// 对应 ttrpc RegisterService,生产版会自动从 protobuf 生成这份注册表。

// Handler 是一个方法调用的处理函数。
//
// 参数:
//   - stream:这条调用对应的逻辑流。handler 在流式场景里用它持续 Send DATA;
//     **unary 调用时 handler 不需要自己发 RESPONSE** —— handler 只要 return (result, err),
//     框架在 handler 返回后自动把 result/err 序列化成 RESPONSE 发回去(见 serveStream)。
//   - args:方法参数的 JSON 字节。handler 自己知道签名,自己 Decode,框架不替它猜类型
//     —— 这正是 codec.go 里 Request.Args 用 RawMessage 的原因。
//
// 返回 (result, err):result 会被序列化成 RESPONSE 的 result 字段;err 不为空时
// 序列化成 RESPONSE.Err。流式 handler 通常返回 (nil, nil),因为数据已经边算边推了。
type Handler func(stream *Stream, args json.RawMessage) (result any, err error)

// Server 持有 service 注册表、监听器,以及两条 goroutine 计数:
//   - wg:        连接级 goroutine(每条 TCP 连接一个,Serve 起的)
//   - handlerWG: 请求级 goroutine(每条逻辑流一个,serveStream 起的)
//
// 为什么 handler 要单独跟踪?因为 handler 是在 conn.go 的 readLoop 里通过
// OnNewStream 回调起 goroutine 的(见 serveStream 注释)。如果 Server.Close 只等
// 连接 goroutine,handler 可能还在跑 —— 那才是真正需要等的东西(比如慢 handler
// 正在干活)。所以把两类 goroutine 分开跟踪,Close 都等。
type Server struct {
	mu       sync.RWMutex
	handlers map[string]Handler

	listener  net.Listener
	conns     map[net.Conn]struct{} // 活跃连接,Close 时主动关掉它们
	wg        sync.WaitGroup        // 等连接级 goroutine
	handlerWG sync.WaitGroup        // 等请求级(handler)goroutine

	closeOnce sync.Once
}

// NewServer 创建一个空 server。之后用 Register 注册方法,Serve 开始接受连接。
func NewServer() *Server {
	return &Server{
		handlers: make(map[string]Handler),
		conns:    make(map[net.Conn]struct{}),
	}
}

// Register 注册一个方法。method 形如 "MathService.Add"。
func (s *Server) Register(method string, h Handler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.handlers[method] = h
}

// Serve 在 listener 上接受连接,每个连接起一个 goroutine 处理。
// 阻塞直到 listener 关闭(通常是调 Close 或出错)。
func (s *Server) Serve(l net.Listener) error {
	// 记录 listener。加锁是因为 Close 会读它 —— 两者并发时不能裸读写
	// (race detector 会报)。Serve 通常在 Close 之前调,但代码上要保证安全。
	s.mu.Lock()
	s.listener = l
	s.mu.Unlock()
	for {
		conn, err := l.Accept()
		if err != nil {
			// listener 关闭后走到这里,正常退出。
			return err
		}
		s.mu.Lock()
		if s.conns == nil {
			// Close 已经跑过,直接丢掉这条刚 accept 来的连接
			s.mu.Unlock()
			conn.Close()
			continue
		}
		s.conns[conn] = struct{}{}
		s.mu.Unlock()

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConn(conn)
		}()
	}
}

// handleConn 处理一条 TCP 连接的全过程。
//
// 一条连接 = 一个 Conn(它内部自带一个读循环 goroutine)。
// "新流的分派"是怎么发生的?关键机制(对应 conn.go):
//  1. Conn 的读循环从 TCP 读到一帧,看帧的 StreamID。
//  2. 如果这个 StreamID 还没注册过(=新流的第一帧),且设了 OnNewStream 回调,
//     读循环就在一个新 goroutine 里调这个回调,把新流的 Stream 作为参数传进去。
//  3. 我们把回调设成 s.serveStream,于是每个新流 = 一个 serveStream goroutine,
//     在里面跑 handler。同一个连接上的多条流天然并发,这就是 Demo 2 的基础。
//
// 所以"分派"不是 server.go 里的一个循环,而是 conn.go 读循环 + 回调的组合。
// 这种"被动开流"对 server 是自然的:server 事先不知道哪个 StreamID 会来。
func (s *Server) handleConn(nc net.Conn) {
	// 用一个 Conn 包裹底层 net.Conn —— 它会自动起读循环。
	conn := NewConn(nc)
	// 注册"被动开流"回调:读到未知 StreamID 的第一帧 → 起 serveStream。
	// 包一层 wrapper:在 conn.go 起 goroutine 之前先 handlerWG.Add,保证
	// Server.Close 的 Wait 一定能覆盖到这条 handler(避免"goroutine 还没 Add
	// 就被 Wait 漏掉"的窗口)。见 serveStream 里的 Done。
	conn.OnNewStream(func(stream *Stream) {
		s.handlerWG.Add(1)
		go func() {
			defer s.handlerWG.Done()
			s.serveStream(stream)
		}()
	})

	// 等连接结束(conn 读循环退出 = 对端关闭,或 Server.Close 关了 nc)。
	<-conn.Done()

	// 把自己从活跃连接表里摘掉(Close 不必再关它)。
	s.mu.Lock()
	delete(s.conns, nc)
	s.mu.Unlock()
}

// serveStream 在 server 侧为一个新出现的 StreamID 跑一条请求的处理逻辑。
// 它在 handleConn 注册的 OnNewStream 回调里被调用(已经在一个独立 goroutine 里,
// 且 handlerWG 已经 Add 过了 —— 见 handleConn 的 wrapper)。
//
// 所以这里**不再** Add/Done handlerWG,由调用方负责。
func (s *Server) serveStream(stream *Stream) {
	// 第一帧必然是 REQUEST(由 client 发起),拿到它。
	reqFrame, err := stream.Recv()
	if err != nil {
		stream.Close()
		return
	}
	if reqFrame.Type != TypeRequest {
		// 协议异常:第一条帧不是 REQUEST。回个错误并关流。
		_ = stream.Send(TypeError, mustEncode(Response{Err: "expected REQUEST as first frame"}))
		stream.Close()
		return
	}

	// 解析出方法名,从注册表找 handler。
	var req Request
	if err := Decode(reqFrame.Payload, &req); err != nil {
		_ = stream.Send(TypeResponse, mustEncode(Response{Err: "bad request payload: " + err.Error()}))
		stream.Close()
		return
	}

	s.mu.RLock()
	h, ok := s.handlers[req.Method]
	s.mu.RUnlock()
	if !ok {
		_ = stream.Send(TypeResponse, mustEncode(Response{Err: fmt.Sprintf("unknown method: %s", req.Method)}))
		stream.Close()
		return
	}

	// 跑 handler。handler 在同一个 serveStream goroutine 里执行(不再开新 goroutine),
	// 这样 handlerWG 才能正确覆盖它的生命周期。同一连接上多条流并发,
	// 靠的是 conn 为每条流都起一个 serveStream goroutine。
	result, herr := h(stream, req.Args)

	// handler 返回值 → RESPONSE。流式 handler 通常 result==nil 且已自己推完数据,
	// 这里仍发一个 RESPONSE 作为"调用完成"的信号(对应 Demo 3 的"id=A RESPONSE" )。
	resp := Response{}
	if herr != nil {
		resp.Err = herr.Error()
	} else if result != nil {
		b, err := Encode(result)
		if err != nil {
			resp.Err = "encode result: " + err.Error()
		} else {
			resp.Result = b
		}
	}
	_ = stream.Send(TypeResponse, mustEncode(resp))

	// 框架统一发 TypeClose 作为流的终结信号(client 收到 → Recv 返回 io.EOF)。
	// unary handler 不自己发 CLOSE,由框架发;流式 handler 一般也是框架发
	// (本项目 demo 里的流式 handler 都没自己发 CLOSE,所以框架统一兜底)。
	// 如果未来 handler 自己发了 CLOSE,client 多收一个 EOF 也无害 —— Recv 看到 EOF 就退出。
	_ = stream.Send(TypeClose, nil)
	stream.Close()
}

// Close 优雅关闭:停止接受新连接 → 主动关掉所有活跃连接(让它们读循环退出)
// → 等所有连接级 goroutine 和所有请求级(handler)goroutine 跑完。
//
// 为什么现在能等到 handler?serveStream 用独立的 s.handlerWG 跟踪自己,
// handler 跑完才会 Done()。旧实现只 wait 连接 goroutine,慢 handler 会被遗漏。
//
// 幂等:多次调用安全。
func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		// 读 listener 要加锁(和 Serve 的写配对)。拿到引用后立刻 Close,
		// 让 Serve 的 Accept 返回错误退出循环。
		s.mu.Lock()
		lis := s.listener
		s.mu.Unlock()
		if lis != nil {
			_ = lis.Close()
		}
		// 主动关掉所有活跃连接,触发它们的读循环退出 → <-conn.Done() 返回 → 连接 goroutine 结束。
		s.mu.Lock()
		for nc := range s.conns {
			_ = nc.Close()
		}
		s.conns = nil // 标记"已关闭",Serve 后续 accept 的连接直接丢
		s.mu.Unlock()

		s.wg.Wait()        // 等所有连接 goroutine 结束
		s.handlerWG.Wait() // 等所有 handler goroutine 结束
	})
	return nil
}

// mustEncode 仅用于"内部回错误响应"这种绝不该失败的场景,失败就 panic。
//
// 为什么这里用 panic 而不是 return error?因为这些调用点的 payload 都是
// Response{Err: "..."} 这种字面量结构,JSON marshal 不可能失败 —— 如果失败,
// 那是程序构造错误(不该发生),用 panic 暴露比悄悄吞掉好。
// 注意:serveStream 跑在独立 goroutine 里,理论上 panic 会让这个 goroutine 挂掉、
// client 收不到 RESPONSE 而挂起。但只要 payload 是 Response 字面量,这条路径不可达。
// 用户自定义的 result 用的是带 error 处理的 Encode(见上方),不走 mustEncode。
func mustEncode(v any) []byte {
	b, err := Encode(v)
	if err != nil {
		panic(fmt.Sprintf("minirpc: cannot encode %v: %v", v, err))
	}
	return b
}
