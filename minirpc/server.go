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
// 为什么参数是 stream.Stream + json.RawMessage?
//   - Stream:handler 可能需要往流里写多条 DATA(流式 RPC,见 Demo 3),
//     即便是 unary 调用,handler 也通过 stream.Send(TypeResponse, ...) 把最终结果回出去。
//   - RawMessage:handler 自己知道方法签名,自己 Decode 参数,框架不替它猜类型。
//
// 返回 (result, err):result 会被序列化成 RESPONSE 的 result 字段;err 不为空时
// 序列化成 RESPONSE.Err。流式 handler 通常返回 (nil, nil),因为数据已经边算边推了。
type Handler func(stream *Stream, args json.RawMessage) (result any, err error)

// Server 持有 service 注册表和监听器。
type Server struct {
	mu       sync.RWMutex
	handlers map[string]Handler

	listener net.Listener
	wg       sync.WaitGroup // 等"所有连接处理 goroutine"结束,供 Close 优雅退出
}

// NewServer 创建一个空 server。之后用 Register 注册方法,Serve 开始接受连接。
func NewServer() *Server {
	return &Server{handlers: make(map[string]Handler)}
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
	s.listener = l
	for {
		conn, err := l.Accept()
		if err != nil {
			// listener 关闭后走到这里,正常退出。
			return err
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			s.handleConn(conn)
		}()
	}
}

// handleConn 处理一条 TCP 连接的全过程。
//
// 一条连接 = 一个 Conn(= 一个读循环 goroutine)+ 一个分派循环。
// 为什么要有"分派循环"?因为读循环只负责"把帧按 StreamID 投递到 channel",
// 但 server 还需要"为新出现的 StreamID 起一个 handler goroutine"。
// 这个分派循环就是用来干这个的:它为每条新流注册一个 channel,然后从 channel 里
// 拿到该流的第一个 REQUEST 帧,起 goroutine 跑 handler。
func (s *Server) handleConn(nc net.Conn) {
	// 用一个 Conn 包裹底层 net.Conn —— 它会自动起读循环。
	conn := NewConn(nc)

	// 新流的入站第一帧(REQUEST)在这里等。
	// server 事先不知道有哪些 StreamID 会来,所以用一个全局 channel 让所有
	// "未注册 StreamID 的帧"都到这里 —— 但 conn.go 的实现是"未注册就丢"。
	// 这里换一个思路:server 侧主动监听 net.Conn 的第一帧 —— 为简化,
	// 我们让 conn 支持一个"通配 channel"。下面 dispatchLoop 里有详细说明。
	//
	// 简化做法:用一个 dispatcher goroutine 自己读 net.Conn 上的"新 stream"。
	// 但 conn.go 已经在读了 —— 为避免双重读,这里采用"server 主动预注册"模式不可行。
	// 改为:让 Conn 提供 OnNewStream 回调。我们用回调实现分派。
	conn.OnNewStream(s.serveStream)

	// 等连接结束(conn 读循环退出 = 对端关闭)
	<-conn.Done()
}

// serveStream 在 server 侧为一个新出现的 StreamID 起一条服务端 Stream。
// conn.go 的读循环在遇到未知 StreamID 的第一帧时调用它。
//
// 这是"server 端的 stream 生命周期起点"。
func (s *Server) serveStream(stream *Stream) {
	// 第一帧必然是 REQUEST(由 client 发起),拿到它。
	reqFrame, err := stream.Recv()
	if err != nil {
		stream.Close()
		return
	}
	if reqFrame.Type != TypeRequest {
		// 协议异常:第一条帧不是 REQUEST。回个错误并关流。
		stream.Send(TypeError, mustEncode(Response{Err: "expected REQUEST as first frame"}))
		stream.Close()
		return
	}

	// 解析出方法名,从注册表找 handler。
	var req Request
	if err := Decode(reqFrame.Payload, &req); err != nil {
		stream.Send(TypeResponse, mustEncode(Response{Err: "bad request payload: " + err.Error()}))
		stream.Close()
		return
	}

	s.mu.RLock()
	h, ok := s.handlers[req.Method]
	s.mu.RUnlock()
	if !ok {
		stream.Send(TypeResponse, mustEncode(Response{Err: fmt.Sprintf("unknown method: %s", req.Method)}))
		stream.Close()
		return
	}

	// 在 server 侧的 goroutine 里跑 handler。注意:这里不阻塞分派循环,
	// 这样同一个 client 上并发来的多条流可以同时处理 —— 这就是 Demo 2 的实现基础。
	// (serveStream 本身就是被 conn 在独立 goroutine 里调的,见 conn.go 的 OnNewStream)
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
	stream.Send(TypeResponse, mustEncode(resp))

	// handler 可能是流式的 —— 它负责自己 Send TypeClose,但如果它忘了,
	// 这里兜底:只要 result/err 都没有,就帮它发 EOF。
	// 注意:不要重复发 CLOSE。流式 handler 通常已经在循环里发了 CLOSE,
	// 此时再发会被 client 当第二个 EOF 忽略(无害),但为干净起见,
	// 我们约定:**unary handler 不发 CLOSE,框架发;流式 handler 自己发**。
	// 这里无法区分,所以采取最简方案:总是发 CLOSE,client 多收一个 EOF 无害。
	stream.Send(TypeClose, nil)
	stream.Close()
}

// Close 停止接受新连接,并等已有连接的 handler 跑完。
func (s *Server) Close() error {
	if s.listener != nil {
		_ = s.listener.Close()
	}
	s.wg.Wait()
	return nil
}

// mustEncode 仅用于"内部回错误响应"这种绝不该失败的场景,失败就 panic。
// 教学/演示项目里这样写够用,避免在错误路径上又堆一层 error。
func mustEncode(v any) []byte {
	b, err := Encode(v)
	if err != nil {
		panic(fmt.Sprintf("minirpc: cannot encode %v: %v", v, err))
	}
	return b
}
