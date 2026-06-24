package minirpc

import (
	"encoding/json"
	"fmt"
	"net"
	"sync/atomic"
)

// client.go 把 frame/codec/conn/stream 组装成一个能用的 client。
//
// client 做的事比 server 简单:
//  1. Dial 一条 TCP,包成 Conn(自带读循环 + 分发)。
//  2. Call(method, args...):生成唯一 StreamID → OpenStream → 发 REQUEST →
//     阻塞 Recv 等到 RESPONSE(忽略中间的 DATA 帧)→ 解 result 返回。这是 unary 调用。
//  3. Stream(method, args...):同上但不阻塞,返回 *Stream。它是所有流式形态
//     (server-streaming / client-streaming / bidi)的原始入口:调用方自己
//     Send/CloseSend/Recv 决定方向。Demo 3 用它做 server-streaming。
//
// 关键点:client.Call 是**阻塞**的,内部就是一个"等到 RESPONSE 才返回"的循环。
// 多个 client.Call 并发跑 → 多个 StreamID 在一条 TCP 上同时飞 → 多路复用。

// Client 是一条到 server 的多路复用连接 + 一个递增的 StreamID 生成器。
type Client struct {
	conn *Conn
	nc   net.Conn // 保留底层引用,Close 时关掉它

	// nextID 是下一个可用 StreamID。
	//
	// client 用奇数(1,3,5,...)步进 2。为什么挑奇数?因为 ttrpc/http2 里有个惯例:
	// "client 发起"用奇数 ID,"server 发起"用偶数 ID,这样双向 RPC 不会撞 ID。
	// 注意:本项目目前只有 client 主动发起调用,server 是纯被动的(收到未知 ID 的第一帧
	// 才新建流),所以 server 这边不做 ID 分配。这个奇偶分离纯粹是为"日后扩展双向 RPC"
	// 留的好习惯 —— Demo 3 的 progress 流就用偶数 ID,正好不跟 Call 的奇数 ID 冲突。
	nextID atomic.Uint32
}

// Dial 连接 addr 并返回一个就绪的 Client。
func Dial(addr string) (*Client, error) {
	nc, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	return NewClient(nc), nil
}

// NewClient 包裹一条已建好的 net.Conn。测试/demo 里有时想自己控制连接。
func NewClient(nc net.Conn) *Client {
	c := &Client{conn: NewConn(nc), nc: nc}
	// 第一个 StreamID 用 1(详见 nextID 注释)。
	c.nextID.Store(1)
	return c
}

// Conn 暴露底层 Conn,供高级用法(比如 demo 里观察连接状态)。
func (c *Client) Conn() *Conn { return c.conn }

// Close 关闭底层 net.Conn。这会让 Conn 的读循环收到 io.EOF 并退出,
// 所有阻塞的 Recv/Call 也会被唤醒返回错误。
func (c *Client) Close() error { return c.nc.Close() }

// allocateID 拿一个新的、唯一的 StreamID,并按奇偶规则步进 2。
func (c *Client) allocateID() uint32 {
	// CAS 循环:返回"步进前的当前值"(也就是分配给这次调用的 ID),再把计数器 +2。
	// 步进 2 保证永远是奇数(从 1 开始)。为什么不用 atomic.Add 一步到位?
	// 因为 Add 返回的是步进后的值,而我们要的是步进前的值作为本次 ID ——
	// CAS 循环虽然啰嗦但语义最直观,教学优先。
	for {
		cur := c.nextID.Load()
		next := cur + 2
		if c.nextID.CompareAndSwap(cur, next) {
			return cur
		}
	}
}

// Call 是一次 unary(非流式)RPC 调用:阻塞直到拿到结果。
//
// args 会按出现顺序打包成一个 JSON 数组作为 Request.Args(每个 handler 自己解出来)。
// out 必须是指针,result 会被反序列化进去。如果 server 返回了 error,
// 这里返回的 error 文本就是 server 端的 error.Error()。
//
// 为什么参数打包成 JSON 数组?因为 Go 的方法可以有多个参数,但 json.Marshal
// 只能编码"一个值"。多参数的约定:打包成数组,[arg0, arg1, ...]。
// handler 端再 Unmarshal 到 []json.RawMessage,逐个解。
func (c *Client) Call(method string, out any, args ...any) error {
	stream, err := c.openAndSend(method, args)
	if err != nil {
		return err
	}
	defer stream.Close()

	// 循环 Recv 直到拿到 RESPONSE。中间可能有 DATA 帧(unary 调用一般没有,
	// 但若 server 实现里顺便推了点东西,这里跳过)。
	for {
		f, err := stream.Recv()
		if err != nil {
			return fmt.Errorf("minirpc: stream closed before response: %w", err)
		}
		if f.Type == TypeResponse {
			var resp Response
			if err := Decode(f.Payload, &resp); err != nil {
				return fmt.Errorf("minirpc: bad response payload: %w", err)
			}
			if resp.Err != "" {
				return rpcError{method: method, msg: resp.Err}
			}
			if out != nil && len(resp.Result) > 0 {
				if err := Decode(resp.Result, out); err != nil {
					return fmt.Errorf("minirpc: decode result: %w", err)
				}
			}
			return nil
		}
		// 其它类型(DATA 等)在 unary 调用里忽略,继续等 RESPONSE。
	}
}

// Stream 发起一次流式调用:把 REQUEST 发出去,立刻返回 *Stream。
// 它是所有流式形态的"原始"入口 —— 调用方拿到 Stream 后自己决定怎么用:
//
//   - server-streaming(server 持续推 DATA):循环 Recv() 消费到 io.EOF。Demo 3 用它。
//   - client-streaming(client 持续推 DATA):循环 Send(TypeData, …) 推数据,
//     推完调 CloseSend() 半关闭发送方向,再 Recv() 等 server 的 RESPONSE。
//   - bidi(双向):Send / Recv 交错,最后 CloseSend + 等 RESPONSE。
//
// 注意:server 发完所有 DATA 后还会发一个 RESPONSE 作为"调用完成"的信号
// (对应 Demo 3 的"id=A RESPONSE"),client 拿到它就知道整个 RPC 结束了。
// 但"调用完成"≠"我已经把流里所有数据处理完",这正是 Demo 3 竞态的根源。
func (c *Client) Stream(method string, args ...any) (*Stream, error) {
	return c.openAndSend(method, args)
}

// openAndSend 是 Call / Stream 共用的前半段:
// 分配 StreamID → OpenStream → 序列化 args → 发 REQUEST。
func (c *Client) openAndSend(method string, args []any) (*Stream, error) {
	id := c.allocateID()
	stream, err := c.conn.OpenStream(id)
	if err != nil {
		return nil, err
	}

	// 把 args 编码成 JSON 数组。即使 args 为空,也用 "[]" 当 payload。
	argsJSON, err := encodeArgs(args)
	if err != nil {
		stream.Close()
		return nil, fmt.Errorf("minirpc: encode args: %w", err)
	}

	req := Request{Method: method, Args: argsJSON}
	payload, err := Encode(req)
	if err != nil {
		stream.Close()
		return nil, fmt.Errorf("minirpc: encode request: %w", err)
	}

	if err := stream.Send(TypeRequest, payload); err != nil {
		stream.Close()
		return nil, fmt.Errorf("minirpc: send request: %w", err)
	}
	return stream, nil
}

// encodeArgs 把 []any 打包成 JSON 数组的 RawMessage。
// 例如 args = [2, 3] → []byte("[2,3]")。
func encodeArgs(args []any) (json.RawMessage, error) {
	// 直接 Marshal []any 就是数组形式,完美。
	b, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(b), nil
}

// rpcError 表示"server 端业务返回的 error"。
type rpcError struct {
	method string
	msg    string
}

func (e rpcError) Error() string { return "minirpc(" + e.method + "): " + e.msg }
