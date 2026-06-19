// Package minirpc 是一个从零开始的教学型 RPC 框架。
//
// 它用最少的代码讲清楚三件事:
//  1. 远程过程调用(frame/codec/stub) —— frame.go / codec.go
//  2. 多路复用(一条 TCP 上按 stream-id 分发到多个逻辑流)—— conn.go
//  3. 流式 RPC(server 持续推送、client 边收边处理、EOF 语义)—— stream.go
//
// server.go 和 client.go 把上面几块拼成可用的 server / client。
//
// 对应真实世界的 containerd/ttrpc:本包里的 Frame ≈ ttrpc 的 message 帧,
// Conn 的 stream-id 分发 ≈ ttrpc 的 stream multiplexing。
// 这里只是去掉了 protobuf / tls / 完整 status code,保留机制本身。
package minirpc

import (
	"encoding/binary"
	"fmt"
	"io"
)

// FrameType 是帧的类型枚举。一条消息是什么用途,全看这个字段。
//
// 为什么要有 Type?因为一条 TCP 连接上会跑各种不同的消息:
// 调用请求、调用响应、流中的一条数据、流结束信号、错误……
// 读循环拿到一帧后,必须先知道"这是什么"才能决定怎么处理。
type FrameType byte

const (
	// TypeRequest: client → server,一次方法调用请求。
	// payload 是 Request。
	TypeRequest FrameType = 1

	// TypeResponse: server → client,unary 调用的最终返回值(含 error)。
	// payload 是 Response。一次调用有且仅有一个 Response。
	TypeResponse FrameType = 2

	// TypeData: 流中的一条数据(双向皆可,本项目主要 server → client 推 progress)。
	// payload 由上层约定(Demo 3 里是 Progress)。
	TypeData FrameType = 3

	// TypeClose: 流结束信号,对应 io.EOF。
	// 收到这帧意味着"这条流不会再有数据了",Recv 应返回 io.EOF。
	TypeClose FrameType = 4

	// TypeError: 流或调用过程中的错误。和 Response.Err 的区别:
	// Response.Err 是"正常调用失败"(业务 error);
	// TypeError 是"传输/流层面"的错误(比如框架内部崩了)。
	TypeError FrameType = 5
)

// String 让日志更可读,教学项目里打印帧时很关键。
func (t FrameType) String() string {
	switch t {
	case TypeRequest:
		return "REQUEST"
	case TypeResponse:
		return "RESPONSE"
	case TypeData:
		return "DATA"
	case TypeClose:
		return "CLOSE"
	case TypeError:
		return "ERROR"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", byte(t))
	}
}

// Frame 是协议的基本单位。一条消息 = 一个 Frame。
//
// 线上格式(教学用,简单为上):
//
//		┌──────────────┬───────────┬──────────┬───────────────┐
//		│  Length      │ StreamID  │ Type     │ Payload       │
//		│  (4 bytes,   │ (4 bytes, │ (1 byte) │ (Length-5     │
//		│   big-endian)│  BE)      │          │  bytes)       │
//		└──────────────┴───────────┴──────────┴───────────────┘
//
//	  - Length:整个帧"剩余部分"的字节数(StreamID + Type + Payload),不含 Length 自身。
//	    为什么要有 Length?因为 TCP 是字节流,没有消息边界 ——
//	    对端根本不知道一次"读"该读多少字节才算"一整条消息"。
//	    先写长度再写 payload,就是经典的"长度前缀"切消息法。
//	  - StreamID:多路复用的钥匙。同一个请求/响应/流事件共享一个 StreamID,
//	    读循环据此把帧投递到对应的逻辑流。详见 conn.go。
//	  - Type:见上面 FrameType 枚举。
//	  - Payload:序列化后的字节(JSON)。具体结构见 codec.go。
type Frame struct {
	StreamID uint32
	Type     FrameType
	Payload  []byte
}

// WriteFrame 把一帧写进 w,格式见 Frame 注释。
//
// 为什么不用 encoding/binary.Write 逐字段写?
// 因为那样会触发多次底层 write(可能触发多次系统调用),
// 也容易和别的 goroutine 的写交错(一条 TCP 不能并发写)。
// 这里先在 buf 里把整帧拼好,再一次性 w.Write,保证一帧 = 一次完整写。
func WriteFrame(w io.Writer, f *Frame) error {
	// 整帧剩余部分 = StreamID(4) + Type(1) + len(Payload)
	length := 4 + 1 + len(f.Payload)
	buf := make([]byte, 4+length) // 4 是 Length 字段自身

	// big-endian 写长度,网络协议惯例(读端按同样字节序解即可)
	binary.BigEndian.PutUint32(buf[0:4], uint32(length))
	binary.BigEndian.PutUint32(buf[4:8], f.StreamID)
	buf[8] = byte(f.Type)
	copy(buf[9:], f.Payload)

	_, err := w.Write(buf)
	return err
}

// ReadFrame 从 r 读出一整帧。
//
// 这是读循环(conn.go 的 readLoop)的核心原语:
// 不断 ReadFrame → 拿到帧 → 按 StreamID 投递 → 再 ReadFrame ……
//
// 注意:Length 一定先读出来 —— 这样才能"刚好"读够 payload,
// 既不丢字节,也不读进下一条消息的地盘。
func ReadFrame(r io.Reader) (*Frame, error) {
	// 第一步:读 4 字节的 Length。
	// 为什么用 io.ReadFull 而不是直接 r.Read?
	// 因为 r.Read 一次可能只返回 1~2 个字节(TCP 字节流,partial read 很常见),
	// ReadFull 保证"读够 N 字节,否则报错",这正是切消息需要的语义。
	var lenBuf [4]byte
	if _, err := io.ReadFull(r, lenBuf[:]); err != nil {
		// 对端关连接时会走到这里(io.EOF / io.ErrUnexpectedEOF),正常现象。
		return nil, err
	}
	length := binary.BigEndian.Uint32(lenBuf[:])

	// 校验 Length。为什么必须校验?因为 Length 来自对端,完全不可信:
	//   - length < 5:后面的切片 rest[0:4] / rest[4] / rest[5:] 会越界 panic。
	//     一个畸形帧(4 个零字节 = length 0)就能把整个读循环崩掉。
	//   - length 巨大(最大 2^32-1):make([]byte, length) 直接吃掉 ~4GB 内存,
	//     一个 4 字节的畸形帧就能 DoS 掉 server。
	// 所以这里给两条硬下限 + 一条软上限。maxFrameSize 是"教学项目可接受的单帧上限",
	// 生产版会做成可配置 / 跟滑动窗口挂钩。
	if length < 5 {
		return nil, fmt.Errorf("minirpc: frame length %d too small (need >= 5)", length)
	}
	if length > maxFrameSize {
		return nil, fmt.Errorf("minirpc: frame length %d exceeds max %d", length, maxFrameSize)
	}

	// 第二步:一次性读出 Length 长度的剩余部分(StreamID + Type + Payload)。
	// 一次性读,避免半截帧被分发出去。
	rest := make([]byte, length)
	if _, err := io.ReadFull(r, rest); err != nil {
		return nil, err
	}

	return &Frame{
		StreamID: binary.BigEndian.Uint32(rest[0:4]),
		Type:     FrameType(rest[4]),
		Payload:  rest[5:], // length 已校验 >= 5,这里切片安全
	}, nil
}

// maxFrameSize 是单帧最大字节数(StreamID + Type + Payload)。
//
// 16 MiB 对教学项目绰绰有余(本项目最大 payload 是几十字节的 JSON)。
// 设这个上限主要是为了挡住恶意 / 错误的"巨大 length"导致的内存放大。
// 对应 ttrpc/HTTP2 的 max frame size 概念,只是这里更宽松。
const maxFrameSize = 16 * 1024 * 1024
