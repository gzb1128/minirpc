package minirpc

import "encoding/json"

// codec.go 负责"序列化":把 Go 值 ↔ 字节流。
//
// 为什么不用 protobuf?教学优先 —— JSON 用标准库就行,可读性高,
// 抓包/打日志都能直接看懂 payload。代价是体积大一点,对本项目无所谓。
// 对应 containerd/ttrpc 的话,那里换成了 protobuf + varint 长度前缀,
// 机制(序列化 → 帧化 → 传输)完全一样,只是"工程外壳"更精炼。

// Request 是 TypeRequest 帧的 payload 结构。
//
// Method 形如 "MathService.Add",server 用它到 service 注册表里找 handler。
// Args 是方法参数的 JSON 字节,为什么用 json.RawMessage 而不是 []any?
// 因为 Go 的方法签名千差万别(Add(int,int) / SlowOp(string,time.Duration)……),
// 框架无法预先知道每个方法的参数类型。所以我们只负责"原样搬运 JSON 字节",
// 真正的反序列化交给 handler 自己(它知道自己的签名)。这就是 RawMessage 的意义。
type Request struct {
	Method string          `json:"method"`
	Args   json.RawMessage `json:"args"`
}

// Response 是 TypeResponse 帧的 payload 结构。
//
// Result 是返回值的 JSON 字节(同样用 RawMessage 让 client 自己反序列化)。
// Err 是 error 文本,空串 = 无错。
// 为什么不用 *error 或 status code?教学优先,error 当字符串最直观。
type Response struct {
	Result json.RawMessage `json:"result,omitempty"`
	Err    string          `json:"err,omitempty"`
}

// Encode 把任意 Go 值序列化成 JSON 字节。frame.go 把它塞进 Frame.Payload。
func Encode(v any) ([]byte, error) {
	return json.Marshal(v)
}

// Decode 把 JSON 字节反序列化进 v。
//
// 注意 v 必须是指针 —— json.Unmarshal 把结果写进 v 指向的内存。
// 这是 net/rpc、ttrpc 等所有 RPC 框架的通用约定。
func Decode(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
