// Demo 1: 远程函数调用入门(unary RPC)
//
// 这个 demo 展示"client.Add(2,3) 怎么调到另一个进程"的完整链路:
//
//	序列化 → 帧化 → TCP 发送 → server 解析 → 执行 handler → 回 RESPONSE
//	→ client 读循环按 StreamID 分发 → 反序列化 → 返回结果
//
// 跑法:
//
//	go run ./cmd/unary
//
// 会看到 server 和 client 的交替日志,最后打印 Add(2,3) = 5。
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"time"

	"minirpc/minirpc"
)

func main() {
	// ── server 侧:监听一个端口,注册 MathService.Add ──────────────────────
	lis, err := net.Listen("tcp", "127.0.0.1:0") // :0 = 让 OS 分配空闲端口
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()

	srv := minirpc.NewServer()
	srv.Register("MathService.Add", func(stream *minirpc.Stream, args json.RawMessage) (any, error) {
		// handler 知道自己的签名是 Add(a, b int) (int, error),
		// 所以自己解参数。框架不替它猜类型 —— 见 codec.go 的 Request.Args 注释。
		var nums []int
		if err := json.Unmarshal(args, &nums); err != nil {
			return nil, fmt.Errorf("bad args: %w", err)
		}
		if len(nums) != 2 {
			return nil, fmt.Errorf("Add wants 2 args, got %d", len(nums))
		}
		log.Printf("[server] 收到请求 stream#%d: Add(%d, %d) — 开始计算", stream.ID(), nums[0], nums[1])
		sum := nums[0] + nums[1]
		log.Printf("[server] 计算完成,准备回 RESPONSE: %d", sum)
		return sum, nil
	})

	go func() {
		log.Printf("[server] 监听 %s", addr)
		if err := srv.Serve(lis); err != nil {
			log.Printf("[server] Serve 返回: %v", err)
		}
	}()
	defer srv.Close()

	// 给 server 一点时间真的开始监听
	time.Sleep(100 * time.Millisecond)

	// ── client 侧:拨号、调 Add、打印 ──────────────────────────────────────
	log.Printf("[client] 拨号 %s (这一步建立 TCP 连接)", addr)
	client, err := minirpc.Dial(addr)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer client.Close() // 关底层 net.Conn → server 读循环收到 EOF 退出

	log.Printf("[client] 调用 MathService.Add(2, 3) —— 这一行的背后:")
	log.Printf("         1) 参数 [2,3] 被 JSON 序列化")
	log.Printf("         2) 包成 Request{method, args}")
	log.Printf("         3) 序列化为 payload,塞进 Frame{StreamID=1, Type=REQUEST}")
	log.Printf("         4) WriteFrame 写进 TCP(带长度前缀)")
	log.Printf("         5) 阻塞等 RESPONSE(同一个 StreamID=1)")

	var result int
	_ = context.Background() // 这里用不到 context,但留着示意:真实 RPC 都支持 context
	if err := client.Call("MathService.Add", &result, 2, 3); err != nil {
		log.Fatalf("Call: %v", err)
	}

	log.Printf("[client] ✅ Add(2, 3) = %d", result)

	time.Sleep(100 * time.Millisecond) // 让 server 端日志有机会打出来
}
