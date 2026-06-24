// Demo 2: 多路复用可视化 ⭐ 核心 demo
//
// 这个 demo 要回答:"为什么一条 TCP 上能并发跑多个请求,互不阻塞,完成顺序还能乱?"
//
// 做法:client 在一条 TCP 上**同时**发起 3 个慢请求(SlowOp),server 端每个请求
// sleep 不同时长(1s / 2s / 0.5s)。我们会看到:
//   - 0.5s 的请求**先**回来(不是按发起顺序)。
//   - 全程只有一条 TCP 连接。
//
// 这就是多路复用。它和 Demo 3 那个真实 bug 的关系:
// "既然一条 TCP 上多个流能各自独立完成,RPC 响应流自然可能比 progress 流先到 ——
// 于是 'RPC 返回' 和 'progress 消费完' 就是两件不同步的事"。
//
// 跑法:
//
//	go run ./cmd/multiplex
package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/gzb1128/minirpc/minirpc"
)

// timer 用于打印"T+0.123s"这样的相对时间戳,让时序一眼能看出来。
var startTime = time.Now()

func ts() string {
	return fmt.Sprintf("T+%.3fs", time.Since(startTime).Seconds())
}

func parseSlowOpDuration(args json.RawMessage) (int, error) {
	var params []any // [durationMillis]
	if err := json.Unmarshal(args, &params); err != nil {
		return 0, err
	}
	if len(params) != 1 {
		return 0, fmt.Errorf("SlowOp wants 1 arg, got %d", len(params))
	}
	durF, ok := params[0].(float64)
	if !ok {
		return 0, fmt.Errorf("SlowOp arg[0] must be number, got %T", params[0])
	}
	return int(durF), nil
}

func main() {
	// ── server ─────────────────────────────────────────────────────────────
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()

	srv := minirpc.NewServer()
	// SlowOp(durationMillis):睡 durationMillis 后返回。
	// 用来制造"不同耗时的并发请求",证明多路复用下完成顺序 ≠ 发起顺序。
	srv.Register("SlowService.SlowOp", func(stream *minirpc.Stream, args json.RawMessage) (any, error) {
		durMs, err := parseSlowOpDuration(args)
		if err != nil {
			return nil, err
		}

		log.Printf("%s  server  收到 stream#%d 的 SlowOp(duration=%dms)", ts(), stream.ID(), durMs)
		// 模拟一个耗时操作。每个请求在 server 自己的 goroutine 里跑,互不影响。
		time.Sleep(time.Duration(durMs) * time.Millisecond)
		log.Printf("%s  server  stream#%d 完成,回 RESPONSE", ts(), stream.ID())
		return durMs, nil
	})

	go func() {
		if err := srv.Serve(lis); err != nil {
			log.Printf("[server] Serve: %v", err)
		}
	}()
	defer srv.Close()
	time.Sleep(100 * time.Millisecond)

	// ── client:一条 TCP,3 个并发请求 ──────────────────────────────────────
	log.Printf("%s  client  拨号 %s —— 全程只用这一条 TCP 连接", ts(), addr)
	client, err := minirpc.Dial(addr)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer client.Close()
	log.Printf("%s  client  ✅ 连接已建立,接下来在上面并发发起 3 个慢请求", ts())

	// 三个请求:期望耗时分别是 1s / 2s / 0.5s。
	// task A/B/C 只是 client 日志里的本地展示标签,不会进入 RPC payload。
	// 真正把 frame map 回逻辑流的是协议层 StreamID,不是业务入参。
	reqs := []slowCall{
		{label: "task A", ms: 1000},
		{label: "task B", ms: 2000},
		{label: "task C", ms: 500},
	}

	var wg sync.WaitGroup
	for _, r := range reqs {
		wg.Add(1)
		r := r
		log.Printf("%s  client  发起 %s (RPC 只发送 duration=%dms)", ts(), r.label, r.ms)
		go func() {
			defer wg.Done()
			var result int
			if err := client.Call("SlowService.SlowOp", &result, r.ms); err != nil {
				log.Printf("%s  client  %s 失败: %v", ts(), r.label, err)
				return
			}
			log.Printf("%s  client  收到 %s 响应   ← %s",
				ts(), r.label, completeMarker(r.label, reqs))
			_ = result // result 没实际用途,demo 只关心完成时序
		}()
		// 不加任何 sleep —— 三个请求"几乎同时"发出,证明它们在一条 TCP 上并发飞。
	}

	wg.Wait()

	log.Printf("")
	log.Printf("═══════════════════════════════════════════════════════════════")
	log.Printf("观察结论:")
	log.Printf("  • 完成顺序 = task C(0.5s) → task A(1s) → task B(2s),不是发起顺序 A→B→C。")
	log.Printf("  • 全程只用了 1 条 TCP 连接(上面那行 '拨号 %s')。", addr)
	log.Printf("  • 这就是多路复用:一条物理连接 + N 条独立逻辑流(StreamID 区分),")
	log.Printf("    各流互不阻塞,谁先完成谁先回。")
	log.Printf("  • task A/B/C 只是 client 本地标签;server 只收到 duration,")
	log.Printf("    frame 能回到正确调用靠的是 header 里的 StreamID。")
	log.Printf("  • 对应真实世界:containerd 在一条 ttrpc 连接上跑多个 RPC,")
	log.Printf("    所以 'RPC 响应流' 和 'progress 流' 可能乱序到达 —— 这是 Demo 3 的伏笔。")
	log.Printf("═══════════════════════════════════════════════════════════════")
}

type slowCall struct {
	label string
	ms    int
}

// completeMarker 生成"← 不是按顺序!证明多路复用" 这种点睛注释,
// 让读者一眼看到"哎这个完成顺序违反了发起顺序"。
//
// rank 计算:按耗时(ms)升序,看 label 对应的请求是第几个 —— 直接数有多少个比它快的就行,
// 不需要排序(元素就 3 个,O(n) 计数最直观)。
func completeMarker(label string, all []slowCall) string {
	// 找到自己的耗时
	myMs := 0
	for _, r := range all {
		if r.label == label {
			myMs = r.ms
			break
		}
	}
	// 比自己快的个数 + 1 = 自己的完成排名(耗时升序)
	rank := 1
	for _, r := range all {
		if r.ms < myMs {
			rank++
		}
	}
	if rank == 1 {
		return "★ 最先完成!不是发起顺序 —— 多路复用的证据"
	}
	return fmt.Sprintf("第 %d 个完成(按耗时,不按发起顺序)", rank)
}
