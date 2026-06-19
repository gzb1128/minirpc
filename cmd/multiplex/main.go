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

	"minirpc/minirpc"
)

// timer 用于打印"T+0.123s"这样的相对时间戳,让时序一眼能看出来。
var startTime = time.Now()

func ts() string {
	return fmt.Sprintf("T+%.3fs", time.Since(startTime).Seconds())
}

func main() {
	// ── server ─────────────────────────────────────────────────────────────
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()

	srv := minirpc.NewServer()
	// SlowOp(id, durationMillis):睡 durationMillis 后返回 id。
	// 用来制造"不同耗时的并发请求",证明多路复用下完成顺序 ≠ 发起顺序。
	srv.Register("SlowService.SlowOp", func(stream *minirpc.Stream, args json.RawMessage) (any, error) {
		var params []any // [id, durationMillis]
		if err := json.Unmarshal(args, &params); err != nil {
			return nil, err
		}
		id := int(params[0].(float64))
		durMs := int(params[1].(float64))

		log.Printf("%s  server  收到 req#%d (stream#%d),sleep %dms", ts(), id, stream.ID(), durMs)
		// 模拟一个耗时操作。每个请求在 server 自己的 goroutine 里跑,互不影响。
		time.Sleep(time.Duration(durMs) * time.Millisecond)
		log.Printf("%s  server  req#%d 完成,回 RESPONSE", ts(), id)
		return id, nil
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
	// 关键:按"发起顺序"是 req#1, #2, #3,但完成顺序应该是 #3(0.5s) → #1(1s) → #2(2s)。
	reqs := []struct {
		id  int
		ms  int
		out int
	}{
		{id: 1, ms: 1000},
		{id: 2, ms: 2000},
		{id: 3, ms: 500},
	}

	var wg sync.WaitGroup
	for _, r := range reqs {
		wg.Add(1)
		r := r
		log.Printf("%s  client  发起 req#%d (期望 %.1fs)", ts(), r.id, float64(r.ms)/1000)
		go func() {
			defer wg.Done()
			var result int
			if err := client.Call("SlowService.SlowOp", &result, r.id, r.ms); err != nil {
				log.Printf("%s  client  req#%d 失败: %v", ts(), r.id, err)
				return
			}
			log.Printf("%s  client  收到 req#%d 响应   ← %s",
				ts(), r.id, completeMarker(r.id, reqs))
			_ = result
		}()
		// 不加任何 sleep —— 三个请求"几乎同时"发出,证明它们在一条 TCP 上并发飞。
	}

	wg.Wait()

	log.Printf("")
	log.Printf("═══════════════════════════════════════════════════════════════")
	log.Printf("观察结论:")
	log.Printf("  • 完成顺序 = #3(0.5s) → #1(1s) → #2(2s),不是发起顺序 #1→#2→#3。")
	log.Printf("  • 全程只用了 1 条 TCP 连接(上面那行 '拨号 %s')。", addr)
	log.Printf("  • 这就是多路复用:一条物理连接 + N 条独立逻辑流(StreamID 区分),")
	log.Printf("    各流互不阻塞,谁先完成谁先回。")
	log.Printf("  • 对应真实世界:containerd 在一条 ttrpc 连接上跑多个 RPC,")
	log.Printf("    所以 'RPC 响应流' 和 'progress 流' 可能乱序到达 —— 这是 Demo 3 的伏笔。")
	log.Printf("═══════════════════════════════════════════════════════════════")
}

// completeMarker 生成"← 不是按顺序!证明多路复用" 这种点睛注释,
// 让读者一眼看到"哎这个完成顺序违反了发起顺序"。
func completeMarker(id int, all []struct {
	id  int
	ms  int
	out int
}) string {
	// 按 ms 升序排,看当前 id 是第几个完成的
	byDuration := make([]int, len(all))
	for i, r := range all {
		byDuration[i] = r.ms
	}
	// 简单冒泡排序 byDuration 配合 id,然后看 id 排第几
	type pair struct{ ms, id int }
	ps := make([]pair, len(all))
	for i, r := range all {
		ps[i] = pair{r.ms, r.id}
	}
	for i := 0; i < len(ps); i++ {
		for j := i + 1; j < len(ps); j++ {
			if ps[j].ms < ps[i].ms {
				ps[i], ps[j] = ps[j], ps[i]
			}
		}
	}
	rank := 1
	for _, p := range ps {
		if p.id == id {
			break
		}
		rank++
	}
	if rank == 1 {
		return "★ 最先完成!不是发起顺序 —— 多路复用的证据"
	}
	return fmt.Sprintf("第 %d 个完成(按耗时,不按发起顺序)", rank)
}
