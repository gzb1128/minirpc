// Demo 3: 流式 RPC + 竞态复现 ⭐ 对应真实 bug(containerd PR #13625)
//
// 这个 demo 是整个项目的高潮。它把"多路复用导致 RPC 响应与 progress 流异步"
// 这个抽象问题,还原成**可复现、可修复**的代码。
//
// 场景(模仿 containerd 的 Import):
//   - client 想导入一个镜像(Import)。这个操作有两部分产出:
//     (a) 一个最终结果(RPC 返回值,代表"导入完成")
//     (b) 一串进度事件(33% → 66% → done,边导入边推)
//   - 两部分产出走**两条独立的逻辑流**(因为多路复用允许这么做):
//     Stream A:client.Call(Import) 阻塞等 RESPONSE(代表"导入完成")
//     Stream B:server 持续推 progress,client 有个 goroutine 不断 Recv
//
// 竞态点(关键!):
//
//	server 推完最后一条 progress(done)和发"Import 完成"的 RESPONSE 是**紧挨着**的,
//	两条流共用一条 TCP。于是 client 这边:
//	  • 主流程的 client.Call 一收到 RESPONSE 就解阻塞 ——
//	    但此刻 Stream B 的最后一条 progress 可能还躺在 channel 里没被 goroutine Recv 掉!
//	  • 如果主流程紧接着去读 progressCount → 可能读到不全(漏了 done)。
//
// 这就是真实代码的结构。我们用两个模式跑出来:
//
//	--buggy:主流程不等 progress goroutine 就读计数 → 偶发漏收
//	--fixed:主流程 join(goroutine 排空后再读)→ 稳定全收
//
// 跑法:
//
//	go run ./cmd/stream --buggy -count 20    # 会看到偶发 "progressCount < 3"
//	go run ./cmd/stream --fixed -count 20    # 全部 == 3
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gzb1128/minirpc/minirpc"
)

// Progress 是流里的一条数据(Demo 3 专用的 payload 结构,见 CONTEXT.md 3.2)。
type Progress struct {
	Event string `json:"event"` // "progress" / "done"
	Value int    `json:"value"` // 百分比
}

func main() {
	// ── 命令行参数 ──────────────────────────────────────────────────────────
	mode := flag.String("mode", "buggy", "运行模式: buggy(复现竞态) 或 fixed(join 修复)")
	count := flag.Int("count", 1, "重复跑多少轮(用来让 buggy 模式暴露偶发竞态)")
	flag.Parse()

	if *mode != "buggy" && *mode != "fixed" {
		log.Fatalf("--mode 只能是 buggy 或 fixed,得到 %q", *mode)
	}

	log.Printf("=== Demo 3: 流式 RPC + 竞态复现 (mode=%s, count=%d) ===", *mode, *count)

	// ── server ─────────────────────────────────────────────────────────────
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()

	srv := minirpc.NewServer()
	// ImportService.Import 的参数:[name, progressStreamID]
	//   name               :镜像名(教学用,不真导入)
	//   progressStreamID   :client 告诉 server "把 progress 推到这条流"
	//
	// 为什么要把 progressStreamID 当参数传?因为我们要让 server 同时操作两条流:
	//   • 主流(自己这条 stream):干完活后发 RESPONSE(代表 Import 完成)
	//   • progress 流(另一条 StreamID):边干边推 progress
	// 这正是 containerd Import 的结构:返回值 + 一个独立的 progress stream。
	srv.Register("ImportService.Import", func(stream *minirpc.Stream, args json.RawMessage) (any, error) {
		var params []any
		if err := json.Unmarshal(args, &params); err != nil {
			return nil, err
		}
		// 防御性解析:坏请求返回 error,而不是 panic。
		// (server handler 跑在独立 goroutine 里没有 recover,panic 会让 client 永久挂起。)
		if len(params) < 2 {
			return nil, fmt.Errorf("Import wants 2 args, got %d", len(params))
		}
		name, ok := params[0].(string)
		if !ok {
			return nil, fmt.Errorf("Import arg[0] must be string, got %T", params[0])
		}
		progressIDFloat, ok := params[1].(float64)
		if !ok {
			return nil, fmt.Errorf("Import arg[1] must be number, got %T", params[1])
		}
		progressStreamID := uint32(progressIDFloat)

		conn := stream.Conn() // 通过 conn 往"另一条流"写 progress

		// 模拟导入过程:推 33% → 66% → done 三条 progress,然后发完成。
		// 为了让竞态更容易复现,这里用极短的间隔(否则在 buggy 模式下
		// goroutine 总是有时间跑完,看不出问题)。
		steps := []Progress{
			{Event: "progress", Value: 33},
			{Event: "progress", Value: 66},
			{Event: "done", Value: 100},
		}
		for _, p := range steps {
			payload, err := json.Marshal(p)
			if err != nil {
				return nil, fmt.Errorf("marshal progress: %w", err)
			}
			// 关键:progress 推到 progressStreamID,不是主流自己的 ID。
			// 检查 Send 错误:写失败说明 client 那边 progress 流可能已关 / 连接断,
			// 再发下去没意义,提前结束。否则会被误当成 Demo 3 的竞态,混淆诊断。
			if err := conn.Send(&minirpc.Frame{StreamID: progressStreamID, Type: minirpc.TypeData, Payload: payload}); err != nil {
				log.Printf("[server] 推 progress 到 stream#%d 失败,停止:%v", progressStreamID, err)
				return nil, fmt.Errorf("send progress: %w", err)
			}
		}
		// 紧接着返回主流的"完成"(handler return → 框架发 RESPONSE)。
		// 注意:此时 progress 流的最后一条 DATA 可能还在网络/on-the-wire,
		// 或已到 client 但还躺在 channel 没被 Recv —— 竞态窗口就在这。
		_ = name
		return "imported", nil
	})

	go func() { _ = srv.Serve(lis) }()
	defer srv.Close()
	time.Sleep(100 * time.Millisecond)

	// ── client:跑 count 轮,统计 progressCount 分布 ──────────────────────
	client, err := minirpc.Dial(addr)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer client.Close()

	var (
		fullCount  int32 // progressCount == 3 的轮数(正确)
		shortCount int32 // progressCount < 3 的轮数(竞态漏收)
	)

	var wg sync.WaitGroup
	for i := 1; i <= *count; i++ {
		wg.Add(1)
		round := i
		go func() {
			defer wg.Done()
			got := runOnce(client, *mode)
			if got == 3 {
				atomic.AddInt32(&fullCount, 1)
			} else {
				atomic.AddInt32(&shortCount, 1)
				log.Printf("  ⚠️  round %d: progressCount = %d (期望 3) —— 漏收了!", round, got)
			}
		}()
	}
	wg.Wait()

	// ── 汇总 ──────────────────────────────────────────────────────────────
	log.Printf("")
	log.Printf("═══════════════════════════════════════════════════════════════")
	log.Printf("模式 %s,跑了 %d 轮:", *mode, *count)
	log.Printf("  progressCount == 3(正确): %d", fullCount)
	log.Printf("  progressCount <  3(漏收): %d", shortCount)
	switch *mode {
	case "buggy":
		if shortCount > 0 {
			log.Printf("  ✅ 成功复现竞态!buggy 模式下确实会偶发漏收 progress。")
		} else {
			log.Printf("  (本轮没复现到 —— 竞态是间歇性的,多跑几次或加大 -count 试试)")
		}
	case "fixed":
		if shortCount == 0 {
			log.Printf("  ✅ fixed 模式下全部正确收到 3 条 progress,join 修复有效。")
		} else {
			log.Printf("  ❌ fixed 模式居然也漏收了?这是 bug,请检查实现。")
		}
	}
	log.Printf("═══════════════════════════════════════════════════════════════")
}

// runOnce 跑一轮 Import,返回这一轮收到的 progress 条数。
//
// 这里的代码结构是"还原真实代码"的(不是故意 sleep 制造竞态):
//   - 一个 goroutine 不停 Recv progress
//   - 主流程 Call Import,阻塞等完成
//   - 主流程拿到完成信号后读 progressCount
//
// buggy / fixed 模式的唯一差异在主流程"拿完完成信号后做什么"。
func runOnce(client *minirpc.Client, mode string) int {
	conn := client.Conn()

	// ── Stream B:progress 流 ──────────────────────────────────────────────
	// 我们先开一条 progress 流,把它的 StreamID 告诉 server。
	// server 收到 Import 请求时,会往这个 StreamID 推 DATA。
	//
	// 为什么需要 client 先开?因为 client 用"主动模式"(知道自己的 StreamID);
	// server 是被动收到这个 ID 后才知道往哪推。这是 client-stream 场景下自然的顺序。
	progressStreamID := allocProgressStreamID()
	recv, err := conn.Register(progressStreamID)
	if err != nil {
		log.Printf("register progress stream: %v", err)
		return 0
	}
	progressStream := minirpc.NewStreamFromChan(progressStreamID, conn, recv)
	defer progressStream.Close()

	// ── progress 消费 goroutine ───────────────────────────────────────────
	// 这对应真实代码里"消费 progress 的 goroutine"(containerd proxy 的 progress goroutine)。
	var progressCount int
	done := make(chan struct{}) // progress goroutine 排空后关闭(供 fixed 模式 join)
	go func() {
		defer close(done)
		for {
			f, err := progressStream.Recv()
			if err != nil {
				return // io.EOF / 流结束 → 退出
			}
			if f.Type == minirpc.TypeData {
				progressCount++
			}
		}
	}()

	// ── Stream A:主调用 ──────────────────────────────────────────────────
	// client.Call 阻塞等 Import 的 RESPONSE。注意:它和 progress 是两条独立流,
	// 在同一条 TCP 上多路复用。谁先到不保证 —— 这就是竞态的根源。
	var result string
	if err := client.Call("ImportService.Import", &result, "myimage", progressStreamID); err != nil {
		log.Printf("Call Import: %v", err)
		return progressCount
	}

	// ╔══════════════════════════════════════════════════════════════════╗
	// ║  ★ buggy / fixed 的唯一差异就在下面这块 ★                          ║
	// ║                                                                    ║
	// ║  Call 返回 = Stream A 收到了 RESPONSE,只代表"Import 完成"。          ║
	// ║  它**不代表** Stream B 的 progress 已经被 goroutine 消费完。         ║
	// ║  最后一条 progress(done)此刻可能还躺在 progressCh 里没被 Recv 掉。 ║
	// ╚══════════════════════════════════════════════════════════════════╝
	if mode == "fixed" {
		// ✅ FIXED:先 join progress goroutine,确保它把 channel 里所有 DATA
		// 都 Recv 掉了,再读 progressCount。这就是 containerd PR #13625 的修复手法
		// (加一个 done channel,主流程等它关闭)。
		// 需要先 Close progress stream,让 Recv 返回 EOF,gofunc 才会退出。
		progressStream.Close()
		<-done
	} else {
		// ❌ BUGGY:不 join,直接读。这还原了"真实但漏了 join"的代码结构 ——
		// 竞态自然发生:有时 goroutine 还没 Recv 完最后一条,我们就读了计数。
		// 不加任何 artificial sleep,让它真实地"间歇性"漏收。
		progressStream.Close()
		// 这里如果加 <-done 就和 fixed 一样了;buggy 故意不加。
		_ = done
	}

	return progressCount
}

// ── 辅助:client 侧自己分配 progress 流的 StreamID ──────────────────────
//
// 主调用 Call 内部会自动分配奇数 StreamID(1,3,5,...,见 client.go)。
// progress 流是 client 自己开的另一条流,我们用一个独立的偶数计数器,
// 保证不与主调用的 ID 冲突(这也是 http2/ttrpc 的惯例)。
var progressIDCounter atomic.Uint32

func init() { progressIDCounter.Store(2) } // 偶数起

func allocProgressStreamID() uint32 {
	for {
		cur := progressIDCounter.Load()
		next := cur + 2
		if progressIDCounter.CompareAndSwap(cur, next) {
			return cur
		}
	}
}
