// Demo 4: 分块大传输(client-streaming)⭐ 补齐"一条逻辑流上持续多帧"
//
// 这个 demo 要回答的问题:
//
//	"如果数据很大(比如一个镜像层 blob),不可能塞进一个 frame,那 RPC 怎么传?"
//
// 答案就是前面三个 demo 一直没直观展示的核心模型 ——
// **一条逻辑流(Stream)不是"发一次就扔",它是一个你可以在上面反复 Send 的通道**:
//
//	client 把大 blob 切成很多块,在**同一条** StreamID 上逐块 Send(TypeData);
//	server 在 **同一条** 流上 Recv 循环,一块一块收、边收边拼,直到 client CloseSend
//	(半关闭发送方向 → server Recv 拿到 io.EOF),再算 SHA256 通过 RESPONSE 返回。
//
// 这正是 containerd 传镜像层的结构:一大块数据 = 一条流上飞很多帧,
// 而不是"一帧来一帧回"。读循环(readLoop)负责把 TCP 字节流里切出来的帧,
// 按 StreamID 推进对应的逻辑流 —— 这条流的 handler 就能源源不断地 Recv 到。
//
// 跟前面 demo 的对照:
//   - Demo 1(unary):      一个流 = 一来一回两帧。最小模型。
//   - Demo 2(multiplex):   多条流并发,但每条仍是一来一回。证明"并发"。
//   - Demo 3(stream):      server→client 单向多帧(progress)。竞态是焦点。
//   - Demo 4(本 demo):     client→server 单向多帧(上传 blob)。聚焦"一条流上持续多帧 + 半关闭"。
//
// 跑法:
//
//	go run ./cmd/upload
//	go run ./cmd/upload -size 64 -total 2048   # 调小块/总量,看块数变化但模型不变
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"github.com/gzb1128/minirpc/minirpc"
)

func main() {
	// ── 命令行参数:控制 blob 大小和分块大小,方便演示"块数变了,逻辑流模型不变" ──
	totalBytes := flag.Int("total", 4096, "要上传的 blob 总字节数")
	chunkSize := flag.Int("size", 256, "每块的最多字节数(实际是字节切分,不保证字符边界)")
	flag.Parse()

	if *totalBytes < 0 || *chunkSize <= 0 {
		log.Fatalf("-total 必须 >= 0,-size 必须 > 0(得到 total=%d size=%d)", *totalBytes, *chunkSize)
	}

	log.Printf("=== Demo 4: 分块大传输 / client-streaming (total=%d bytes, chunkSize=%d) ===",
		*totalBytes, *chunkSize)

	// ── server:注册 UploadService.UploadBlob ──────────────────────────────
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	addr := lis.Addr().String()

	srv := minirpc.NewServer()
	// UploadService.UploadBlob 的 handler:第一帧 REQUEST 只负责分派到这个方法;
	// 真正的数据在**后续同 StreamID 的 DATA 帧**里,由 handler 在 Recv 循环里收。
	//
	// 注意:handler 不自己发 RESPONSE —— 它只要 return (result, err),
	// 框架在它返回后自动发 RESPONSE + TypeClose(见 server.go serveStream)。
	srv.Register("UploadService.UploadBlob", func(stream *minirpc.Stream, args json.RawMessage) (any, error) {
		log.Printf("[server] 收到 stream#%d 的 UploadBlob 请求,开始 Recv 循环收数据块...", stream.ID())

		// server 边收边累算 hash —— 和 client 的累算是独立的,最后比对。
		h := sha256.New()
		var totalBytes int
		chunkIdx := 0
		for {
			f, err := stream.Recv()
			if err == io.EOF {
				// client 用 CloseSend 发了 TypeClose → 排在它前面的 DATA 都被我们
				// Recv 掉之后,这里拿到 io.EOF。这正是"远端发送方向结束"的信号。
				break
			}
			if err != nil {
				return nil, fmt.Errorf("server Recv 第 %d 块失败: %w", chunkIdx, err)
			}
			if f.Type != minirpc.TypeData {
				return nil, fmt.Errorf("期望 DATA 帧,收到 %s", f.Type)
			}

			// payload 是 JSON 编码的字符串块(见 client 侧 Encode)。
			var chunk string
			if err := json.Unmarshal(f.Payload, &chunk); err != nil {
				return nil, fmt.Errorf("decode chunk#%d: %w", chunkIdx, err)
			}
			h.Write([]byte(chunk))
			totalBytes += len(chunk)
			chunkIdx++
			log.Printf("[server]   Recv chunk#%d: %3d bytes  (累计 %d bytes)  %q",
				chunkIdx, len(chunk), totalBytes, preview(chunk))
		}

		log.Printf("[server] stream#%d 收完:共 %d 块 / %d bytes,算 SHA256", stream.ID(), chunkIdx, totalBytes)
		// 把 hex hash 作为 result 返回 → 框架发 RESPONSE。
		return hex.EncodeToString(h.Sum(nil)), nil
	})

	go func() {
		if err := srv.Serve(lis); err != nil {
			log.Printf("[server] Serve: %v", err)
		}
	}()
	defer srv.Close()
	time.Sleep(100 * time.Millisecond) // 让 Serve 真的开始 accept

	// ── client:生成 blob + 切块 + 本地算 hash + 分块推上去 ───────────────
	log.Printf("[client] 拨号 %s —— 全程只用这一条 TCP", addr)
	client, err := minirpc.Dial(addr)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer client.Close()

	blob := makeBlob(*totalBytes)
	chunks := chunkBlob(blob, *chunkSize)

	// client 本地也算一份 hash,用来和 server 返回的对比。
	// 这是顺序敏感的验证:任何一块丢了/乱了,hex 就不一样。
	localHash := sha256.New()
	localHash.Write(blob)
	wantHex := hex.EncodeToString(localHash.Sum(nil))

	log.Printf("[client] blob=%d bytes,切成 %d 块(每块 <= %d bytes),本地 SHA256=%s",
		len(blob), len(chunks), *chunkSize, wantHex)

	// client.Stream 返回原始 *Stream:这是所有流式形态(server/client/bidi)的入口。
	// 这里我们只用它的 client-streaming 能力 —— Send 多帧 + CloseSend + Recv RESPONSE。
	stream, err := client.Stream("UploadService.UploadBlob")
	if err != nil {
		log.Fatalf("Stream: %v", err)
	}
	defer stream.Close()

	// 关键循环:在同一条 StreamID 上反复 Send。每一帧的 header 都带同一个 StreamID,
	// server 的 readLoop 据此把它们全部投递到同一条流 —— handler 的 Recv 才能依次拿到。
	for i, chunk := range chunks {
		payload, err := json.Marshal(string(chunk)) // 文本块:JSON 字符串,日志可读
		if err != nil {
			log.Fatalf("marshal chunk#%d: %v", i, err)
		}
		if err := stream.Send(minirpc.TypeData, payload); err != nil {
			log.Fatalf("Send chunk#%d: %v", i, err)
		}
		log.Printf("[client]   Send chunk#%d: %3d bytes  %q", i+1, len(chunk), preview(string(chunk)))
	}

	// ★ 半关闭:告诉 server"我发完了"。
	// 它发一个 TypeClose 帧(协议层),不影响本地接收队列 —— 调完仍可以 Recv 等 RESPONSE。
	// 不要用 Close()!Close 是本地拆掉接收队列,会让下面的 Recv 直接返回 EOF 拿不到结果。
	if err := stream.CloseSend(); err != nil {
		log.Fatalf("CloseSend: %v", err)
	}
	log.Printf("[client] CloseSend → 发了 TypeClose,server 的 Recv 循环会拿到 io.EOF 退出")

	// 收 server 的 RESPONSE(框架在 handler return 后发的),拿到 server 算的 hash。
	var serverHex string
	for {
		f, err := stream.Recv()
		if err != nil {
			log.Fatalf("client 等 RESPONSE 时流结束: %v", err)
		}
		if f.Type != minirpc.TypeResponse {
			continue // 跳过其它帧(理论上这里只有 RESPONSE)
		}
		var resp minirpc.Response
		if err := json.Unmarshal(f.Payload, &resp); err != nil {
			log.Fatalf("decode response: %v", err)
		}
		if resp.Err != "" {
			log.Fatalf("server 返回错误: %s", resp.Err)
		}
		if err := json.Unmarshal(resp.Result, &serverHex); err != nil {
			log.Fatalf("decode server hash: %v", err)
		}
		break
	}
	// 再 Recv 一次 → io.EOF(框架发的 TypeClose,表示这条流彻底结束)。
	if _, err := stream.Recv(); err != io.EOF {
		log.Fatalf("RESPONSE 之后期望 io.EOF,得到 %v", err)
	}

	// ── 验证 ─────────────────────────────────────────────────────────────
	log.Printf("[client] server SHA256 = %s", serverHex)
	log.Printf("[client] local  SHA256 = %s", wantHex)
	match := serverHex == wantHex
	frameCount := 1 + len(chunks) + 1 + 1 + 1 // REQUEST + N×DATA + CloseSend CLOSE + RESPONSE + 框架 CLOSE
	if match {
		log.Printf("[client] ✅ hash 匹配!%d bytes 完整按序到达", len(blob))
	} else {
		log.Printf("[client] ❌ hash 不符!传输过程中有丢帧/乱序/损坏")
	}

	log.Printf("")
	log.Printf("═══════════════════════════════════════════════════════════════")
	log.Printf("观察结论:")
	log.Printf("  • 全程 1 条 TCP + 1 条逻辑流(stream#%d),上面飞了 %d 个 frame:", stream.ID(), frameCount)
	log.Printf("      1 REQUEST(分派 method)+ %d DATA(逐块)+ 1 CLOSE(client CloseSend)", len(chunks))
	log.Printf("      + 1 RESPONSE(server 返回 hash)+ 1 CLOSE(框架发,流结束)")
	log.Printf("  • 第一帧 REQUEST 只做分派;后续同 StreamID 的 DATA 由 readLoop 投递到同一条流,")
	log.Printf("    handler 在 Recv 循环里逐块消费 —— 这就是'逻辑流上持续多帧'的模型。")
	log.Printf("  • CloseSend 是协议帧(远端 EOF),不是本地 Close(拆接收队列)。")
	log.Printf("    调完 CloseSend 仍能 Recv 到 server 的 RESPONSE。")
	log.Printf("  • SHA256 对比是顺序敏感的:任何一块乱序/丢失,hash 立刻不符。")
	if !match {
		log.Printf("  • ❌ 本次 hash 不符,说明验证机制成功抓到了传输异常。")
	}
	log.Printf("  • 对应真实世界:containerd 传镜像层 blob = 一条 ttrpc 流上飞大量 DATA 帧,")
	log.Printf("    而不是塞进单个请求。'大对象' 必须靠流式分块。")
	log.Printf("═══════════════════════════════════════════════════════════════")
}

// makeBlob 生成一段可重复的伪 blob 文本。
// 用固定模式拼接,保证同样 totalBytes 每次生成的字节完全一致(便于复现 hash)。
// 内容形如 "UploadBlob-Demo4-0001-UploadBlob-Demo4-0002-...",纯 ASCII,日志里能读。
func makeBlob(totalBytes int) []byte {
	if totalBytes == 0 {
		return nil
	}
	buf := make([]byte, 0, totalBytes)
	for i := 0; len(buf) < totalBytes; i++ {
		buf = append(buf, []byte(fmt.Sprintf("UploadBlob-Demo4-%04d-", i))...)
	}
	return buf[:totalBytes] // 截到正好 totalBytes(最后那个模式可能被截断,无所谓)
}

// chunkBlob 把 blob 按**字节**切成 size 上限的块。
// 为什么按字节不按字符?因为传输对象本质是字节流 blob(镜像层就是字节),
// SHA256 也按字节算 —— 只要 client 和 server 用同样字节拼回就对得上。
// 多字节 UTF-8 字符可能被切断,这是字节流的真实表现(见 main_test.go 的用例)。
//
// size 必须 > 0,否则 panic(零长度块没意义,是调用方 bug)。
func chunkBlob(blob []byte, size int) [][]byte {
	if size <= 0 {
		panic(fmt.Sprintf("chunkBlob: size 必须 > 0,得到 %d", size))
	}
	var chunks [][]byte
	for start := 0; start < len(blob); start += size {
		end := start + size
		if end > len(blob) {
			end = len(blob)
		}
		chunks = append(chunks, blob[start:end])
	}
	return chunks
}

// preview 截取一段可读预览用于日志(只显示前若干字符,避免刷屏)。
func preview(s string) string {
	const max = 24
	if len(s) <= max {
		return s
	}
	return s[:max] + "…"
}
