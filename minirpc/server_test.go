package minirpc

import (
	"encoding/json"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

// server_test.go 做端到端(server + 真 client 走 net.Pipe / 真端口)+ 错误路径测试。
//
// 之前 server.go 整个文件零测试,所以以下分支都没被覆盖:
//   - serveStream 的 happy path(handler 返回 result → RESPONSE)
//   - handler 返回 err → RESPONSE.Err
//   - 未知 method → 错误 RESPONSE
//   - 坏 payload → 错误 RESPONSE
//   - Server.Close 的优雅关闭语义(等 handler 跑完)

// startServer 在本地随机端口起一个 server,通过 register 注入 handler,
// 返回 addr。server 和 listener 都注册了 t.Cleanup 自动关闭,调用方不用管。
func startServer(t *testing.T, register func(*Server)) (addr string) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := NewServer()
	register(srv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		_ = lis.Close()
		_ = srv.Close()
	})
	// 给 Serve goroutine 一点时间真的开始 accept
	time.Sleep(50 * time.Millisecond)
	return lis.Addr().String()
}

// TestServerUnaryCallHappyPath 是最基础的端到端:client.Call 一个真 handler,
// 拿回 result。守护 serveStream 的正常路径 + RESPONSE 序列化。
func TestServerUnaryCallHappyPath(t *testing.T) {
	addr := startServer(t, func(s *Server) {
		s.Register("Echo.ToUpper", func(stream *Stream, args json.RawMessage) (any, error) {
			var s []string
			if err := json.Unmarshal(args, &s); err != nil {
				return nil, err
			}
			if len(s) != 1 {
				return nil, errors.New("want 1 string arg")
			}
			return strings.ToUpper(s[0]), nil
		})
	})

	client, err := Dial(addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer client.Close()

	var out string
	if err := client.Call("Echo.ToUpper", &out, "hello"); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if out != "HELLO" {
		t.Errorf("got %q, want HELLO", out)
	}
}

// TestServerHandlerReturnsError 守护:handler return err → RESPONSE.Err → client 收到 error。
func TestServerHandlerReturnsError(t *testing.T) {
	addr := startServer(t, func(s *Server) {
		s.Register("Bad.Op", func(stream *Stream, args json.RawMessage) (any, error) {
			return nil, errors.New("intentional failure")
		})
	})

	client, err := Dial(addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer client.Close()

	var out any
	err = client.Call("Bad.Op", &out)
	if err == nil {
		t.Fatal("Call should return error for failing handler")
	}
	if !strings.Contains(err.Error(), "intentional failure") {
		t.Errorf("error %q should contain server's err text", err.Error())
	}
}

// TestServerUnknownMethod 守护:调用没注册的 method → 错误 RESPONSE。
func TestServerUnknownMethod(t *testing.T) {
	addr := startServer(t, func(s *Server) {
		// 注册一个无关的方法
		s.Register("Something.Else", func(stream *Stream, args json.RawMessage) (any, error) {
			return nil, nil
		})
	})

	client, err := Dial(addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer client.Close()

	var out any
	err = client.Call("Does.Not.Exist", &out)
	if err == nil {
		t.Fatal("Call to unknown method should error")
	}
	if !strings.Contains(err.Error(), "unknown method") {
		t.Errorf("error %q should mention 'unknown method'", err.Error())
	}
}

// TestServerCloseWaitsForHandlers 守护我们这次修复的核心:Server.Close 必须等
// 正在跑的 handler 跑完,而不是只等连接关闭。
//
// 做法:注册一个 sleep 200ms 的 handler,client 发请求后(此时 handler 在跑),
// 立刻调 Server.Close()。如果 Close 不等 handler,Close 会很快返回,
// 但 handler 还在 sleep —— 我们用一个 channel 记录 handler 是否跑完。
func TestServerCloseWaitsForHandlers(t *testing.T) {
	handlerDone := make(chan struct{})

	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := NewServer()
	srv.Register("Slow.Op", func(stream *Stream, args json.RawMessage) (any, error) {
		time.Sleep(200 * time.Millisecond)
		close(handlerDone)
		return "done", nil
	})
	go func() { _ = srv.Serve(lis) }()
	time.Sleep(50 * time.Millisecond)

	client, err := Dial(lis.Addr().String())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	// 发请求(handler 在跑),不等结果
	go func() {
		var out string
		_ = client.Call("Slow.Op", &out)
	}()

	// 等 handler 真的进入 sleep(给它一点时间被调度)
	time.Sleep(50 * time.Millisecond)

	// 关 server。关键断言:Close 必须等到 handlerDone。
	closeDone := make(chan struct{})
	go func() {
		_ = srv.Close()
		close(closeDone)
	}()

	select {
	case <-handlerDone:
		// 好,handler 跑完了
	case <-time.After(2 * time.Second):
		t.Fatal("Server.Close returned before handler finished — Close should wait for in-flight handlers")
	}

	// Close 也应该最终返回
	select {
	case <-closeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Server.Close never returned")
	}

	_ = client.Close()
}

// TestServerCloseIdempotent 守护 Close 幂等(内部用 closeOnce)。
func TestServerCloseIdempotent(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := NewServer()
	go func() { _ = srv.Serve(lis) }()
	time.Sleep(50 * time.Millisecond)

	for i := 0; i < 3; i++ {
		if err := srv.Close(); err != nil {
			t.Fatalf("Close #%d: %v", i, err)
		}
	}
}

// TestServerConcurrentCalls 守护:同一连接上并发多个调用,各自拿到正确结果
// (不会串 StreamID / 串结果)。这是 Demo 2 在测试层面的等价物。
func TestServerConcurrentCalls(t *testing.T) {
	addr := startServer(t, func(s *Server) {
		s.Register("Math.Double", func(stream *Stream, args json.RawMessage) (any, error) {
			var nums []int
			if err := json.Unmarshal(args, &nums); err != nil {
				return nil, err
			}
			if len(nums) != 1 {
				return nil, errors.New("want 1 arg")
			}
			return nums[0] * 2, nil
		})
	})

	client, err := Dial(addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer client.Close()

	const n = 10
	type result struct {
		in, out int
		err     error
	}
	results := make([]result, n)
	done := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			var out int
			err := client.Call("Math.Double", &out, i)
			results[i] = result{in: i, out: out, err: err}
			done <- struct{}{}
		}()
	}
	for i := 0; i < n; i++ {
		<-done
	}
	for i, r := range results {
		if r.err != nil {
			t.Errorf("call %d: %v", i, r.err)
			continue
		}
		if r.out != r.in*2 {
			t.Errorf("call in=%d: got out=%d, want %d (results may be crossed — multiplexing bug)", r.in, r.out, r.in*2)
		}
	}
}

// TestServerResponseClosePairing 守护 serveStream 的协议契约:一次 unary 调用
// 必须产生**恰好一个 RESPONSE + 恰好一个 CLOSE**(对应 frame.go 里 TypeResponse 的注释
// "一次调用有且仅有一个 Response")。
//
// 为什么要数帧?因为 client.Call 只看第一个 RESPONSE 就返回,多发的 RESPONSE 会被
// 静默丢弃 —— 没有这个测试的话,一个"错误路径发了两次 RESPONSE"或"漏发 CLOSE"
// 的回归会无声通过(Call 仍然成功),但流式消费者会因此挂死。这个测试数帧,
// 不让任何一种回归漏掉。
//
// 做法:不开 server,直接用一对 net.Pipe 把"client 写 REQUEST"和"server 端跑 serveStream"
// 接起来,然后在 client 端数收到的所有帧的 Type。
func TestServerResponseClosePairing(t *testing.T) {
	// a 端 = server conn(serveStream 往它写 RESPONSE/CLOSE);b 端 = client 读这些帧。
	a, b := net.Pipe()

	conn := NewConn(a) // server 端,自带 readLoop

	srv := NewServer()
	srv.Register("P.Count", func(stream *Stream, args json.RawMessage) (any, error) {
		return 42, nil
	})
	conn.OnNewStream(func(stream *Stream) {
		srv.handlerWG.Add(1)
		go func() {
			defer srv.handlerWG.Done()
			srv.serveStream(stream)
		}()
	})

	// client 端:OpenStream + 发 REQUEST。
	clientConn := NewConn(b) // client 端,自带 readLoop
	stream, err := clientConn.OpenStream(1)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	req := Request{Method: "P.Count", Args: json.RawMessage("[]")}
	payload, _ := Encode(req)
	if err := stream.Send(TypeRequest, payload); err != nil {
		t.Fatalf("send REQUEST: %v", err)
	}

	// 数收到的帧的 Type,直到 io.EOF。期望顺序:RESPONSE, CLOSE(→EOF)。
	var seen []FrameType
	for {
		f, err := stream.Recv()
		if err != nil {
			break // io.EOF / 任何错误都结束数帧
		}
		seen = append(seen, f.Type)
	}

	// 清理:关 pipe 两端 → 两个 readLoop 退出。srv.Close 等 handlerWG
	// (保证 serveStream 不会往已关的 a 写,触发 race)。
	_ = a.Close()
	_ = b.Close()
	<-conn.Done()
	<-clientConn.Done()
	_ = srv.Close()

	// 断言:恰好一个 RESPONSE,然后 CLOSE 转成 EOF。
	// (注意循环变量不能用 t,会和 *testing.T 参数重名 —— 用 ft。)
	gotResponse := 0
	for _, ft := range seen {
		if ft == TypeResponse {
			gotResponse++
		}
	}
	if gotResponse != 1 {
		t.Errorf("expected exactly 1 RESPONSE frame, got %d (full sequence: %v)", gotResponse, seen)
	}
	// 不应该看到第二类不该出现的帧(比如重复 RESPONSE,或 DATA)。
	for i, ft := range seen {
		if ft != TypeResponse && ft != TypeClose {
			t.Errorf("frame %d: unexpected type %v in unary call", i, ft)
		}
	}
}
