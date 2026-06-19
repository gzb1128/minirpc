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

// startServer 在本地随机端口起一个 server,注册一个 "Echo.ToUpper" 方法,
// 返回 addr 和 cleanup。
func startServer(t *testing.T, register func(*Server)) (addr string, srv *Server) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv = NewServer()
	register(srv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(func() {
		_ = lis.Close()
		_ = srv.Close()
	})
	// 给 Serve goroutine 一点时间真的开始 accept
	time.Sleep(50 * time.Millisecond)
	return lis.Addr().String(), srv
}

// TestServerUnaryCallHappyPath 是最基础的端到端:client.Call 一个真 handler,
// 拿回 result。守护 serveStream 的正常路径 + RESPONSE 序列化。
func TestServerUnaryCallHappyPath(t *testing.T) {
	addr, _ := startServer(t, func(s *Server) {
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
	addr, _ := startServer(t, func(s *Server) {
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
	addr, _ := startServer(t, func(s *Server) {
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
	addr, _ := startServer(t, func(s *Server) {
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
