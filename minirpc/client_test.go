package minirpc

import (
	"encoding/json"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

// client_test.go 守护 client.go 的公开 API。之前 client.go 零测试。
//
// 重点覆盖:
//   - Call happy path(参数编码 / 结果解码)
//   - Call server 返回 error
//   - Call 连接断开 → error
//   - allocateID 的奇偶性(client 用奇数 ID)
//   - Call 多参数(数组打包)
//   - Client.Close 之后资源释放

// startEchoServer 起 server。handler 由 register 注入。
func startEchoServer(t *testing.T, register func(*Server)) (addr string) {
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
	time.Sleep(50 * time.Millisecond)
	return lis.Addr().String()
}

// TestClientCallMultiArg 守护:多参数被打包成 JSON 数组,handler 能逐个解出,
// 返回值也能被 client 正确解码。这是 client.Call 最核心的契约。
func TestClientCallMultiArg(t *testing.T) {
	addr := startEchoServer(t, func(s *Server) {
		s.Register("Math.Add", func(stream *Stream, args json.RawMessage) (any, error) {
			var n []int
			if err := json.Unmarshal(args, &n); err != nil {
				return nil, err
			}
			if len(n) != 2 {
				return nil, errors.New("want 2 args")
			}
			return n[0] + n[1], nil
		})
	})

	c, err := Dial(addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	var sum int
	if err := c.Call("Math.Add", &sum, 10, 20); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if sum != 30 {
		t.Errorf("got %d, want 30", sum)
	}
}

// TestClientCallZeroArg 守护:无参数时 args 是 "[]",handler 也能处理。
func TestClientCallZeroArg(t *testing.T) {
	addr := startEchoServer(t, func(s *Server) {
		s.Register("Misc.Ping", func(stream *Stream, args json.RawMessage) (any, error) {
			return "pong", nil
		})
	})

	c, err := Dial(addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	var out string
	if err := c.Call("Misc.Ping", &out); err != nil {
		t.Fatalf("Call: %v", err)
	}
	if out != "pong" {
		t.Errorf("got %q, want pong", out)
	}
}

// TestClientCallReturnsServerError 守护:server handler 返回 error →
// client.Call 拿到带 server 错误文本的 error。
func TestClientCallReturnsServerError(t *testing.T) {
	addr := startEchoServer(t, func(s *Server) {
		s.Register("Fail.Op", func(stream *Stream, args json.RawMessage) (any, error) {
			return nil, errors.New("boom-from-server")
		})
	})

	c, err := Dial(addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	var out any
	err = c.Call("Fail.Op", &out)
	if err == nil {
		t.Fatal("Call should propagate server error")
	}
	if !strings.Contains(err.Error(), "boom-from-server") {
		t.Errorf("error %q should contain server's message", err.Error())
	}
}

// TestClientCallConnectionClosed 守护:连接断开后,client.Call 返回 error
// (而不是永久阻塞)。
//
// 这条很重要:如果 Call 永久阻塞,说明连接断开没唤醒阻塞中的 Recv。
// 做法:起一个真 server,client 连上、发一个会阻塞的请求,然后关掉 client 连接,
// Call 必须在超时内返回 error。
//
// 注意:handler 用一个可控的 channel 来"卡住",测试结束时关闭它让 handler 退出
// (否则 Server.Close 的 handlerWG.Wait 会一直等)。
func TestClientCallConnectionClosed(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := NewServer()
	block := make(chan struct{})
	srv.Register("Blocked", func(stream *Stream, args json.RawMessage) (any, error) {
		<-block // 卡住直到测试结束关 channel
		return nil, nil
	})
	go func() { _ = srv.Serve(lis) }()
	// 注意:不 defer srv.Close(),因为它会等 handler。我们在断言后手动 close(block) 再 Close。
	time.Sleep(50 * time.Millisecond)
	defer func() {
		close(block) // 让 handler 解除阻塞
		_ = srv.Close()
	}()

	c, err := Dial(lis.Addr().String())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	// 在另一个 goroutine 里发起 Call(它会阻塞,因为 handler 在 <-block)
	var out any
	callErr := make(chan error, 1)
	go func() {
		callErr <- c.Call("Blocked", &out)
	}()

	// 给 Call 一点时间真的发出去并阻塞在 Recv 上
	time.Sleep(100 * time.Millisecond)

	// 主动关 client 连接 —— 这模拟"连接断"
	_ = c.Close()

	// Call 必须返回(error),而不是永久阻塞
	select {
	case <-callErr:
		// 好,返回了。关键是不挂死。
	case <-time.After(2 * time.Second):
		t.Fatal("Call blocked forever after connection closed — should return error")
	}
}

// TestClientAllocateIDIsOdd 守护 client.go 注释里声称的不变量:
// client 分配的 StreamID 永远是奇数(1, 3, 5, ...),且单调递增。
//
// 为什么这个不变量重要?它和 Demo 3 的 progress 流(偶数 ID)配合,
// 保证两类流不撞 ID。如果有人把步进从 +2 改成 +1,这条测试会失败。
//
// allocateID 是私有方法,但测试在包内,可以直接调。
func TestClientAllocateIDIsOdd(t *testing.T) {
	// 不需要真 server —— allocateID 不碰网络。但 Client 需要构造。
	// 用 net.Pipe 起一对连接,a 端包成 Client(它的 readLoop 会跑但不影响测试),
	// b 端留着不动,c.Close() 时关 a → readLoop 退出,b 端在测试结束后由 GC 处理。
	a, b := net.Pipe()
	_ = b // 不用,留着防止 b 端立刻 EOF(虽然这里无所谓)
	c := NewClient(a)
	defer c.Close()

	var prev uint32
	for i := 0; i < 5; i++ {
		id := c.allocateID()
		if id%2 == 0 {
			t.Errorf("allocateID #%d returned %d (even), want odd", i, id)
		}
		if id <= prev {
			t.Errorf("StreamID not monotonic: got %d after %d", id, prev)
		}
		prev = id
	}
}

// TestClientCloseReleasesConnection 守护:Client.Close 后底层 net.Conn 真的关了,
// 之后 Call 会失败(而不是用半开连接)。
func TestClientCloseReleasesConnection(t *testing.T) {
	addr := startEchoServer(t, func(s *Server) {
		s.Register("Noop", func(stream *Stream, args json.RawMessage) (any, error) {
			return nil, nil
		})
	})

	c, err := Dial(addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}

	// 先正常调用一次,确认连接是好的
	var out any
	if err := c.Call("Noop", &out); err != nil {
		t.Fatalf("first Call: %v", err)
	}

	// Close
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Close 后再 Call 应该失败
	err = c.Call("Noop", &out)
	if err == nil {
		t.Error("Call after Close should fail, but returned nil")
	}
}

// TestClientConcurrentCallsDontCross 守护:并发 Call 各自拿到对应的结果,
// 不会因为 StreamID 复用 / 结果串台。这是 client 侧的多路复用正确性。
func TestClientConcurrentCallsDontCross(t *testing.T) {
	addr := startEchoServer(t, func(s *Server) {
		s.Register("Echo.OneArg", func(stream *Stream, args json.RawMessage) (any, error) {
			var a []int
			if err := json.Unmarshal(args, &a); err != nil {
				return nil, err
			}
			if len(a) != 1 {
				return nil, errors.New("want 1 arg")
			}
			return a[0], nil
		})
	})

	c, err := Dial(addr)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	const n = 20
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		i := i
		go func() {
			var got int
			if err := c.Call("Echo.OneArg", &got, i); err != nil {
				errs <- err
				return
			}
			if got != i {
				errs <- errors.New("result crossed")
				return
			}
			errs <- nil
		}()
	}
	for i := 0; i < n; i++ {
		if err := <-errs; err != nil {
			t.Errorf("concurrent call %d: %v", i, err)
		}
	}
}
