package minirpc

import (
	"io"
	"net"
	"testing"
	"time"
)

// conn_test.go 验证多路复用分发的核心:
// 一条 conn(模拟的 TCP)上,两个不同 StreamID 的帧被各自投递到各自的队列,
// 互不阻塞、互不错位。这就是 Demo 2 想看到的机制在底层长什么样。

// dialPipe 用 net.Pipe 造一对内存连接,返回"server 端" + "喂帧用的 client 端"。
//
// 为什么用 net.Pipe 而不是 bytes.Buffer?因为 net.Pipe 是真正的并发安全双向流,
// 而且读空时**阻塞**(像真实 socket),不会像 bytes.Buffer 那样返回 io.EOF 让读循环
// 提前退出 —— 这正是测多路复用"持续读帧"需要的语义。
func dialPipe(t *testing.T) (server *netPipeConn, feeder *netPipeConn) {
	t.Helper()
	a, b := net.Pipe()
	return &netPipeConn{a}, &netPipeConn{b}
}

// netPipeConn 把 net.Pipe 的一端适配成 io.ReadWriter。
type netPipeConn struct{ c net.Conn }

func (n *netPipeConn) Read(p []byte) (int, error)  { return n.c.Read(p) }
func (n *netPipeConn) Write(p []byte) (int, error) { return n.c.Write(p) }
func (n *netPipeConn) Close() error                { return n.c.Close() }

// TestDispatchByStreamID 是 conn.go 的"灵魂测试":
// 把两个不同 StreamID 的帧塞进同一条连接,
// 读循环必须把它们分别投递到两个队列,不能搞混。
func TestDispatchByStreamID(t *testing.T) {
	server, feeder := dialPipe(t)
	defer server.Close()
	defer feeder.Close()
	conn := NewConn(server)

	recv1, err := conn.Register(1)
	if err != nil {
		t.Fatalf("Register 1: %v", err)
	}
	recv2, err := conn.Register(2)
	if err != nil {
		t.Fatalf("Register 2: %v", err)
	}

	// 从 feeder 端塞 4 帧:交错的两条流
	frames := []*Frame{
		{StreamID: 1, Type: TypeData, Payload: []byte("a1")},
		{StreamID: 2, Type: TypeData, Payload: []byte("b1")},
		{StreamID: 1, Type: TypeData, Payload: []byte("a2")},
		{StreamID: 2, Type: TypeData, Payload: []byte("b2")},
	}
	go func() {
		for _, f := range frames {
			if err := WriteFrame(feeder, f); err != nil {
				return
			}
		}
	}()

	// 收 stream 1 的两帧,应该是 a1, a2
	got1 := <-recv1.ch
	if string(got1.Payload) != "a1" {
		t.Errorf("stream1 first: got %q want a1", got1.Payload)
	}
	got1b := <-recv1.ch
	if string(got1b.Payload) != "a2" {
		t.Errorf("stream1 second: got %q want a2", got1b.Payload)
	}
	// 收 stream 2 的两帧,应该是 b1, b2
	got2 := <-recv2.ch
	if string(got2.Payload) != "b1" {
		t.Errorf("stream2 first: got %q want b1", got2.Payload)
	}
	got2b := <-recv2.ch
	if string(got2b.Payload) != "b2" {
		t.Errorf("stream2 second: got %q want b2", got2b.Payload)
	}
}

// TestMultiplexOrderIndependent 验证"完成顺序 ≠ 发起顺序":
// stream 2 的帧先到(对应"快请求先回"),stream 1 的帧后到。
// 这正是 Demo 2 想跑出来的现象,这里在底层就验证了它的可行性。
func TestMultiplexOrderIndependent(t *testing.T) {
	server, feeder := dialPipe(t)
	defer server.Close()
	defer feeder.Close()
	conn := NewConn(server)

	recv1, _ := conn.Register(1)
	recv2, _ := conn.Register(2)

	// 先塞 stream2 的响应,再塞 stream1 的 —— 模拟 server 先完成 2
	go func() {
		WriteFrame(feeder, &Frame{StreamID: 2, Type: TypeResponse, Payload: []byte("fast")})
		WriteFrame(feeder, &Frame{StreamID: 1, Type: TypeResponse, Payload: []byte("slow")})
	}()

	// recv2 应该先收到
	select {
	case f := <-recv2.ch:
		if string(f.Payload) != "fast" {
			t.Errorf("stream2: got %q want fast", f.Payload)
		}
	case <-time.After(time.Second):
		t.Fatal("stream2 did not receive in time")
	}

	select {
	case f := <-recv1.ch:
		if string(f.Payload) != "slow" {
			t.Errorf("stream1: got %q want slow", f.Payload)
		}
	case <-time.After(time.Second):
		t.Fatal("stream1 did not receive in time")
	}
}

// TestOnNewStream 验证 server 模式的"被动开流":
// 读循环遇到未注册的 StreamID,调 onNewStream 回调。
func TestOnNewStream(t *testing.T) {
	server, feeder := dialPipe(t)
	defer server.Close()
	defer feeder.Close()
	conn := NewConn(server)

	ready := make(chan *Stream, 4)
	conn.OnNewStream(func(s *Stream) { ready <- s })

	go func() {
		WriteFrame(feeder, &Frame{StreamID: 7, Type: TypeRequest, Payload: []byte("hello7")})
		WriteFrame(feeder, &Frame{StreamID: 9, Type: TypeRequest, Payload: []byte("hello9")})
	}()

	streams := map[uint32]*Stream{}
	for i := 0; i < 2; i++ {
		select {
		case s := <-ready:
			streams[s.ID()] = s
		case <-time.After(time.Second):
			t.Fatal("onNewStream not called in time")
		}
	}
	if len(streams) != 2 {
		t.Fatalf("expected 2 streams, got %d", len(streams))
	}

	for id, want := range map[uint32]string{7: "hello7", 9: "hello9"} {
		s := streams[id]
		f, err := s.Recv()
		if err != nil {
			t.Fatalf("stream %d Recv: %v", id, err)
		}
		if string(f.Payload) != want {
			t.Errorf("stream %d: got %q want %q", id, f.Payload, want)
		}
	}
}

// TestCloseThenRecvReturnsEOF 验证:流 Close 后,阻塞中的 Recv 会返回 io.EOF,
// 且不会 panic(send-on-closed 之类的坑都规避掉了)。
func TestCloseThenRecvReturnsEOF(t *testing.T) {
	server, feeder := dialPipe(t)
	defer server.Close()
	defer feeder.Close()
	conn := NewConn(server)

	stream, err := conn.OpenStream(1)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}

	recvDone := make(chan error, 1)
	go func() {
		_, err := stream.Recv()
		recvDone <- err
	}()

	time.Sleep(50 * time.Millisecond) // 等 goroutine 进入阻塞 Recv
	stream.Close()

	select {
	case err := <-recvDone:
		if err != io.EOF {
			t.Errorf("Recv after close: got %v want io.EOF", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Recv never returned after Close")
	}
}

// TestUnregisterThenDispatchNoPanic 验证关键的不变量:
// 流被 Unregister(Close)后,读循环再收到该 StreamID 的帧**不会 panic**
// (历史上这里出过 send-on-closed-channel 的 bug)。
func TestUnregisterThenDispatchNoPanic(t *testing.T) {
	server, feeder := dialPipe(t)
	defer server.Close()
	defer feeder.Close()
	conn := NewConn(server)

	stream, err := conn.OpenStream(5)
	if err != nil {
		t.Fatalf("OpenStream: %v", err)
	}
	stream.Close() // 从 map 摘掉,关闭 done

	// 塞一帧到已关闭的 StreamID —— 读循环绝不能 panic,而是丢弃这帧
	WriteFrame(feeder, &Frame{StreamID: 5, Type: TypeData, Payload: []byte("late")})
	time.Sleep(100 * time.Millisecond)

	// 连接还活着:再发一帧到新流,能正常收到
	recv, err := conn.Register(6)
	if err != nil {
		t.Fatalf("Register 6 after close: %v", err)
	}
	WriteFrame(feeder, &Frame{StreamID: 6, Type: TypeData, Payload: []byte("ok")})
	select {
	case f := <-recv.ch:
		if string(f.Payload) != "ok" {
			t.Errorf("stream6: got %q want ok", f.Payload)
		}
	case <-time.After(time.Second):
		t.Fatal("stream6 did not receive — readLoop probably panicked")
	}
}

// TestDuplicateRegister 验证重复注册同一 StreamID 报错。
func TestDuplicateRegister(t *testing.T) {
	server, feeder := dialPipe(t)
	defer server.Close()
	defer feeder.Close()
	conn := NewConn(server)

	if _, err := conn.Register(5); err != nil {
		t.Fatalf("first Register: %v", err)
	}
	if _, err := conn.Register(5); err == nil {
		t.Fatal("second Register should fail")
	}
}
