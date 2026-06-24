package minirpc

import (
	"io"
	"net"
	"strings"
	"testing"
	"time"
)

// stream_test.go 守护 stream.go 里最微妙的不变量:
// 本地 Close 和远端 TypeClose 是两种不同语义,不能混在一起。
//
//   - 本地 Close 关闭 recv.done,表示调用方放弃这条流,Recv 应该尽快 EOF。
//   - 远端 TypeClose 是排在 recv.ch 里的协议帧,Recv 应该先按 FIFO 读完前面的 DATA,
//     再在 TypeClose 上返回 io.EOF。

// newBareStream 用内部构造直接造一条 stream(不依赖真实网络),
// 精确控制"队列里有什么"。返回 (stream, recv),recv.close() 可模拟本地 Close
// 的"关 done"那一步(但不走 Unregister,所以不碰 conn,conn 可以为 nil)。
//
// 这种"绕过 conn 直接测 Recv 语义"的写法,正是 unit test 应该做的:
// 把被测的最小单元(Stream.Recv)孤立出来,精确喂输入。
func newBareStream(t *testing.T) (*Stream, *streamRecv) {
	t.Helper()
	recv := newStreamRecv()
	// conn 传 nil:这些测试只调 Recv(不调 Send / Close),不会碰 conn。
	s := newStream(42, nil, recv)
	return s, recv
}

// newLiveStream 起一对 net.Pipe + 一个真 Conn,返回 server 端的 stream。
// 用于需要调 Close()(它内部要走 conn.Unregister)的测试。
func newLiveStream(t *testing.T) (*Stream, *Conn, func()) {
	t.Helper()
	a, b := net.Pipe()
	conn := NewConn(a) // server 端 conn,自带 readLoop
	recv, err := conn.Register(7)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	s := newStream(7, conn, recv)
	cleanup := func() {
		b.Close()
		<-conn.Done()
	}
	return s, conn, cleanup
}

// TestRecvReturnsEOFAfterLocalCloseWithPendingFrames 守护本地 Close 语义:
// done 被关闭表示调用方放弃这条流,即使队列里还有已投递帧,Recv 也应该 EOF。
func TestRecvReturnsEOFAfterLocalCloseWithPendingFrames(t *testing.T) {
	s, recv := newBareStream(t)

	// 投 3 帧进队列,再模拟本地 Close。
	frames := []*Frame{
		{StreamID: 42, Type: TypeData, Payload: []byte("a")},
		{StreamID: 42, Type: TypeData, Payload: []byte("b")},
		{StreamID: 42, Type: TypeData, Payload: []byte("c")},
	}
	for _, f := range frames {
		recv.ch <- f
	}
	recv.close()

	_, err := s.Recv()
	if err != io.EOF {
		t.Fatalf("Recv after local close with pending frames: got %v, want io.EOF", err)
	}
}

// TestRecvReturnsEOFOnEmptyQueueClose 守护另一个端点:
// 队列空 + done 关 → 立即返回 io.EOF(不要死锁或空转)。
func TestRecvReturnsEOFOnEmptyQueueClose(t *testing.T) {
	s, recv := newBareStream(t)
	recv.close() // 队列空就关 done

	done := make(chan error, 1)
	go func() {
		_, err := s.Recv()
		done <- err
	}()

	select {
	case err := <-done:
		if err != io.EOF {
			t.Fatalf("Recv on closed-empty stream: got %v, want io.EOF", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Recv on closed-empty stream blocked forever — should return io.EOF promptly")
	}
}

// TestRecvBlocksWhenQueueEmptyAndDoneOpen 守护"正常等待"语义:
// 队列空 + done 未关 → Recv 阻塞(不立即返回 nil/EOF)。
// 用一个 goroutine 投帧来"唤醒"它。
func TestRecvBlocksWhenQueueEmptyAndDoneOpen(t *testing.T) {
	s, recv := newBareStream(t)

	got := make(chan *Frame, 1)
	go func() {
		f, err := s.Recv()
		if err != nil {
			t.Errorf("Recv: %v", err)
			return
		}
		got <- f
	}()

	// 短暂等待,确认 Recv 真的阻塞了(没立刻返回)
	select {
	case f := <-got:
		t.Fatalf("Recv returned %q before any frame was sent — should have blocked", f.Payload)
	case <-time.After(50 * time.Millisecond):
		// 好,确实在阻塞
	}

	// 投一帧,Recv 应该被唤醒
	recv.ch <- &Frame{StreamID: 42, Type: TypeData, Payload: []byte("hello")}
	select {
	case f := <-got:
		if string(f.Payload) != "hello" {
			t.Errorf("got %q, want hello", f.Payload)
		}
	case <-time.After(time.Second):
		t.Fatal("Recv did not wake up after frame was sent")
	}
}

// TestRecvTypeCloseFrameReturnsEOF 守护"收到 TypeClose 帧 → io.EOF"的转换。
// 这是 server 显式发"流结束"信号的语义。
func TestRecvTypeCloseFrameReturnsEOF(t *testing.T) {
	s, recv := newBareStream(t)
	recv.ch <- &Frame{StreamID: 42, Type: TypeClose, Payload: nil}
	_, err := s.Recv()
	if err != io.EOF {
		t.Fatalf("Recv on TypeClose frame: got %v, want io.EOF", err)
	}
}

// TestRecvDrainsDataBeforeRemoteTypeClose 守护远端 close frame 的 FIFO 语义:
// DATA 和 TypeClose 都在 recv.ch 里,所以 TypeClose 前面的 DATA 必须先被读到。
func TestRecvDrainsDataBeforeRemoteTypeClose(t *testing.T) {
	s, recv := newBareStream(t)
	recv.ch <- &Frame{StreamID: 42, Type: TypeData, Payload: []byte("a")}
	recv.ch <- &Frame{StreamID: 42, Type: TypeData, Payload: []byte("b")}
	recv.ch <- &Frame{StreamID: 42, Type: TypeClose, Payload: nil}

	for i, want := range []string{"a", "b"} {
		got, err := s.Recv()
		if err != nil {
			t.Fatalf("frame %d before TypeClose: Recv returned %v, want %q", i, err, want)
		}
		if string(got.Payload) != want {
			t.Errorf("frame %d: got payload %q, want %q", i, got.Payload, want)
		}
	}
	_, err := s.Recv()
	if err != io.EOF {
		t.Fatalf("Recv on trailing TypeClose: got %v, want io.EOF", err)
	}
}

// TestRecvTypeErrorFrameReturnsError 守护"收到 TypeError 帧 → streamError"。
// 同时验证 payload 里的 Response.Err 被正确提取。
func TestRecvTypeErrorFrameReturnsError(t *testing.T) {
	s, recv := newBareStream(t)
	errPayload := mustEncode(Response{Err: "boom: bad thing"})
	recv.ch <- &Frame{StreamID: 42, Type: TypeError, Payload: errPayload}
	_, err := s.Recv()
	if err == nil {
		t.Fatal("Recv on TypeError: got nil, want error")
	}
	if msg := err.Error(); !strings.Contains(msg, "boom: bad thing") {
		t.Errorf("error message %q does not contain server's err text", msg)
	}
}

// TestRecvTypeErrorMalformedPayload 守护 streamErr 的 fallback:
// payload 不是合法 JSON / 没有 Err 字段时,不要吞掉,把 raw 附上。
func TestRecvTypeErrorMalformedPayload(t *testing.T) {
	s, recv := newBareStream(t)
	raw := []byte("not-json-at-all")
	recv.ch <- &Frame{StreamID: 42, Type: TypeError, Payload: raw}
	_, err := s.Recv()
	if err == nil {
		t.Fatal("Recv on malformed TypeError: got nil, want error")
	}
	if msg := err.Error(); !strings.Contains(msg, "not-json-at-all") {
		t.Errorf("error message %q should preserve raw payload for debugging", msg)
	}
}

// TestRecvTypeErrorEmptyErr 同上但合法 JSON + 空 Err。
func TestRecvTypeErrorEmptyErr(t *testing.T) {
	s, recv := newBareStream(t)
	// Err 为空 → streamErr 走 fallback,带上 raw payload
	recv.ch <- &Frame{StreamID: 42, Type: TypeError, Payload: []byte(`{"err":""}`)}
	_, err := s.Recv()
	if err == nil {
		t.Fatal("Recv on TypeError with empty err: got nil, want error")
	}
}

// TestCloseIdempotent 守护 Close 幂等性(多次调用安全)。
// 用真实 Conn,因为 Close 内部要走 conn.Unregister。
func TestCloseIdempotent(t *testing.T) {
	s, _, cleanup := newLiveStream(t)
	defer cleanup()
	for i := 0; i < 3; i++ {
		if err := s.Close(); err != nil {
			t.Fatalf("Close #%d: %v", i, err)
		}
	}
}

// TestRecvStopsAfterPublicCloseWithLiveReadLoop 覆盖公共 Close 路径:
// 即使 readLoop 已经把帧投进队列,公共 Stream.Close 也表示本地放弃接收,
// 后续 Recv 应该直接 EOF。
func TestRecvStopsAfterPublicCloseWithLiveReadLoop(t *testing.T) {
	a, b := net.Pipe()
	conn := NewConn(a) // server 端,自带 readLoop
	defer func() {
		// 关 b 让 a 的 readLoop 退出,再等它退出完。
		_ = b.Close()
		<-conn.Done()
	}()

	recv, err := conn.Register(11)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	stream := newStream(11, conn, recv)

	// 从 b 端写 3 帧 DATA(StreamID=11)进 pipe。readLoop 会把它们投进 recv.ch。
	frames := []*Frame{
		{StreamID: 11, Type: TypeData, Payload: []byte("p1")},
		{StreamID: 11, Type: TypeData, Payload: []byte("p2")},
		{StreamID: 11, Type: TypeData, Payload: []byte("p3")},
	}
	written := make(chan struct{})
	go func() {
		for _, f := range frames {
			if err := WriteFrame(b, f); err != nil {
				return
			}
		}
		close(written)
	}()
	// 等写完 —— net.Pipe 是同步的,写完 = 对端 ReadFrame 已经读到了。
	// 再给 readLoop 一点时间把 3 帧从 pipe 投进 recv.ch(它们的容量足够装下)。
	<-written
	// busy-wait 直到 recv.ch 里有 3 帧(确定 readLoop 已经投完,而不是还在 pipe 里)。
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(recv.ch) == 3 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if len(recv.ch) != 3 {
		t.Fatalf("readLoop did not deliver 3 frames into recv.ch (got %d)", len(recv.ch))
	}

	// 此刻 3 帧都在 recv.ch 里。调**公共** Stream.Close() → Unregister 关 done。
	if err := stream.Close(); err != nil {
		t.Fatalf("Stream.Close: %v", err)
	}

	_, err = stream.Recv()
	if err != io.EOF {
		t.Fatalf("Recv after public Close with queued frames: got %v, want io.EOF", err)
	}
}
