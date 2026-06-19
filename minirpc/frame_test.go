package minirpc

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

// frame_test.go 验证最底层的帧编解码。
// 这一层如果错了,上面所有多路复用都白搭,所以必须先把它测透。

func TestFrameRoundTrip(t *testing.T) {
	cases := []struct {
		name  string
		frame *Frame
	}{
		{"request", &Frame{StreamID: 1, Type: TypeRequest, Payload: []byte(`{"method":"M.A","args":[1,2]}`)}},
		{"response", &Frame{StreamID: 42, Type: TypeResponse, Payload: []byte(`{"result":5}`)}},
		{"data", &Frame{StreamID: 7, Type: TypeData, Payload: []byte("hello")}},
		{"close", &Frame{StreamID: 99, Type: TypeClose, Payload: nil}},
		{"empty-payload", &Frame{StreamID: 3, Type: TypeClose, Payload: []byte{}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			if err := WriteFrame(&buf, tc.frame); err != nil {
				t.Fatalf("WriteFrame: %v", err)
			}
			got, err := ReadFrame(&buf)
			if err != nil {
				t.Fatalf("ReadFrame: %v", err)
			}
			if got.StreamID != tc.frame.StreamID {
				t.Errorf("StreamID: got %d want %d", got.StreamID, tc.frame.StreamID)
			}
			if got.Type != tc.frame.Type {
				t.Errorf("Type: got %v want %v", got.Type, tc.frame.Type)
			}
			if !bytes.Equal(got.Payload, tc.frame.Payload) {
				t.Errorf("Payload: got %q want %q", got.Payload, tc.frame.Payload)
			}
		})
	}
}

// TestFrameMultiple 验证"多帧连续写在同一个流里能正确切分"。
// 这是切消息边界的核心:TCP 没有边界,读循环靠 Length 字段切。
func TestFrameMultiple(t *testing.T) {
	frames := []*Frame{
		{StreamID: 1, Type: TypeRequest, Payload: []byte("first")},
		{StreamID: 2, Type: TypeResponse, Payload: []byte("second")},
		{StreamID: 3, Type: TypeData, Payload: []byte("third")},
	}
	var buf bytes.Buffer
	for _, f := range frames {
		if err := WriteFrame(&buf, f); err != nil {
			t.Fatalf("WriteFrame: %v", err)
		}
	}
	for i, want := range frames {
		got, err := ReadFrame(&buf)
		if err != nil {
			t.Fatalf("frame %d: ReadFrame: %v", i, err)
		}
		if got.StreamID != want.StreamID || got.Type != want.Type || !bytes.Equal(got.Payload, want.Payload) {
			t.Errorf("frame %d: got {%d,%v,%q} want {%d,%v,%q}",
				i, got.StreamID, got.Type, got.Payload, want.StreamID, want.Type, want.Payload)
		}
	}
}

// TestFrameLengthPrefix 验证 Length 字段的语义:
// 它是"剩余部分"长度(不含自身 4 字节),这是切消息的关键。
func TestFrameLengthPrefix(t *testing.T) {
	var buf bytes.Buffer
	f := &Frame{StreamID: 5, Type: TypeData, Payload: []byte("payload!")}
	if err := WriteFrame(&buf, f); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	raw := buf.Bytes()
	if len(raw) < 4 {
		t.Fatalf("output too short: %d bytes", len(raw))
	}
	length := binary.BigEndian.Uint32(raw[:4])
	// length 应该 = StreamID(4) + Type(1) + len(Payload)
	wantLen := uint32(4 + 1 + len(f.Payload))
	if length != wantLen {
		t.Errorf("Length field: got %d want %d", length, wantLen)
	}
	// raw 总长 = 4(Length 字段自己)+ length
	if int(4+length) != len(raw) {
		t.Errorf("total bytes: got %d want %d", len(raw), 4+int(length))
	}
}

// TestReadFrameEOF 验证空流读到 io.EOF(对端关连接时这是常态)。
func TestReadFrameEOF(t *testing.T) {
	empty := bytes.NewReader(nil)
	_, err := ReadFrame(empty)
	if !errors.Is(err, io.EOF) {
		t.Errorf("ReadFrame on empty: got %v want io.EOF", err)
	}
}

// TestReadFrameRejectsTooSmall 验证 length 校验:一个 length < 5 的畸形帧
// 必须被 ReadFrame 拒绝(返回 error),而不是 panic。
//
// 这守护的是 frame.go 里那个"length 已校验 >= 5"的注释承诺 —— 如果有人
// 删掉了那个 if length < 5 的检查,下面的用例会立刻失败(或 panic)。
func TestReadFrameRejectsTooSmall(t *testing.T) {
	cases := []uint32{0, 1, 2, 3, 4}
	for _, length := range cases {
		// 构造 4 字节 Length 前缀(BE),后面随便跟点字节(够不够无所谓,
		// 因为校验在 ReadFrame 读 payload 之前发生)。
		buf := bytes.NewBuffer(nil)
		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], length)
		buf.Write(lenBuf[:])
		buf.Write(make([]byte, length)) // length 字节,匹配声明(避免卡在 ReadFull)

		f, err := ReadFrame(buf)
		if err == nil {
			t.Errorf("length=%d: ReadFrame should have errored, got frame %+v", length, f)
		}
	}
}

// TestReadFrameRejectsTooLarge 验证 length 上限:超过 maxFrameSize 的 length
// 必须被拒绝(避免 4 字节触发数 GB 分配的 DoS)。
func TestReadFrameRejectsTooLarge(t *testing.T) {
	buf := bytes.NewBuffer(nil)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], maxFrameSize+1)
	buf.Write(lenBuf[:])
	// 不必真写那么多 payload —— 校验在 make([]byte, length) 之前发生,
	// 所以 ReadFrame 会在分配前就 return error。

	_, err := ReadFrame(buf)
	if err == nil {
		t.Error("ReadFrame should reject length > maxFrameSize")
	}
}

// TestReadFrameAcceptsBoundary 验证边界 length == 5(刚好:StreamID 4 + Type 1,
// payload 为空)被正常接受,且 Payload 是空切片(不 panic)。
func TestReadFrameAcceptsBoundary(t *testing.T) {
	// 手工拼一帧:length=5, StreamID=7, Type=TypeData, 无 payload。
	buf := bytes.NewBuffer(nil)
	var b [9]byte // 4(length) + 4(streamid) + 1(type)
	binary.BigEndian.PutUint32(b[0:4], 5)
	binary.BigEndian.PutUint32(b[4:8], 7)
	b[8] = byte(TypeData)
	buf.Write(b[:])

	f, err := ReadFrame(buf)
	if err != nil {
		t.Fatalf("length=5: ReadFrame errored: %v (should be accepted)", err)
	}
	if f.StreamID != 7 || f.Type != TypeData {
		t.Errorf("got {%d, %v}, want {7, DATA}", f.StreamID, f.Type)
	}
	if len(f.Payload) != 0 {
		t.Errorf("Payload should be empty, got %d bytes", len(f.Payload))
	}
}

// TestPartialRead 验证 ReadFrame 能正确处理"字节分批到达"的情况。
// TCP 是字节流,一次 Read 可能只返回半个帧 —— ReadFrame 必须用 io.ReadFull 兜住。
func TestPartialRead(t *testing.T) {
	f := &Frame{StreamID: 100, Type: TypeResponse, Payload: []byte("split-me")}
	var buf bytes.Buffer
	if err := WriteFrame(&buf, f); err != nil {
		t.Fatalf("WriteFrame: %v", err)
	}

	// 一字节一字节地喂 —— 模拟最极端的字节流到达。
	r := &oneByteReader{src: buf.Bytes()}
	got, err := ReadFrame(r)
	if err != nil {
		t.Fatalf("ReadFrame with 1-byte-at-a-time reader: %v", err)
	}
	if got.StreamID != f.StreamID || got.Type != f.Type || !bytes.Equal(got.Payload, f.Payload) {
		t.Errorf("got {%d,%v,%q} want {%d,%v,%q}",
			got.StreamID, got.Type, got.Payload, f.StreamID, f.Type, f.Payload)
	}
}

type oneByteReader struct {
	src []byte
	pos int
}

func (r *oneByteReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.src) {
		return 0, io.EOF
	}
	p[0] = r.src[r.pos]
	r.pos++
	return 1, nil
}
