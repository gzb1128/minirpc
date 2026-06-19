package minirpc

import (
	"encoding/json"
	"testing"
)

// codec_test.go 验证序列化层。这层负责把 Go 值 ↔ JSON 字节,
// 上层的 Request / Response 都依赖它。

func TestEncodeDecodeRoundTrip(t *testing.T) {
	type point struct {
		X int `json:"x"`
		Y int `json:"y"`
	}
	in := point{X: 3, Y: 4}
	b, err := Encode(in)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var out point
	if err := Decode(b, &out); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if out != in {
		t.Errorf("round trip: got %+v want %+v", out, in)
	}
}

// TestRequestArgsRawMessage 验证 Request.Args 用 RawMessage,
// 可以在不知道具体类型时原样搬运,handler 端再二次解码。
func TestRequestArgsRawMessage(t *testing.T) {
	// 模拟 client 打包参数:[2,3] → JSON 数组字节
	args := []any{2, 3}
	argsJSON, _ := json.Marshal(args)

	req := Request{Method: "MathService.Add", Args: argsJSON}
	b, err := Encode(req)
	if err != nil {
		t.Fatalf("Encode request: %v", err)
	}

	var got Request
	if err := Decode(b, &got); err != nil {
		t.Fatalf("Decode request: %v", err)
	}
	if got.Method != req.Method {
		t.Errorf("Method: got %q want %q", got.Method, req.Method)
	}
	// Args 应该原样保留为 JSON 数组
	var nums []int
	if err := json.Unmarshal(got.Args, &nums); err != nil {
		t.Fatalf("re-decode args: %v", err)
	}
	if len(nums) != 2 || nums[0] != 2 || nums[1] != 3 {
		t.Errorf("args: got %v want [2 3]", nums)
	}
}

// TestResponseErr 验证 Response.Err 空串表示无错(omitempty 序列化时省略)。
func TestResponseErr(t *testing.T) {
	resp := Response{Result: json.RawMessage("5")}
	b, err := Encode(resp)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	var got Response
	if err := Decode(b, &got); err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if got.Err != "" {
		t.Errorf("Err should be empty, got %q", got.Err)
	}
}
