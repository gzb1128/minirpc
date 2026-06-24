package main

import (
	"bytes"
	"testing"
)

// TestChunkBlob 按**字节**切分(不是按字符),因为传输的对象本质是字节流 blob,
// SHA256 也按字节算 —— 切分按字节最贴近真实场景,且只要 client/server 用同样
// 的字节拼回就对得上。
func TestChunkBlob(t *testing.T) {
	tests := []struct {
		name string
		text string
		size int
		want [][]byte // 期望的每一块字节;nil 表示期望空结果
	}{
		{
			name: "整除",
			text: "abcdef",
			size: 2,
			want: [][]byte{{'a', 'b'}, {'c', 'd'}, {'e', 'f'}},
		},
		{
			name: "不整除,最后一块不足 size",
			text: "abcdefg",
			size: 3,
			want: [][]byte{{'a', 'b', 'c'}, {'d', 'e', 'f'}, {'g'}},
		},
		{
			name: "size 大于全文,只有一块(且就是全文)",
			text: "abc",
			size: 100,
			want: [][]byte{{'a', 'b', 'c'}},
		},
		{
			name: "空文本 → 零块",
			text: "",
			size: 4,
			want: [][]byte{},
		},
		{
			name: "多字节 UTF-8 按字节切,会切断字符(预期行为)",
			// "中文" 的 UTF-8 编码:E4 B8 AD E6 96 87(各 3 字节)。
			// size=4 会把 "中"(E4 B8 AD)完整放进第一块,再把 "文" 的第一字节 E6
			// 也挤进第一块 —— 于是 "文" 被切断(E6 | 96 87)。
			// 这正是字节流 blob 的真实样子:切分不关心字符边界,
			// 只要拼回的字节序列和原文一致,SHA256 就对。
			text: "中文",
			size: 4,
			want: [][]byte{
				{0xE4, 0xB8, 0xAD, 0xE6}, // "中"(3) + "文"的第一字节 E6
				{0x96, 0x87},              // "文"的剩余 2 字节
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := chunkBlob([]byte(tt.text), tt.size)
			if len(got) != len(tt.want) {
				t.Fatalf("chunkBlob 块数 = %d, want %d (got=%v)", len(got), len(tt.want), got)
			}
			for i := range got {
				if !bytes.Equal(got[i], tt.want[i]) {
					t.Errorf("块 %d = %x, want %x", i, got[i], tt.want[i])
				}
			}
			// 关键不变量:所有块按顺序拼回去 == 原文。这是 SHA256 能对上的前提。
			var reassembled []byte
			for _, c := range got {
				reassembled = append(reassembled, c...)
			}
			if !bytes.Equal(reassembled, []byte(tt.text)) {
				t.Errorf("拼回 != 原文: got %x, want %x", reassembled, []byte(tt.text))
			}
		})
	}
}

// TestChunkBlobPanicsOnBadSize 守护:size 必须 > 0,否则无法切分。
// (零长度的块会无限循环或产生零字节块,没意义 —— 用 panic 暴露调用方错误。)
func TestChunkBlobPanicsOnBadSize(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("chunkBlob(text, 0) 应当 panic,但没有")
		}
	}()
	_ = chunkBlob([]byte("abc"), 0)
}
