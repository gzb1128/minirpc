package main

import "testing"

func TestParseSlowOpDurationRejectsBusinessID(t *testing.T) {
	got, err := parseSlowOpDuration([]byte(`[500]`))
	if err != nil {
		t.Fatalf("parseSlowOpDuration valid args: %v", err)
	}
	if got != 500 {
		t.Fatalf("parseSlowOpDuration valid args = %d, want 500", got)
	}

	if _, err := parseSlowOpDuration([]byte(`[1,500]`)); err == nil {
		t.Fatal("parseSlowOpDuration accepted [businessID,duration], want error")
	}
}

func TestCompleteMarkerUsesDisplayLabelOnly(t *testing.T) {
	calls := []slowCall{
		{label: "task A", ms: 1000},
		{label: "task B", ms: 2000},
		{label: "task C", ms: 500},
	}

	tests := []struct {
		label string
		want  string
	}{
		{label: "task C", want: "★ 最先完成!不是发起顺序 —— 多路复用的证据"},
		{label: "task A", want: "第 2 个完成(按耗时,不按发起顺序)"},
		{label: "task B", want: "第 3 个完成(按耗时,不按发起顺序)"},
	}

	for _, tt := range tests {
		t.Run(tt.label, func(t *testing.T) {
			if got := completeMarker(tt.label, calls); got != tt.want {
				t.Fatalf("completeMarker(%q) = %q, want %q", tt.label, got, tt.want)
			}
		})
	}
}
