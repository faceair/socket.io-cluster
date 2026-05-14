package sio

import (
	"bytes"
	"testing"
	"unsafe"
)

func TestPooledBytesBucketAndFrameReuse(t *testing.T) {
	buf := acquireBytes(257)
	if got := cap(buf.B); got != 512 {
		t.Fatalf("cap=%d, want 512", got)
	}
	buf.Release()

	frame := newEngineMessageFrame([]byte(`2["warmup"]`))
	releaseEngineFrame(frame)
	allocs := testing.AllocsPerRun(1000, func() {
		f := newEngineMessageFrame([]byte(`2["event",1]`))
		releaseEngineFrame(f)
	})
	if allocs != 0 {
		t.Fatalf("newEngineMessageFrame allocations=%v, want 0", allocs)
	}
}

func TestByteBatchViewsSurviveGrowth(t *testing.T) {
	batch := acquireByteBatch(2, 1)
	defer batch.Release()
	first := batch.AppendString("a")
	second := batch.AppendBytes(bytes.Repeat([]byte{'b'}, 1024))
	views := batch.Views()
	if len(views) != 2 {
		t.Fatalf("views=%d, want 2", len(views))
	}
	if string(first) != "a" || string(views[0]) != "a" {
		t.Fatalf("first view corrupted: %q %q", first, views[0])
	}
	if len(second) != 1024 || len(views[1]) != 1024 || views[1][0] != 'b' {
		t.Fatalf("second view corrupted")
	}
	if !subsliceOf(views[0], batch.raw.B) || !subsliceOf(views[1], batch.raw.B) {
		t.Fatalf("views are not backed by batch raw buffer")
	}
}

func TestEncodedArgsUsesPooledContiguousViews(t *testing.T) {
	encoded, err := encodeAnyArgs([]any{[]byte(`{"x":1}`), Binary("abc")})
	if err != nil {
		t.Fatal(err)
	}
	defer encoded.Release()
	jsonViews := encoded.JSONViews()
	binaryViews := encoded.BinaryViews()
	if len(jsonViews) != 2 || len(binaryViews) != 1 {
		t.Fatalf("json=%d binary=%d", len(jsonViews), len(binaryViews))
	}
	for _, view := range jsonViews {
		if !subsliceOf(view, encoded.json.raw.B) {
			t.Fatalf("json view %q not backed by json raw", view)
		}
	}
	for _, view := range binaryViews {
		if !subsliceOf(view, encoded.binary.raw.B) {
			t.Fatalf("binary view %q not backed by binary raw", view)
		}
	}
}

func subsliceOf(view, raw []byte) bool {
	if len(view) == 0 {
		return true
	}
	if len(raw) == 0 {
		return false
	}
	vp := uintptr(unsafe.Pointer(unsafe.SliceData(view)))
	rp := uintptr(unsafe.Pointer(unsafe.SliceData(raw)))
	return vp >= rp && vp+uintptr(len(view)) <= rp+uintptr(len(raw))
}
