package sio

import (
	"io"
	"math/bits"
	"strconv"
	"sync"
)

const (
	minByteBucketBits = 8  // 256B
	maxByteBucketBits = 16 // 64KiB

	minByteBucketSize = 1 << minByteBucketBits
	maxByteBucketSize = 1 << maxByteBucketBits
	byteBucketCount   = maxByteBucketBits - minByteBucketBits + 1

	minViewBucketBits = 1 // 2 views
	maxViewBucketBits = 6 // 64 views
	viewBucketCount   = maxViewBucketBits - minViewBucketBits + 1
)

var (
	byteBuckets   [byteBucketCount]sync.Pool
	zeroBytePool  = sync.Pool{New: func() any { return &pooledBytes{} }}
	viewBuckets   [viewBucketCount]sync.Pool
	zeroViewsPool = sync.Pool{New: func() any { return &pooledByteViews{} }}
	batchPool     = sync.Pool{New: func() any { return &byteBatch{} }}
)

type pooledBytes struct {
	B    []byte
	base []byte
}

func acquireBytes(minCap int) *pooledBytes {
	if minCap <= 0 {
		owned := zeroBytePool.Get().(*pooledBytes)
		owned.B = nil
		owned.base = nil
		return owned
	}
	idx := byteBucketIndexForMinCap(minCap)
	if idx < 0 {
		base := make([]byte, 0, minCap)
		return &pooledBytes{B: base[:0], base: base[:0]}
	}
	if v := byteBuckets[idx].Get(); v != nil {
		owned := v.(*pooledBytes)
		owned.B = owned.base[:0]
		return owned
	}
	size := 1 << (minByteBucketBits + idx)
	base := make([]byte, 0, size)
	return &pooledBytes{B: base[:0], base: base[:0]}
}

func (b *pooledBytes) Ensure(extra int) {
	if b == nil || extra <= 0 || cap(b.B)-len(b.B) >= extra {
		return
	}
	need := len(b.B) + extra
	next := acquireBytes(need)
	next.B = append(next.B, b.B...)
	oldBase := b.base
	*b = *next
	next.B = nil
	next.base = nil
	releaseByteBase(oldBase)
}

func (b *pooledBytes) Resize(n int) []byte {
	if b == nil {
		return nil
	}
	if cap(b.base) < n {
		b.Ensure(n - len(b.B))
	}
	b.B = b.base[:n]
	return b.B
}

func (b *pooledBytes) AppendByte(v byte) []byte {
	b.Ensure(1)
	b.B = append(b.B, v)
	return b.B
}

func (b *pooledBytes) AppendBytes(v []byte) []byte {
	b.Ensure(len(v))
	b.B = append(b.B, v...)
	return b.B
}

func (b *pooledBytes) AppendString(v string) []byte {
	b.Ensure(len(v))
	b.B = append(b.B, v...)
	return b.B
}

func (b *pooledBytes) AppendUint(v uint64) []byte {
	b.Ensure(20)
	b.B = strconv.AppendUint(b.B, v, 10)
	return b.B
}

func (b *pooledBytes) AppendInt(v int) []byte {
	b.Ensure(20)
	b.B = strconv.AppendInt(b.B, int64(v), 10)
	return b.B
}

func (b *pooledBytes) AppendQuote(v string) []byte {
	b.Ensure(len(v)*6 + 2)
	b.B = strconv.AppendQuote(b.B, v)
	return b.B
}

func (b *pooledBytes) Release() {
	if b == nil || b.base == nil {
		if b != nil {
			b.B = nil
			zeroBytePool.Put(b)
		}
		return
	}
	c := cap(b.base)
	idx := byteBucketIndexForExactCap(c)
	if idx < 0 {
		b.B = nil
		b.base = nil
		zeroBytePool.Put(b)
		return
	}
	b.B = b.base[:0]
	byteBuckets[idx].Put(b)
}

func releaseByteBase(base []byte) {
	if base == nil {
		return
	}
	c := cap(base)
	idx := byteBucketIndexForExactCap(c)
	if idx < 0 {
		return
	}
	byteBuckets[idx].Put(&pooledBytes{B: base[:0], base: base[:0]})
}

func byteBucketIndexForMinCap(minCap int) int {
	if minCap <= minByteBucketSize {
		return 0
	}
	if minCap > maxByteBucketSize {
		return -1
	}
	return bits.Len(uint(minCap-1)) - minByteBucketBits
}

func byteBucketIndexForExactCap(capacity int) int {
	if capacity < minByteBucketSize || capacity > maxByteBucketSize || capacity&(capacity-1) != 0 {
		return -1
	}
	return bits.Len(uint(capacity)) - minByteBucketBits - 1
}

type pooledByteViews struct {
	V    [][]byte
	base [][]byte
}

func acquireByteViews(minCap int) *pooledByteViews {
	if minCap <= 0 {
		views := zeroViewsPool.Get().(*pooledByteViews)
		views.V = nil
		views.base = nil
		return views
	}
	idx := viewBucketIndexForMinCap(minCap)
	if idx < 0 {
		base := make([][]byte, 0, minCap)
		return &pooledByteViews{V: base[:0], base: base[:0]}
	}
	if v := viewBuckets[idx].Get(); v != nil {
		views := v.(*pooledByteViews)
		views.V = views.base[:0]
		return views
	}
	size := 1 << (minViewBucketBits + idx)
	base := make([][]byte, 0, size)
	return &pooledByteViews{V: base[:0], base: base[:0]}
}

func (v *pooledByteViews) Append(item []byte) {
	if v == nil {
		return
	}
	if len(v.V) == cap(v.V) {
		v.Grow(1)
	}
	v.V = append(v.V, item)
}

func (v *pooledByteViews) Grow(extra int) {
	if v == nil || extra <= 0 || cap(v.V)-len(v.V) >= extra {
		return
	}
	next := acquireByteViews(len(v.V) + extra)
	next.V = append(next.V, v.V...)
	oldBase := v.base
	oldViews := v.V
	*v = *next
	next.V = nil
	next.base = nil
	releaseViewBase(oldBase, oldViews)
}

func (v *pooledByteViews) Release() {
	if v == nil || v.base == nil {
		if v != nil {
			v.V = nil
			zeroViewsPool.Put(v)
		}
		return
	}
	for i := range v.V {
		v.V[i] = nil
	}
	c := cap(v.base)
	idx := viewBucketIndexForExactCap(c)
	if idx < 0 {
		v.V = nil
		v.base = nil
		zeroViewsPool.Put(v)
		return
	}
	v.V = v.base[:0]
	viewBuckets[idx].Put(v)
}

func releaseViewBase(base [][]byte, used [][]byte) {
	if base == nil {
		return
	}
	for i := range used {
		used[i] = nil
	}
	c := cap(base)
	idx := viewBucketIndexForExactCap(c)
	if idx < 0 {
		return
	}
	viewBuckets[idx].Put(&pooledByteViews{V: base[:0], base: base[:0]})
}

func viewBucketIndexForMinCap(minCap int) int {
	minSize := 1 << minViewBucketBits
	maxSize := 1 << maxViewBucketBits
	if minCap <= minSize {
		return 0
	}
	if minCap > maxSize {
		return -1
	}
	return bits.Len(uint(minCap-1)) - minViewBucketBits
}

func viewBucketIndexForExactCap(capacity int) int {
	minSize := 1 << minViewBucketBits
	maxSize := 1 << maxViewBucketBits
	if capacity < minSize || capacity > maxSize || capacity&(capacity-1) != 0 {
		return -1
	}
	return bits.Len(uint(capacity)) - minViewBucketBits - 1
}

type byteBatch struct {
	raw   *pooledBytes
	views *pooledByteViews
}

func acquireByteBatch(viewCap, byteCap int) *byteBatch {
	b := batchPool.Get().(*byteBatch)
	b.raw = acquireBytes(byteCap)
	b.views = acquireByteViews(viewCap)
	return b
}

func readAllPooled(r io.Reader, hint int64) (*pooledBytes, error) {
	minCap := 4096
	if hint > 0 && hint <= int64(maxInt()) {
		minCap = int(hint)
	}
	owned := acquireBytes(minCap)
	if hint > 0 {
		owned.Resize(int(hint))
		_, err := io.ReadFull(r, owned.B)
		if err != nil {
			owned.Release()
			return nil, err
		}
		return owned, nil
	}
	var scratch [32 * 1024]byte
	for {
		n, err := r.Read(scratch[:])
		if n > 0 {
			owned.AppendBytes(scratch[:n])
		}
		if err == io.EOF {
			return owned, nil
		}
		if err != nil {
			owned.Release()
			return nil, err
		}
	}
}

func maxInt() int {
	return int(^uint(0) >> 1)
}

func (b *byteBatch) AppendBytes(v []byte) []byte {
	if b == nil {
		return nil
	}
	b.ensure(len(v))
	start := len(b.raw.B)
	b.raw.AppendBytes(v)
	item := b.raw.B[start:len(b.raw.B)]
	b.views.Append(item)
	return item
}

func (b *byteBatch) AppendString(v string) []byte {
	if b == nil {
		return nil
	}
	b.ensure(len(v))
	start := len(b.raw.B)
	b.raw.AppendString(v)
	item := b.raw.B[start:len(b.raw.B)]
	b.views.Append(item)
	return item
}

func (b *byteBatch) AppendPlaceholder(num int) []byte {
	if b == nil {
		return nil
	}
	b.ensure(64)
	start := len(b.raw.B)
	b.raw.AppendString(`{"_placeholder":true,"num":`)
	b.raw.AppendInt(num)
	b.raw.AppendByte('}')
	item := b.raw.B[start:len(b.raw.B)]
	b.views.Append(item)
	return item
}

func (b *byteBatch) ensure(extra int) {
	if b == nil || b.raw == nil || extra <= 0 || cap(b.raw.B)-len(b.raw.B) >= extra {
		return
	}
	b.raw.Ensure(extra)
	cursor := 0
	for i, view := range b.views.V {
		size := len(view)
		b.views.V[i] = b.raw.B[cursor : cursor+size]
		cursor += size
	}
}

func (b *byteBatch) Views() [][]byte {
	if b == nil || b.views == nil {
		return nil
	}
	return b.views.V
}

func (b *byteBatch) Release() {
	if b == nil {
		return
	}
	if b.views != nil {
		b.views.Release()
		b.views = nil
	}
	if b.raw != nil {
		b.raw.Release()
		b.raw = nil
	}
	batchPool.Put(b)
}
