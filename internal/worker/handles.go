package worker

import "sync"

// handleTable maps the opaque integers stored inside Python objects onto
// the Go values they stand for. The table keeps the values alive: C never
// stores Go pointers.
//
// A handle packs a slot index (low 32 bits, off by one so that zero is never
// a valid handle) and the slot generation (high bits), which makes a stale
// handle resolve to nil instead of to whatever reused the slot.
type handleTable struct {
	mu    sync.Mutex
	slots []handleSlot
	free  []uint32
}

type handleSlot struct {
	v   any
	gen uint32
}

var handles handleTable

func (t *handleTable) put(v any) uintptr {
	t.mu.Lock()
	var idx uint32
	if n := len(t.free); n > 0 {
		idx = t.free[n-1]
		t.free = t.free[:n-1]
	} else {
		t.slots = append(t.slots, handleSlot{})
		idx = uint32(len(t.slots) - 1)
	}
	s := &t.slots[idx]
	s.v = v
	h := uint64(s.gen)<<32 | uint64(idx+1)
	t.mu.Unlock()
	return uintptr(h)
}

func (t *handleTable) get(h uintptr) any {
	idx := uint32(uint64(h)&0xffffffff) - 1
	gen := uint32(uint64(h) >> 32)
	t.mu.Lock()
	var v any
	if int(idx) < len(t.slots) && t.slots[idx].gen == gen {
		v = t.slots[idx].v
	}
	t.mu.Unlock()
	return v
}

func (t *handleTable) del(h uintptr) any {
	idx := uint32(uint64(h)&0xffffffff) - 1
	gen := uint32(uint64(h) >> 32)
	t.mu.Lock()
	var v any
	if int(idx) < len(t.slots) && t.slots[idx].gen == gen {
		s := &t.slots[idx]
		v = s.v
		s.v = nil
		s.gen++
		t.free = append(t.free, idx)
	}
	t.mu.Unlock()
	return v
}
