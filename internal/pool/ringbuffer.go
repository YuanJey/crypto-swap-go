package pool

import (
	"runtime"
	"sync/atomic"
)

// EventType distinguishes the raw payload
type EventType int

const (
	EventTypeTicker EventType = iota
	EventTypeOrder
	EventTypeAccount
)

// RawEvent is pre-allocated and avoids GC allocations during WebSocket read
type RawEvent struct {
	Type    EventType
	Payload []byte // Sliced from a larger buffer
}

// RingBuffer is a lock-free Single-Producer Multi-Consumer (SPMC) queue
// optimized for HFT payload dispatching.
type RingBuffer struct {
	buffer   []RawEvent
	capacity uint64
	mask     uint64
	head     uint64
	tail     uint64
}

// NewRingBuffer creates a new lock-free ring buffer. capacity must be a power of 2.
func NewRingBuffer(capacity uint64) *RingBuffer {
	if capacity == 0 || capacity&(capacity-1) != 0 {
		panic("ringbuffer capacity must be a non-zero power of 2")
	}

	return &RingBuffer{
		buffer:   make([]RawEvent, capacity),
		capacity: capacity,
		mask:     capacity - 1,
		head:     0,
		tail:     0,
	}
}

// Put writes an event into the buffer (Producer)
func (rb *RingBuffer) Put(eventType EventType, payload []byte) bool {
	tail := atomic.LoadUint64(&rb.tail)
	head := atomic.LoadUint64(&rb.head)

	if tail-head >= rb.capacity {
		return false // Buffer full
	}

	idx := tail & rb.mask
	rb.buffer[idx] = RawEvent{
		Type:    eventType,
		Payload: payload,
	}

	atomic.AddUint64(&rb.tail, 1)
	return true
}

// Get reads an event from the buffer (Consumer)
func (rb *RingBuffer) Get() (RawEvent, bool) {
	for {
		head := atomic.LoadUint64(&rb.head)
		tail := atomic.LoadUint64(&rb.tail)

		if head == tail {
			return RawEvent{}, false // Buffer empty
		}

		idx := head & rb.mask
		event := rb.buffer[idx]

		if atomic.CompareAndSwapUint64(&rb.head, head, head+1) {
			return event, true
		}

		runtime.Gosched()
	}
}
