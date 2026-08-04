package pool

import "testing"

func TestRingBufferPutGet(t *testing.T) {
	rb := NewRingBuffer(2)

	if !rb.Put(EventTypeTicker, []byte("a")) {
		t.Fatal("first Put returned false")
	}
	if !rb.Put(EventTypeOrder, []byte("b")) {
		t.Fatal("second Put returned false")
	}
	if rb.Put(EventTypeAccount, []byte("c")) {
		t.Fatal("Put succeeded when buffer should be full")
	}

	event, ok := rb.Get()
	if !ok {
		t.Fatal("first Get returned false")
	}
	if event.Type != EventTypeTicker || string(event.Payload) != "a" {
		t.Fatalf("first event = (%v, %q), want ticker/a", event.Type, event.Payload)
	}

	event, ok = rb.Get()
	if !ok {
		t.Fatal("second Get returned false")
	}
	if event.Type != EventTypeOrder || string(event.Payload) != "b" {
		t.Fatalf("second event = (%v, %q), want order/b", event.Type, event.Payload)
	}

	if _, ok := rb.Get(); ok {
		t.Fatal("Get succeeded when buffer should be empty")
	}
}

func TestNewRingBufferPanicsForInvalidCapacity(t *testing.T) {
	for _, capacity := range []uint64{0, 3} {
		t.Run("capacity", func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatalf("NewRingBuffer(%d) did not panic", capacity)
				}
			}()
			NewRingBuffer(capacity)
		})
	}
}
