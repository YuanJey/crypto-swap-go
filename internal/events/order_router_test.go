package events

import (
	"context"
	"testing"
	"time"

	"github.com/crypto-swap-go/pkg/models"
)

func TestRouterOnceOrderMatchesClientIDAndEvent(t *testing.T) {
	router := NewRouter()
	ch := make(chan *models.OrderUpdate, 1)

	router.OnceOrder("order-1", models.OrderEventFilled, orderListenerFunc(func(update *models.OrderUpdate) {
		ch <- update
	}))

	router.DispatchOrder(&models.OrderUpdate{ClientOrderID: "order-1", Status: models.OrderStatusNew})
	assertNoOrderUpdate(t, ch)

	router.DispatchOrder(&models.OrderUpdate{ClientOrderID: "other", Status: models.OrderStatusFilled})
	assertNoOrderUpdate(t, ch)

	router.DispatchOrder(&models.OrderUpdate{ClientOrderID: "order-1", Status: models.OrderStatusFilled})
	update := assertOrderUpdate(t, ch)
	if update.ClientOrderID != "order-1" {
		t.Fatalf("ClientOrderID = %q, want order-1", update.ClientOrderID)
	}

	router.DispatchOrder(&models.OrderUpdate{ClientOrderID: "order-1", Status: models.OrderStatusFilled})
	assertNoOrderUpdate(t, ch)
}

func TestRouterWaitOrder(t *testing.T) {
	router := NewRouter()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	result := make(chan *models.OrderUpdate, 1)
	errs := make(chan error, 1)
	go func() {
		update, err := router.WaitOrder(ctx, "order-2", models.OrderEventFilled)
		if err != nil {
			errs <- err
			return
		}
		result <- update
	}()

	waitRouterOnceOrderRegistered(t, router)
	router.DispatchOrder(&models.OrderUpdate{ClientOrderID: "order-2", Status: models.OrderStatusFilled})

	select {
	case err := <-errs:
		t.Fatalf("WaitOrder() error = %v", err)
	case update := <-result:
		if update.ClientOrderID != "order-2" {
			t.Fatalf("ClientOrderID = %q, want order-2", update.ClientOrderID)
		}
	case <-time.After(time.Second):
		t.Fatal("WaitOrder() timed out")
	}
}

func TestRouterOnceAlgoOrder(t *testing.T) {
	router := NewRouter()
	ch := make(chan *models.AlgoOrderUpdate, 1)

	router.OnceAlgoOrder("algo-1", models.OrderEventNew, algoOrderListenerFunc(func(update *models.AlgoOrderUpdate) {
		ch <- update
	}))

	router.DispatchAlgoOrder(&models.AlgoOrderUpdate{ClientOrderID: "algo-1", Status: models.OrderStatusNew})
	update := assertAlgoOrderUpdate(t, ch)
	if update.ClientOrderID != "algo-1" {
		t.Fatalf("ClientOrderID = %q, want algo-1", update.ClientOrderID)
	}
}

func TestRouterDetachOrderListener(t *testing.T) {
	router := NewRouter()
	ch := make(chan *models.OrderUpdate, 1)
	detach := router.AttachOrder(orderListenerFunc(func(update *models.OrderUpdate) {
		ch <- update
	}))
	detach()

	router.DispatchOrder(&models.OrderUpdate{ClientOrderID: "order-3", Status: models.OrderStatusFilled})
	assertNoOrderUpdate(t, ch)
}

func TestRouterDispatchDoesNotBlockOnSlowListener(t *testing.T) {
	router := NewRouter()
	block := make(chan struct{})
	router.AttachOrder(orderListenerFunc(func(update *models.OrderUpdate) {
		<-block
	}))
	defer close(block)

	done := make(chan struct{})
	go func() {
		router.DispatchOrder(&models.OrderUpdate{ClientOrderID: "order-4", Status: models.OrderStatusFilled})
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(50 * time.Millisecond):
		t.Fatal("DispatchOrder blocked on slow listener")
	}
}

func TestRouterDispatchRecoversListenerPanic(t *testing.T) {
	router := NewRouter()
	ch := make(chan *models.OrderUpdate, 1)
	router.AttachOrder(orderListenerFunc(func(update *models.OrderUpdate) {
		panic("boom")
	}))
	router.AttachOrder(orderListenerFunc(func(update *models.OrderUpdate) {
		ch <- update
	}))

	router.DispatchOrder(&models.OrderUpdate{ClientOrderID: "order-5", Status: models.OrderStatusFilled})
	update := assertOrderUpdate(t, ch)
	if update.ClientOrderID != "order-5" {
		t.Fatalf("ClientOrderID = %q, want order-5", update.ClientOrderID)
	}
}

func waitRouterOnceOrderRegistered(t *testing.T, router *Router) {
	t.Helper()
	deadline := time.After(time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-deadline:
			t.Fatal("timed out waiting for once order registration")
		case <-ticker.C:
			router.mu.Lock()
			registered := len(router.onceOrders) > 0
			router.mu.Unlock()
			if registered {
				return
			}
		}
	}
}

func assertOrderUpdate(t *testing.T, ch <-chan *models.OrderUpdate) *models.OrderUpdate {
	t.Helper()
	select {
	case update := <-ch:
		return update
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for order update")
	}
	return nil
}

func assertNoOrderUpdate(t *testing.T, ch <-chan *models.OrderUpdate) {
	t.Helper()
	select {
	case update := <-ch:
		t.Fatalf("unexpected order update: %+v", update)
	case <-time.After(20 * time.Millisecond):
	}
}

func assertAlgoOrderUpdate(t *testing.T, ch <-chan *models.AlgoOrderUpdate) *models.AlgoOrderUpdate {
	t.Helper()
	select {
	case update := <-ch:
		return update
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for algo order update")
	}
	return nil
}
