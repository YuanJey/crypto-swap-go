package events

import (
	"context"
	"fmt"
	"sync"

	"github.com/crypto-swap-go/pkg/models"
	"github.com/crypto-swap-go/pkg/modules"
)

type Router struct {
	mu sync.Mutex

	nextID int

	orderListeners map[int]modules.TradingListener
	algoListeners  map[int]modules.AlgoOrderListener
	onceOrders     map[int]onceOrder
	onceAlgos      map[int]onceAlgoOrder
}

type onceOrder struct {
	clientOrderID string
	event         models.OrderEvent
	listener      modules.TradingListener
}

type onceAlgoOrder struct {
	clientOrderID string
	event         models.OrderEvent
	listener      modules.AlgoOrderListener
}

type orderListenerFunc func(*models.OrderUpdate)

func (f orderListenerFunc) OnOrderUpdate(update *models.OrderUpdate) {
	f(update)
}

type algoOrderListenerFunc func(*models.AlgoOrderUpdate)

func (f algoOrderListenerFunc) OnAlgoOrderUpdate(update *models.AlgoOrderUpdate) {
	f(update)
}

func NewRouter() *Router {
	return &Router{
		orderListeners: make(map[int]modules.TradingListener),
		algoListeners:  make(map[int]modules.AlgoOrderListener),
		onceOrders:     make(map[int]onceOrder),
		onceAlgos:      make(map[int]onceAlgoOrder),
	}
}

func (r *Router) AttachOrder(listener modules.TradingListener) func() {
	if listener == nil {
		return func() {}
	}
	r.mu.Lock()
	id := r.next()
	r.orderListeners[id] = listener
	r.mu.Unlock()

	return func() {
		r.mu.Lock()
		delete(r.orderListeners, id)
		r.mu.Unlock()
	}
}

func (r *Router) AttachAlgoOrder(listener modules.AlgoOrderListener) func() {
	if listener == nil {
		return func() {}
	}
	r.mu.Lock()
	id := r.next()
	r.algoListeners[id] = listener
	r.mu.Unlock()

	return func() {
		r.mu.Lock()
		delete(r.algoListeners, id)
		r.mu.Unlock()
	}
}

func (r *Router) OnceOrder(clientOrderID string, event models.OrderEvent, listener modules.TradingListener) func() {
	if listener == nil {
		return func() {}
	}
	r.mu.Lock()
	id := r.next()
	r.onceOrders[id] = onceOrder{
		clientOrderID: clientOrderID,
		event:         normalizeOrderEvent(event),
		listener:      listener,
	}
	r.mu.Unlock()

	return func() {
		r.mu.Lock()
		delete(r.onceOrders, id)
		r.mu.Unlock()
	}
}

func (r *Router) OnceAlgoOrder(clientOrderID string, event models.OrderEvent, listener modules.AlgoOrderListener) func() {
	if listener == nil {
		return func() {}
	}
	r.mu.Lock()
	id := r.next()
	r.onceAlgos[id] = onceAlgoOrder{
		clientOrderID: clientOrderID,
		event:         normalizeOrderEvent(event),
		listener:      listener,
	}
	r.mu.Unlock()

	return func() {
		r.mu.Lock()
		delete(r.onceAlgos, id)
		r.mu.Unlock()
	}
}

func (r *Router) WaitOrder(ctx context.Context, clientOrderID string, event models.OrderEvent) (*models.OrderUpdate, error) {
	ch := make(chan *models.OrderUpdate, 1)
	detach := r.OnceOrder(clientOrderID, event, orderListenerFunc(func(update *models.OrderUpdate) {
		ch <- update
	}))
	defer detach()

	select {
	case update := <-ch:
		return update, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("wait order %q %s: %w", clientOrderID, normalizeOrderEvent(event), ctx.Err())
	}
}

func (r *Router) WaitAlgoOrder(ctx context.Context, clientOrderID string, event models.OrderEvent) (*models.AlgoOrderUpdate, error) {
	ch := make(chan *models.AlgoOrderUpdate, 1)
	detach := r.OnceAlgoOrder(clientOrderID, event, algoOrderListenerFunc(func(update *models.AlgoOrderUpdate) {
		ch <- update
	}))
	defer detach()

	select {
	case update := <-ch:
		return update, nil
	case <-ctx.Done():
		return nil, fmt.Errorf("wait algo order %q %s: %w", clientOrderID, normalizeOrderEvent(event), ctx.Err())
	}
}

func (r *Router) DispatchOrder(update *models.OrderUpdate) {
	if update == nil {
		return
	}

	listeners, onceListeners := r.matchOrderListeners(update.ClientOrderID, update.Status)
	for _, listener := range listeners {
		invokeOrderListener(listener, update)
	}
	for _, listener := range onceListeners {
		invokeOrderListener(listener, update)
	}
}

func (r *Router) DispatchAlgoOrder(update *models.AlgoOrderUpdate) {
	if update == nil {
		return
	}

	listeners, onceListeners := r.matchAlgoListeners(update.ClientOrderID, update.Status)
	for _, listener := range listeners {
		invokeAlgoOrderListener(listener, update)
	}
	for _, listener := range onceListeners {
		invokeAlgoOrderListener(listener, update)
	}
}

func invokeOrderListener(listener modules.TradingListener, update *models.OrderUpdate) {
	go func() {
		defer func() {
			_ = recover()
		}()
		listener.OnOrderUpdate(update)
	}()
}

func invokeAlgoOrderListener(listener modules.AlgoOrderListener, update *models.AlgoOrderUpdate) {
	go func() {
		defer func() {
			_ = recover()
		}()
		listener.OnAlgoOrderUpdate(update)
	}()
}

func (r *Router) matchOrderListeners(clientOrderID string, status models.OrderStatus) ([]modules.TradingListener, []modules.TradingListener) {
	r.mu.Lock()
	defer r.mu.Unlock()

	listeners := make([]modules.TradingListener, 0, len(r.orderListeners))
	for _, listener := range r.orderListeners {
		listeners = append(listeners, listener)
	}

	onceListeners := make([]modules.TradingListener, 0)
	for id, once := range r.onceOrders {
		if once.clientOrderID == clientOrderID && eventMatchesStatus(once.event, status) {
			onceListeners = append(onceListeners, once.listener)
			delete(r.onceOrders, id)
		}
	}
	return listeners, onceListeners
}

func (r *Router) matchAlgoListeners(clientOrderID string, status models.OrderStatus) ([]modules.AlgoOrderListener, []modules.AlgoOrderListener) {
	r.mu.Lock()
	defer r.mu.Unlock()

	listeners := make([]modules.AlgoOrderListener, 0, len(r.algoListeners))
	for _, listener := range r.algoListeners {
		listeners = append(listeners, listener)
	}

	onceListeners := make([]modules.AlgoOrderListener, 0)
	for id, once := range r.onceAlgos {
		if once.clientOrderID == clientOrderID && eventMatchesStatus(once.event, status) {
			onceListeners = append(onceListeners, once.listener)
			delete(r.onceAlgos, id)
		}
	}
	return listeners, onceListeners
}

func (r *Router) next() int {
	r.nextID++
	return r.nextID
}

func normalizeOrderEvent(event models.OrderEvent) models.OrderEvent {
	if event == "" {
		return models.OrderEventAll
	}
	return event
}

func eventMatchesStatus(event models.OrderEvent, status models.OrderStatus) bool {
	switch normalizeOrderEvent(event) {
	case models.OrderEventAll:
		return true
	case models.OrderEventNew:
		return status == models.OrderStatusNew
	case models.OrderEventPartiallyFilled:
		return status == models.OrderStatusPartiallyFilled
	case models.OrderEventFilled:
		return status == models.OrderStatusFilled
	case models.OrderEventCanceled:
		return status == models.OrderStatusCanceled
	case models.OrderEventRejected:
		return status == models.OrderStatusRejected
	default:
		return models.OrderStatus(event) == status
	}
}
