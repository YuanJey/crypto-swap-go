package models

import (
	"github.com/shopspring/decimal"
)

// OrderStatus defines unified order status across exchanges
type OrderStatus string

const (
	OrderStatusNew             OrderStatus = "NEW"
	OrderStatusPartiallyFilled OrderStatus = "PARTIALLY_FILLED"
	OrderStatusFilled          OrderStatus = "FILLED"
	OrderStatusCanceled        OrderStatus = "CANCELED"
	OrderStatusRejected        OrderStatus = "REJECTED"
)

// OrderEvent defines the event a listener wants to observe.
type OrderEvent string

const (
	OrderEventAll             OrderEvent = "ALL"
	OrderEventNew             OrderEvent = "NEW"
	OrderEventPartiallyFilled OrderEvent = "PARTIALLY_FILLED"
	OrderEventFilled          OrderEvent = "FILLED"
	OrderEventCanceled        OrderEvent = "CANCELED"
	OrderEventRejected        OrderEvent = "REJECTED"
)

type OrderSide string

const (
	OrderSideBuy  OrderSide = "BUY"
	OrderSideSell OrderSide = "SELL"
)

type PositionMode string

const (
	PositionModeOneWay PositionMode = "ONE_WAY"
	PositionModeHedge  PositionMode = "HEDGE"
)

type PositionSide string

const (
	PositionSideLong  PositionSide = "LONG"
	PositionSideShort PositionSide = "SHORT"
	PositionSideBoth  PositionSide = "BOTH"
)

type MarginMode string

const (
	MarginModeCross    MarginMode = "CROSS"
	MarginModeIsolated MarginMode = "ISOLATED"
)

type TradingAccountConfig struct {
	PositionMode PositionMode
	MarginMode   MarginMode
	Leverage     int
	Symbols      []string
}

type TPSLReq struct {
	Symbol        string
	ClientOrderID string
	Side          OrderSide
	PositionSide  PositionSide
	MarginMode    MarginMode
	Quantity      decimal.Decimal
	BaseQuantity  decimal.Decimal // Unified base-asset quantity. If set, SDK converts it to exchange order quantity.
	TakeProfit    decimal.Decimal
	StopLoss      decimal.Decimal
}

type TPSLOrder struct {
	TakeProfitClientOrderID string
	TakeProfitOrderID       string
	StopLossClientOrderID   string
	StopLossOrderID         string
}

type TrailingOrderReq struct {
	Symbol          string
	ClientOrderID   string
	Side            OrderSide
	PositionSide    PositionSide
	MarginMode      MarginMode
	ReduceOnly      bool
	Quantity        decimal.Decimal
	BaseQuantity    decimal.Decimal // Unified base-asset quantity. If set, SDK converts it to exchange order quantity.
	ActivationPrice decimal.Decimal
	CallbackSpread  decimal.Decimal // Unified price-distance callback. Preferred.
	CallbackRatio   decimal.Decimal // Optional fallback; 0.05 means 5%.
}

type TrailingOrder struct {
	ClientOrderID string
	OrderID       string
}

type OpenOrder struct {
	Exchange      string
	Symbol        string
	ClientOrderID string
	ExchangeID    string
	Side          OrderSide
	PositionSide  PositionSide
	Status        OrderStatus
	Price         decimal.Decimal
	Quantity      decimal.Decimal
	FilledQty     decimal.Decimal
	AvgPrice      decimal.Decimal
	UpdateTime    int64
}

type OpenAlgoOrder struct {
	Exchange      string
	Symbol        string
	ClientOrderID string
	ExchangeID    string
	Side          OrderSide
	PositionSide  PositionSide
	Status        OrderStatus
	TriggerPrice  decimal.Decimal
	OrderPrice    decimal.Decimal
	Quantity      decimal.Decimal
	UpdateTime    int64
}

type AmendOrderReq struct {
	Symbol           string
	ClientOrderID    string
	ExchangeID       string
	NewClientOrderID string
	Side             OrderSide
	Price            decimal.Decimal
	Quantity         decimal.Decimal
	BaseQuantity     decimal.Decimal // Unified base-asset quantity. If set, SDK converts it to exchange order quantity.
}

// OrderUpdate represents an incoming order update from the exchange WebSocket
type OrderUpdate struct {
	Exchange      string // "binance" or "okx"
	Symbol        string // Normalized symbol, e.g., "BTC-USDT"
	ClientOrderID string // Strategy provided ID
	ExchangeID    string // Exchange internal ID
	Side          OrderSide
	Status        OrderStatus
	Price         decimal.Decimal
	Quantity      decimal.Decimal
	FilledQty     decimal.Decimal
	AvgPrice      decimal.Decimal
	UpdateTime    int64 // Milliseconds
}

// AlgoOrderUpdate represents a conditional/algo order update such as TP/SL.
type AlgoOrderUpdate struct {
	Exchange      string
	Symbol        string
	ClientOrderID string
	ExchangeID    string
	Side          OrderSide
	PositionSide  PositionSide
	Status        OrderStatus
	TriggerPrice  decimal.Decimal
	OrderPrice    decimal.Decimal
	Quantity      decimal.Decimal
	UpdateTime    int64
}

// PlaceOrderReq is a unified request structure for submitting an order
type PlaceOrderReq struct {
	Symbol        string
	ClientOrderID string
	Side          OrderSide
	PositionSide  PositionSide
	MarginMode    MarginMode
	ReduceOnly    bool
	Price         decimal.Decimal
	Quantity      decimal.Decimal
	BaseQuantity  decimal.Decimal // Unified base-asset quantity. If set, SDK converts it to exchange order quantity.
	TimeInForce   string          // "GTC", "IOC", "FOK"
}
