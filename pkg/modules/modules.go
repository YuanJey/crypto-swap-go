package modules

import (
	"context"

	"github.com/YuanJey/crypto-swap-go/pkg/models"
)

// MarketListener processes high frequency market data
type MarketListener interface {
	OnTicker(ticker *models.Ticker)
}

// CandleListener processes OHLCV updates.
type CandleListener interface {
	OnCandle(candle *models.Candle)
}

// TradingListener processes execution reports
type TradingListener interface {
	OnOrderUpdate(update *models.OrderUpdate)
}

// AlgoOrderListener processes conditional/algo order reports such as TP/SL.
type AlgoOrderListener interface {
	OnAlgoOrderUpdate(update *models.AlgoOrderUpdate)
}

// AccountListener processes portfolio updates
type AccountListener interface {
	OnPositionUpdate(position *models.Position)
	OnBalanceUpdate(balance *models.AccountBalance)
}

// PositionListener processes position updates.
type PositionListener interface {
	OnPositionUpdate(position *models.Position)
}

// BalanceListener processes balance updates.
type BalanceListener interface {
	OnBalanceUpdate(balance *models.AccountBalance)
}

// MarketModule manages market data streams
type MarketModule interface {
	AttachMarketListener(listener MarketListener)
	AttachCandleListener(listener CandleListener)
	SubscribeTickers(symbols []string) error
	SubscribeCandles(symbols []string, timeframe string) error
	GetOHLCV(ctx context.Context, symbol, timeframe string, limit int) ([]models.Candle, error)
	GetInstrument(ctx context.Context, symbol string) (*models.Instrument, error)
}

// TradingModule handles order placement and execution reports
type TradingModule interface {
	AttachTradingListener(listener TradingListener) func()
	AttachAlgoOrderListener(listener AlgoOrderListener) func()
	AttachAccountListener(listener AccountListener)
	AttachPositionListener(listener PositionListener) func()
	AttachBalanceListener(listener BalanceListener) func()
	OnceOrder(clientOrderID string, event models.OrderEvent, listener TradingListener) func()
	OnceAlgoOrder(clientOrderID string, event models.OrderEvent, listener AlgoOrderListener) func()
	WaitOrder(ctx context.Context, clientOrderID string, event models.OrderEvent) (*models.OrderUpdate, error)
	WaitAlgoOrder(ctx context.Context, clientOrderID string, event models.OrderEvent) (*models.AlgoOrderUpdate, error)
	ConfigureTradingAccount(ctx context.Context, cfg models.TradingAccountConfig) error
	SetPositionMode(ctx context.Context, mode models.PositionMode) error
	GetPositionMode(ctx context.Context) (models.PositionMode, error)
	SetMarginMode(ctx context.Context, symbol string, mode models.MarginMode) error
	SetLeverage(ctx context.Context, symbol string, leverage int, positionSide models.PositionSide, marginMode models.MarginMode) error
	PlaceOrder(ctx context.Context, req models.PlaceOrderReq) (string, error)
	PlaceMarketOrder(ctx context.Context, req models.PlaceOrderReq) (string, error)
	GetOpenOrders(ctx context.Context, symbol string) ([]models.OpenOrder, error)
	AmendOrder(ctx context.Context, req models.AmendOrderReq) (string, error)
	CancelOrder(ctx context.Context, symbol, clientOrderID string) error
	PlaceTPSL(ctx context.Context, req models.TPSLReq) (models.TPSLOrder, error)
	PlaceTrailingOrder(ctx context.Context, req models.TrailingOrderReq) (models.TrailingOrder, error)
	GetOpenAlgoOrders(ctx context.Context, symbol string) ([]models.OpenAlgoOrder, error)
	CancelTPSL(ctx context.Context, symbol string, order models.TPSLOrder) error
	UpdateTPSL(ctx context.Context, old models.TPSLOrder, req models.TPSLReq) (models.TPSLOrder, error)
	GetPosition(ctx context.Context, symbol string, side models.PositionSide) (*models.Position, error)
	ClosePosition(ctx context.Context, symbol string, side models.PositionSide) (string, error)
}

// ExchangeClient is the top level adapter for an exchange
type ExchangeClient interface {
	MarketModule() MarketModule
	TradingModule() TradingModule
	Start(ctx context.Context) error
	Stop() error
}
