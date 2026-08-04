package binance

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/crypto-swap-go/internal/integration"
	"github.com/crypto-swap-go/pkg/models"
	"github.com/shopspring/decimal"
)

func TestIntegrationBinanceListenKey(t *testing.T) {
	integration.RequireIntegration(t)

	module := NewTradingModule(
		integration.RequireEnv(t, "BINANCE_API_KEY"),
		integration.RequireEnv(t, "BINANCE_SECRET_KEY"),
		true,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	listenKey, err := module.getListenKey(ctx)
	if err != nil {
		t.Fatalf("getListenKey() error = %v", err)
	}
	if listenKey == "" {
		t.Fatal("getListenKey() returned an empty listenKey")
	}
}

func TestIntegrationBinanceMarketTicker(t *testing.T) {
	integration.RequireIntegration(t)

	ticker := waitBinanceTicker(t)
	if ticker.Symbol != "BTCUSDT" {
		t.Fatalf("ticker.Symbol = %q, want BTCUSDT", ticker.Symbol)
	}
	if ticker.BidPrice.IsZero() || ticker.AskPrice.IsZero() {
		t.Fatalf("ticker has zero prices: bid=%s ask=%s", ticker.BidPrice, ticker.AskPrice)
	}
}

func TestIntegrationBinanceGetInstrument(t *testing.T) {
	integration.RequireIntegration(t)

	module := NewMarketModule(true)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	instrument, err := module.GetInstrument(ctx, "BTCUSDT")
	if err != nil {
		t.Fatalf("GetInstrument() error = %v", err)
	}
	if instrument.Symbol != "BTCUSDT" {
		t.Fatalf("Symbol = %q, want BTCUSDT", instrument.Symbol)
	}
	if instrument.LotSize.IsZero() || instrument.CtVal.IsZero() {
		t.Fatalf("instrument has zero lot size or contract value: %+v", instrument)
	}
	t.Logf("Binance instrument: symbol=%s lotSize=%s baseLotSize=%s ctVal=%s stepSize=%s baseStepSize=%s tickSize=%s", instrument.Symbol, instrument.LotSize, instrument.BaseLotSize, instrument.CtVal, instrument.StepSize, instrument.BaseStepSize, instrument.TickSize)
}

func TestIntegrationBinanceGetOHLCV(t *testing.T) {
	integration.RequireIntegration(t)

	module := NewMarketModule(true)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	candles, err := module.GetOHLCV(ctx, "BTCUSDT", "1m", 2)
	if err != nil {
		t.Fatalf("GetOHLCV() error = %v", err)
	}
	if len(candles) == 0 {
		t.Fatal("GetOHLCV() returned no candles")
	}
	if candles[0].Open.IsZero() || candles[0].Close.IsZero() {
		t.Fatalf("GetOHLCV() returned zero price candle: %+v", candles[0])
	}
	t.Logf("Binance candle: symbol=%s timeframe=%s ts=%d open=%s close=%s volume=%s", candles[0].Symbol, candles[0].Timeframe, candles[0].Timestamp, candles[0].Open, candles[0].Close, candles[0].Volume)
}

func TestIntegrationBinancePlaceAndCancelOrder(t *testing.T) {
	integration.RequireIntegration(t)

	ticker := waitBinanceTicker(t)
	price := ticker.BidPrice.Mul(decimal.NewFromFloat(0.99)).Round(1)
	clientOrderID := fmt.Sprintf("csgo%d", time.Now().UnixNano())

	module := NewTradingModule(
		integration.RequireEnv(t, "BINANCE_API_KEY"),
		integration.RequireEnv(t, "BINANCE_SECRET_KEY"),
		true,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cleanupBinanceOpenOrders(t, ctx, module, "BTCUSDT")

	if err := module.ConfigureTradingAccount(ctx, models.TradingAccountConfig{
		PositionMode: models.PositionModeHedge,
		MarginMode:   models.MarginModeCross,
		Leverage:     3,
		Symbols:      []string{"BTCUSDT"},
	}); err != nil {
		t.Fatalf("ConfigureTradingAccount() error = %v", err)
	}
	mode, err := module.GetPositionMode(ctx)
	if err != nil {
		t.Fatalf("GetPositionMode() error = %v", err)
	}
	if mode != models.PositionModeHedge {
		t.Fatalf("GetPositionMode() = %q, want %q", mode, models.PositionModeHedge)
	}

	orderID, err := module.PlaceOrder(ctx, models.PlaceOrderReq{
		Symbol:        "BTCUSDT",
		ClientOrderID: clientOrderID,
		Side:          models.OrderSideBuy,
		PositionSide:  models.PositionSideLong,
		MarginMode:    models.MarginModeCross,
		Price:         price,
		Quantity:      decimal.RequireFromString("0.001"),
		TimeInForce:   "GTC",
	})
	if err != nil {
		t.Fatalf("PlaceOrder() error = %v", err)
	}
	if orderID == "" {
		t.Fatal("PlaceOrder() returned an empty order ID")
	}
	waitOpenOrder(t, ctx, module, "BTCUSDT", clientOrderID)

	amendedID, err := module.AmendOrder(ctx, models.AmendOrderReq{
		Symbol:        "BTCUSDT",
		ClientOrderID: clientOrderID,
		Side:          models.OrderSideBuy,
		Price:         ticker.BidPrice.Mul(decimal.NewFromFloat(0.985)).Round(1),
		Quantity:      decimal.RequireFromString("0.001"),
	})
	if err != nil {
		t.Fatalf("AmendOrder() error = %v", err)
	}
	if amendedID == "" {
		t.Fatal("AmendOrder() returned an empty order ID")
	}
	waitOpenOrder(t, ctx, module, "BTCUSDT", clientOrderID)

	if err := module.CancelOrder(ctx, "BTCUSDT", clientOrderID); err != nil {
		t.Fatalf("CancelOrder() error = %v", err)
	}
}

func TestIntegrationBinancePlaceTrailingOrder(t *testing.T) {
	integration.RequireIntegration(t)

	ticker := waitBinanceTicker(t)
	module := NewTradingModule(
		integration.RequireEnv(t, "BINANCE_API_KEY"),
		integration.RequireEnv(t, "BINANCE_SECRET_KEY"),
		true,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cleanupBinanceOpenOrders(t, ctx, module, "BTCUSDT")
	if err := module.ConfigureTradingAccount(ctx, models.TradingAccountConfig{
		PositionMode: models.PositionModeHedge,
		MarginMode:   models.MarginModeCross,
		Leverage:     3,
		Symbols:      []string{"BTCUSDT"},
	}); err != nil {
		t.Fatalf("ConfigureTradingAccount() error = %v", err)
	}

	clientOrderID := fmt.Sprintf("cstrail%d", time.Now().UnixNano())
	trailingOnce := make(chan *models.AlgoOrderUpdate, 1)
	module.OnceAlgoOrder(clientOrderID, models.OrderEventNew, algoOrderListenerFunc(func(update *models.AlgoOrderUpdate) {
		trailingOnce <- update
	}))
	activationPrice := ticker.AskPrice.Mul(decimal.NewFromFloat(1.02)).Round(1)
	order, err := module.PlaceTrailingOrder(ctx, models.TrailingOrderReq{
		Symbol:          "BTCUSDT",
		ClientOrderID:   clientOrderID,
		Side:            models.OrderSideBuy,
		PositionSide:    models.PositionSideLong,
		MarginMode:      models.MarginModeCross,
		Quantity:        decimal.RequireFromString("0.001"),
		ActivationPrice: activationPrice,
		CallbackSpread:  activationPrice.Mul(decimal.RequireFromString("0.05")).Round(1),
	})
	if err != nil {
		t.Fatalf("PlaceTrailingOrder() error = %v", err)
	}
	if order.OrderID == "" {
		t.Fatal("PlaceTrailingOrder() returned empty order ID")
	}
	waitAlgoOrderCallback(t, trailingOnce, func(update *models.AlgoOrderUpdate) bool {
		return update.ClientOrderID == clientOrderID && update.Status == models.OrderStatusNew
	}, "Binance injected trailing order create")
	waitOpenAlgoOrder(t, ctx, module, "BTCUSDT", clientOrderID)
	if err := module.cancelAlgoOrderID(ctx, order.OrderID); err != nil {
		t.Fatalf("Cancel trailing order error = %v", err)
	}
}

func TestIntegrationBinanceFullTradingLifecycle(t *testing.T) {
	integration.RequireIntegration(t)

	module := NewTradingModule(
		integration.RequireEnv(t, "BINANCE_API_KEY"),
		integration.RequireEnv(t, "BINANCE_SECRET_KEY"),
		true,
	)
	callbacks := make(chan *models.OrderUpdate, 16)
	positionUpdates := make(chan *models.Position, 16)
	balanceUpdates := make(chan *models.AccountBalance, 16)
	module.AttachTradingListener(tradingListenerFunc(func(update *models.OrderUpdate) {
		callbacks <- update
	}))
	module.AttachPositionListener(positionListenerFunc(func(position *models.Position) {
		positionUpdates <- position
	}))
	module.AttachBalanceListener(balanceListenerFunc(func(balance *models.AccountBalance) {
		balanceUpdates <- balance
	}))

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if err := module.Start(ctx); err != nil {
		t.Fatalf("Start() error = %v", err)
	}
	defer module.Stop()

	cleanupBinanceOpenOrders(t, ctx, module, "BTCUSDT")

	if err := module.ConfigureTradingAccount(ctx, models.TradingAccountConfig{
		PositionMode: models.PositionModeHedge,
		MarginMode:   models.MarginModeCross,
		Leverage:     3,
		Symbols:      []string{"BTCUSDT"},
	}); err != nil {
		t.Fatalf("ConfigureTradingAccount() error = %v", err)
	}
	waitIntegrationStep(t, "Binance account configuration")

	openClientID := fmt.Sprintf("csopen%d", time.Now().UnixNano())
	openOnce := make(chan *models.OrderUpdate, 1)
	module.OnceOrder(openClientID, models.OrderEventFilled, tradingListenerFunc(func(update *models.OrderUpdate) {
		openOnce <- update
	}))
	openOrderID, err := module.PlaceMarketOrder(ctx, models.PlaceOrderReq{
		Symbol:        "BTCUSDT",
		ClientOrderID: openClientID,
		Side:          models.OrderSideBuy,
		PositionSide:  models.PositionSideLong,
		MarginMode:    models.MarginModeCross,
		Quantity:      decimal.RequireFromString("0.001"),
	})
	if err != nil {
		t.Fatalf("PlaceMarketOrder(open) error = %v", err)
	}
	if openOrderID == "" {
		t.Fatal("PlaceMarketOrder(open) returned empty order ID")
	}
	waitOrderCallback(t, callbacks, func(update *models.OrderUpdate) bool {
		return update.ClientOrderID == openClientID && update.Status == models.OrderStatusFilled
	}, "Binance open market fill")
	waitOrderCallback(t, openOnce, func(update *models.OrderUpdate) bool {
		return update.ClientOrderID == openClientID && update.Status == models.OrderStatusFilled
	}, "Binance injected open market fill")
	waitPositionCallback(t, positionUpdates, func(position *models.Position) bool {
		return position.Exchange == "binance" && position.Symbol == "BTCUSDT" && position.PositionSide == "LONG" && position.PositionAmt.Abs().GreaterThanOrEqual(decimal.RequireFromString("0.001"))
	}, "Binance open position update")
	waitBalanceCallback(t, balanceUpdates, func(balance *models.AccountBalance) bool {
		return balance.Exchange == "binance" && balance.Asset == "USDT"
	}, "Binance balance update")
	waitIntegrationStep(t, "Binance open callbacks and account updates")

	position := waitPosition(t, ctx, module, "BTCUSDT", models.PositionSideLong, decimal.RequireFromString("0.001"))
	tpslReq := binanceTPSLFromPosition(position, "cstpsl")
	tpAlgoOnce := make(chan *models.AlgoOrderUpdate, 1)
	slAlgoOnce := make(chan *models.AlgoOrderUpdate, 1)
	module.OnceAlgoOrder(tpslReq.ClientOrderID+"tp", models.OrderEventNew, algoOrderListenerFunc(func(update *models.AlgoOrderUpdate) {
		tpAlgoOnce <- update
	}))
	module.OnceAlgoOrder(tpslReq.ClientOrderID+"sl", models.OrderEventNew, algoOrderListenerFunc(func(update *models.AlgoOrderUpdate) {
		slAlgoOnce <- update
	}))
	tpsl, err := module.PlaceTPSL(ctx, tpslReq)
	if err != nil {
		t.Fatalf("PlaceTPSL() error = %v", err)
	}
	if tpsl.TakeProfitOrderID == "" || tpsl.StopLossOrderID == "" {
		t.Fatalf("PlaceTPSL() returned incomplete order IDs: %+v", tpsl)
	}
	waitAlgoOrderCallback(t, tpAlgoOnce, func(update *models.AlgoOrderUpdate) bool {
		return update.ClientOrderID == tpsl.TakeProfitClientOrderID && update.Status == models.OrderStatusNew
	}, "Binance injected take-profit create")
	waitAlgoOrderCallback(t, slAlgoOnce, func(update *models.AlgoOrderUpdate) bool {
		return update.ClientOrderID == tpsl.StopLossClientOrderID && update.Status == models.OrderStatusNew
	}, "Binance injected stop-loss create")
	waitOpenAlgoOrder(t, ctx, module, "BTCUSDT", tpsl.TakeProfitClientOrderID)
	waitOpenAlgoOrder(t, ctx, module, "BTCUSDT", tpsl.StopLossClientOrderID)
	waitIntegrationStep(t, "Binance initial take-profit and stop-loss callbacks")

	addClientID := fmt.Sprintf("csadd%d", time.Now().UnixNano())
	addOnce := make(chan *models.OrderUpdate, 1)
	module.OnceOrder(addClientID, models.OrderEventFilled, tradingListenerFunc(func(update *models.OrderUpdate) {
		addOnce <- update
	}))
	addOrderID, err := module.PlaceMarketOrder(ctx, models.PlaceOrderReq{
		Symbol:        "BTCUSDT",
		ClientOrderID: addClientID,
		Side:          models.OrderSideBuy,
		PositionSide:  models.PositionSideLong,
		MarginMode:    models.MarginModeCross,
		Quantity:      decimal.RequireFromString("0.001"),
	})
	if err != nil {
		t.Fatalf("PlaceMarketOrder(add) error = %v", err)
	}
	if addOrderID == "" {
		t.Fatal("PlaceMarketOrder(add) returned empty order ID")
	}
	waitOrderCallback(t, callbacks, func(update *models.OrderUpdate) bool {
		return update.ClientOrderID == addClientID && update.Status == models.OrderStatusFilled
	}, "Binance add market fill")
	waitOrderCallback(t, addOnce, func(update *models.OrderUpdate) bool {
		return update.ClientOrderID == addClientID && update.Status == models.OrderStatusFilled
	}, "Binance injected add market fill")
	waitPositionCallback(t, positionUpdates, func(position *models.Position) bool {
		return position.Exchange == "binance" && position.Symbol == "BTCUSDT" && position.PositionSide == "LONG" && position.PositionAmt.Abs().GreaterThanOrEqual(decimal.RequireFromString("0.002"))
	}, "Binance add position update")
	waitIntegrationStep(t, "Binance add callbacks and position quantity update")

	position = waitPosition(t, ctx, module, "BTCUSDT", models.PositionSideLong, decimal.RequireFromString("0.002"))
	updateReq := binanceTPSLFromPosition(position, "csutpsl")
	updateTPAlgoOnce := make(chan *models.AlgoOrderUpdate, 1)
	updateSLAlgoOnce := make(chan *models.AlgoOrderUpdate, 1)
	module.OnceAlgoOrder(updateReq.ClientOrderID+"tp", models.OrderEventNew, algoOrderListenerFunc(func(update *models.AlgoOrderUpdate) {
		updateTPAlgoOnce <- update
	}))
	module.OnceAlgoOrder(updateReq.ClientOrderID+"sl", models.OrderEventNew, algoOrderListenerFunc(func(update *models.AlgoOrderUpdate) {
		updateSLAlgoOnce <- update
	}))
	updatedTPSL, err := module.UpdateTPSL(ctx, tpsl, updateReq)
	if err != nil {
		t.Fatalf("UpdateTPSL() error = %v", err)
	}
	if updatedTPSL.TakeProfitOrderID == "" || updatedTPSL.StopLossOrderID == "" {
		t.Fatalf("UpdateTPSL() returned incomplete order IDs: %+v", updatedTPSL)
	}
	waitAlgoOrderCallback(t, updateTPAlgoOnce, func(update *models.AlgoOrderUpdate) bool {
		return update.ClientOrderID == updatedTPSL.TakeProfitClientOrderID && update.Status == models.OrderStatusNew
	}, "Binance injected updated take-profit create")
	waitAlgoOrderCallback(t, updateSLAlgoOnce, func(update *models.AlgoOrderUpdate) bool {
		return update.ClientOrderID == updatedTPSL.StopLossClientOrderID && update.Status == models.OrderStatusNew
	}, "Binance injected updated stop-loss create")
	waitIntegrationStep(t, "Binance refreshed take-profit and stop-loss callbacks")

	cancelTPAlgoOnce := make(chan *models.AlgoOrderUpdate, 1)
	cancelSLAlgoOnce := make(chan *models.AlgoOrderUpdate, 1)
	module.OnceAlgoOrder(updatedTPSL.TakeProfitClientOrderID, models.OrderEventCanceled, algoOrderListenerFunc(func(update *models.AlgoOrderUpdate) {
		cancelTPAlgoOnce <- update
	}))
	module.OnceAlgoOrder(updatedTPSL.StopLossClientOrderID, models.OrderEventCanceled, algoOrderListenerFunc(func(update *models.AlgoOrderUpdate) {
		cancelSLAlgoOnce <- update
	}))
	if err := module.CancelTPSL(ctx, "BTCUSDT", updatedTPSL); err != nil {
		t.Fatalf("CancelTPSL() error = %v", err)
	}
	waitAlgoOrderCallback(t, cancelTPAlgoOnce, func(update *models.AlgoOrderUpdate) bool {
		return update.ClientOrderID == updatedTPSL.TakeProfitClientOrderID && update.Status == models.OrderStatusCanceled
	}, "Binance injected take-profit cancel")
	waitAlgoOrderCallback(t, cancelSLAlgoOnce, func(update *models.AlgoOrderUpdate) bool {
		return update.ClientOrderID == updatedTPSL.StopLossClientOrderID && update.Status == models.OrderStatusCanceled
	}, "Binance injected stop-loss cancel")
	waitIntegrationStep(t, "Binance take-profit and stop-loss cancel callbacks")

	closeOrderID, err := module.ClosePosition(ctx, "BTCUSDT", models.PositionSideLong)
	if err != nil {
		t.Fatalf("ClosePosition() error = %v", err)
	}
	if closeOrderID == "" {
		t.Fatal("ClosePosition() returned empty order ID")
	}
	waitOrderCallback(t, callbacks, func(update *models.OrderUpdate) bool {
		return update.ExchangeID == closeOrderID && update.Status == models.OrderStatusFilled
	}, "Binance close market fill")
	waitPositionCallback(t, positionUpdates, func(position *models.Position) bool {
		return position.Exchange == "binance" && position.Symbol == "BTCUSDT" && position.PositionSide == "LONG" && position.PositionAmt.IsZero()
	}, "Binance close position update")
	waitIntegrationStep(t, "Binance close callbacks and flat position")
}

func binanceTPSLFromPosition(position *models.Position, idPrefix string) models.TPSLReq {
	return models.TPSLReq{
		Symbol:        "BTCUSDT",
		ClientOrderID: fmt.Sprintf("%s%d", idPrefix, time.Now().UnixNano()),
		Side:          models.OrderSideBuy,
		PositionSide:  models.PositionSideLong,
		MarginMode:    models.MarginModeCross,
		Quantity:      position.PositionAmt.Abs(),
		TakeProfit:    position.EntryPrice.Mul(decimal.NewFromFloat(1.02)).Round(1),
		StopLoss:      position.EntryPrice.Mul(decimal.NewFromFloat(0.98)).Round(1),
	}
}

func waitPosition(t *testing.T, ctx context.Context, module *TradingModule, symbol string, side models.PositionSide, min decimal.Decimal) *models.Position {
	t.Helper()
	deadline := time.After(15 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("context canceled while waiting for position: %v", ctx.Err())
		case <-deadline:
			t.Fatalf("timed out waiting for position %s %s >= %s", symbol, side, min)
		case <-ticker.C:
			position, err := module.GetPosition(ctx, symbol, side)
			if err == nil && position.PositionAmt.Abs().GreaterThanOrEqual(min) && !position.EntryPrice.IsZero() {
				return position
			}
		}
	}
}

func waitOpenOrder(t *testing.T, ctx context.Context, module *TradingModule, symbol, clientOrderID string) models.OpenOrder {
	t.Helper()
	deadline := time.After(10 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("context canceled waiting for open order %s: %v", clientOrderID, ctx.Err())
		case <-deadline:
			t.Fatalf("timed out waiting for open order %s", clientOrderID)
		case <-ticker.C:
			orders, err := module.GetOpenOrders(ctx, symbol)
			if err != nil {
				continue
			}
			for _, order := range orders {
				if order.ClientOrderID == clientOrderID {
					return order
				}
			}
		}
	}
}

func waitOpenAlgoOrder(t *testing.T, ctx context.Context, module *TradingModule, symbol, clientOrderID string) models.OpenAlgoOrder {
	t.Helper()
	deadline := time.After(10 * time.Second)
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			t.Fatalf("context canceled waiting for open TPSL order %s: %v", clientOrderID, ctx.Err())
		case <-deadline:
			t.Fatalf("timed out waiting for open TPSL order %s", clientOrderID)
		case <-ticker.C:
			orders, err := module.GetOpenAlgoOrders(ctx, symbol)
			if err != nil {
				continue
			}
			for _, order := range orders {
				if order.ClientOrderID == clientOrderID {
					return order
				}
			}
		}
	}
}

func cleanupBinanceOpenOrders(t *testing.T, ctx context.Context, module *TradingModule, symbol string) {
	t.Helper()
	openOrders, err := module.GetOpenOrders(ctx, symbol)
	if err == nil {
		for _, order := range openOrders {
			if order.ClientOrderID != "" {
				_ = module.CancelOrder(ctx, symbol, order.ClientOrderID)
				continue
			}
			if order.ExchangeID != "" {
				_ = module.cancelOrderID(ctx, symbol, order.ExchangeID)
			}
		}
	}

	tpslOrders, err := module.GetOpenAlgoOrders(ctx, symbol)
	if err == nil {
		for _, order := range tpslOrders {
			if order.ExchangeID != "" {
				_ = module.cancelAlgoOrderID(ctx, order.ExchangeID)
			}
		}
	}
}

func waitBinanceTicker(t *testing.T) *models.Ticker {
	t.Helper()
	tickers := make(chan *models.Ticker, 1)
	module := NewMarketModule(false)
	module.AttachMarketListener(marketListenerFunc(func(ticker *models.Ticker) {
		tickers <- ticker
	}))
	defer module.Stop()

	if err := module.SubscribeTickers([]string{"btcusdt"}); err != nil {
		t.Fatalf("SubscribeTickers() error = %v", err)
	}

	select {
	case ticker := <-tickers:
		return ticker
	case <-time.After(15 * time.Second):
		t.Fatal("timed out waiting for Binance ticker")
	}
	return nil
}

type marketListenerFunc func(*models.Ticker)

func (f marketListenerFunc) OnTicker(ticker *models.Ticker) {
	f(ticker)
}

type tradingListenerFunc func(*models.OrderUpdate)

func (f tradingListenerFunc) OnOrderUpdate(update *models.OrderUpdate) {
	f(update)
}

type algoOrderListenerFunc func(*models.AlgoOrderUpdate)

func (f algoOrderListenerFunc) OnAlgoOrderUpdate(update *models.AlgoOrderUpdate) {
	f(update)
}

func waitOrderCallback(t *testing.T, callbacks <-chan *models.OrderUpdate, match func(*models.OrderUpdate) bool, label string) {
	t.Helper()
	timeout := time.After(20 * time.Second)
	for {
		select {
		case update := <-callbacks:
			if match(update) {
				return
			}
		case <-timeout:
			t.Fatalf("timed out waiting for order callback: %s", label)
		}
	}
}

func waitAlgoOrderCallback(t *testing.T, callbacks <-chan *models.AlgoOrderUpdate, match func(*models.AlgoOrderUpdate) bool, label string) {
	t.Helper()
	deadline := time.After(15 * time.Second)
	for {
		select {
		case update := <-callbacks:
			if match(update) {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %s", label)
		}
	}
}

type accountListener struct {
	onPosition func(*models.Position)
	onBalance  func(*models.AccountBalance)
}

func (l accountListener) OnPositionUpdate(position *models.Position) {
	if l.onPosition != nil {
		l.onPosition(position)
	}
}

func (l accountListener) OnBalanceUpdate(balance *models.AccountBalance) {
	if l.onBalance != nil {
		l.onBalance(balance)
	}
}

type positionListenerFunc func(*models.Position)

func (f positionListenerFunc) OnPositionUpdate(position *models.Position) {
	f(position)
}

type balanceListenerFunc func(*models.AccountBalance)

func (f balanceListenerFunc) OnBalanceUpdate(balance *models.AccountBalance) {
	f(balance)
}

func waitIntegrationStep(t *testing.T, label string) {
	t.Helper()
	t.Logf("%s verified; waiting 2s before next step", label)
	time.Sleep(2 * time.Second)
}

func waitPositionCallback(t *testing.T, callbacks <-chan *models.Position, match func(*models.Position) bool, label string) {
	t.Helper()
	timeout := time.After(20 * time.Second)
	for {
		select {
		case update := <-callbacks:
			if match(update) {
				return
			}
		case <-timeout:
			t.Fatalf("timed out waiting for position callback: %s", label)
		}
	}
}

func waitBalanceCallback(t *testing.T, callbacks <-chan *models.AccountBalance, match func(*models.AccountBalance) bool, label string) {
	t.Helper()
	timeout := time.After(20 * time.Second)
	for {
		select {
		case update := <-callbacks:
			if match(update) {
				return
			}
		case <-timeout:
			t.Fatalf("timed out waiting for balance callback: %s", label)
		}
	}
}
