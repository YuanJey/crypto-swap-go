# crypto-swap-go

High-Frequency Trading SDK for Binance and OKX, featuring lock-free queues, decoupled architecture, and nanosecond latency decoding.

## Changelog
### [v0.1.0-alpha] 
- **Core Architecture**: Introduced `pkg/models` (neutral stream/order models using `shopspring/decimal`) and `pkg/modules` (Listener/Module abstractions).
- **High-Performance Pool**: Added `internal/pool/ringbuffer.go` for lock-free event dispatching.
- **Transport Layer**: Implemented resilient WebSocket client in `internal/transport/ws_client.go` with Exponential Backoff and Goroutine leak prevention.
- **Exchange Adapters**:
  - `internal/exchange/binance`: Added futures book ticker parsing and unified ticker callbacks.
  - `internal/exchange/okx`: Added V5 specific subscription patterns for `tickers`.
- **Factory Entrypoint**: Wired clients dynamically in `pkg/client/factory.go`.
- **Security**: Added HMAC-SHA256 and Base64 signatures in `pkg/secure/signature.go`.

## Trading Account Setup

Configure trading account state explicitly after creating a client:

```go
err := client.TradingModule().ConfigureTradingAccount(ctx, models.TradingAccountConfig{
	PositionMode: models.PositionModeHedge,
	MarginMode:   models.MarginModeCross,
	Leverage:     3,
	Symbols:      []string{"BTCUSDT"},
})
```

This call is intentionally explicit because it changes exchange account state.
When order fields are omitted, orders default to hedge position sides and the
configured margin mode.

## Order Event Routing

Global listeners still receive every order update:

```go
detach := trading.AttachTradingListener(listener)
defer detach()
```

For request-scoped handling, register before placing the order:

```go
clientOrderID := "strategy123open1"
trading.OnceOrder(clientOrderID, models.OrderEventFilled, listener)
_, err := trading.PlaceMarketOrder(ctx, models.PlaceOrderReq{
	Symbol:        "BTCUSDT",
	ClientOrderID: clientOrderID,
	Side:          models.OrderSideBuy,
	PositionSide:  models.PositionSideLong,
	Quantity:      qty,
})
```

Take-profit and stop-loss algo orders use separate listeners. The SDK derives
client algo IDs as `ClientOrderID + "tp"` and `ClientOrderID + "sl"` so the
caller can register callbacks before `PlaceTPSL`:

```go
baseID := "strategy123tpsl1"
trading.OnceAlgoOrder(baseID+"tp", models.OrderEventNew, algoListener)
trading.OnceAlgoOrder(baseID+"sl", models.OrderEventNew, algoListener)
order, err := trading.PlaceTPSL(ctx, models.TPSLReq{
	Symbol:        "BTCUSDT",
	ClientOrderID: baseID,
	Side:          models.OrderSideBuy,
	PositionSide:  models.PositionSideLong,
	Quantity:      qty,
	TakeProfit:    takeProfit,
	StopLoss:      stopLoss,
})
```

## Open Order Management

Query and amend normal limit orders:

```go
orders, err := trading.GetOpenOrders(ctx, "BTCUSDT")

orderID, err := trading.AmendOrder(ctx, models.AmendOrderReq{
	Symbol:        "BTCUSDT",
	ClientOrderID: "strategy123limit1",
	Side:          models.OrderSideBuy, // required by Binance amend order
	Price:         newPrice,
	Quantity:      newQty,
})
```

Query open take-profit / stop-loss algo orders:

```go
algoOrders, err := trading.GetOpenAlgoOrders(ctx, "BTC-USDT-SWAP")
```

Place a trailing order:

```go
trailing, err := trading.PlaceTrailingOrder(ctx, models.TrailingOrderReq{
	Symbol:          "BTC-USDT-SWAP",
	ClientOrderID:   "strategy123trail1",
	Side:            models.OrderSideBuy,
	PositionSide:    models.PositionSideLong,
	MarginMode:      models.MarginModeCross,
	Quantity:        qty,
	ActivationPrice: activePrice,
	CallbackSpread:  decimal.RequireFromString("100"), // price-distance callback
})
```

## Contract Metadata

Use `MarketModule.GetInstrument` to fetch contract metadata before sizing an
order:

```go
instrument, err := client.MarketModule().GetInstrument(ctx, "BTC-USDT-SWAP")
baseMinQty := instrument.BaseLotSize
baseStep := instrument.BaseStepSize
contractValue := instrument.CtVal
```

For OKX swaps, `CtVal` is the exchange contract face value. For Binance USDT-M
contracts, orders are sized in base asset quantity, so `CtVal` and `CtMult`
default to `1`. Strategy sizing can use `BaseLotSize`, `BaseMinQty`, and
`BaseStepSize` as normalized base asset quantities.

## Market Data

Real-time best bid/ask prices:

```go
market.AttachMarketListener(tickerListener)
err := market.SubscribeTickers([]string{"BTCUSDT"})
```

Historical candles:

```go
candles, err := market.GetOHLCV(ctx, "BTC-USDT-SWAP", "1m", 100)
```

Real-time candles:

```go
market.AttachCandleListener(candleListener)
err := market.SubscribeCandles([]string{"BTC-USDT-SWAP"}, "1m")
```

## Integration Tests

Create a `.env` file in the repository root:

```text
BINANCE_API_KEY=...
BINANCE_SECRET_KEY=...
OKX_API_KEY=...
OKX_SECRET_KEY=...
OKX_PASSPHRASE=...
OKX_SIMULATED=1
```

Run offline tests:

```bash
go test ./...
```

Run exchange integration tests against simulation/testnet credentials:

```bash
CRYPTO_SWAP_INTEGRATION=1 go test ./internal/exchange/binance ./internal/exchange/okx -run TestIntegration -count=1 -v
```

The trading integration tests set hedge/long-short position mode and 3x
leverage before placing and canceling small simulation/testnet limit orders.
The full lifecycle tests also cover market open, take-profit/stop-loss order
placement, add-on market orders, position average price refresh, take-profit /
stop-loss replacement, market close, order update callbacks, and account
balance / position WebSocket callbacks.
