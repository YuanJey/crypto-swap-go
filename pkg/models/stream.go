package models

import "github.com/shopspring/decimal"

// Ticker represents unified highest bid / lowest ask quotes
type Ticker struct {
	Exchange  string
	Symbol    string
	BidPrice  decimal.Decimal
	BidQty    decimal.Decimal
	AskPrice  decimal.Decimal
	AskQty    decimal.Decimal
	Timestamp int64
}

// Candle represents unified OHLCV data.
type Candle struct {
	Exchange    string
	Symbol      string
	Timeframe   string
	Timestamp   int64
	Open        decimal.Decimal
	High        decimal.Decimal
	Low         decimal.Decimal
	Close       decimal.Decimal
	Volume      decimal.Decimal
	VolCcy      decimal.Decimal
	VolCcyQuote decimal.Decimal
	Confirm     int
}

// Instrument represents unified perpetual contract metadata.
type Instrument struct {
	Exchange     string
	Symbol       string
	BaseAsset    string
	QuoteAsset   string
	ContractType string
	CtVal        decimal.Decimal // Contract face value; Binance USDT-M uses 1 base asset unit.
	CtMult       decimal.Decimal // Contract multiplier; defaults to 1 when not provided.
	LotSize      decimal.Decimal // Minimum order quantity in exchange order units.
	MinQty       decimal.Decimal
	StepSize     decimal.Decimal
	BaseLotSize  decimal.Decimal // LotSize converted to base asset quantity.
	BaseMinQty   decimal.Decimal // MinQty converted to base asset quantity.
	BaseStepSize decimal.Decimal // StepSize converted to base asset quantity.
	TickSize     decimal.Decimal
}

// OrderQuantityFromBase converts a normalized base-asset quantity into the
// exchange order quantity. OKX swaps return contract count; Binance USDT-M
// returns the same base quantity because CtVal/CtMult default to 1.
func (i Instrument) OrderQuantityFromBase(baseQty decimal.Decimal) decimal.Decimal {
	if baseQty.IsZero() {
		return decimal.Zero
	}
	unit := i.contractUnit()
	qty := baseQty.Div(unit)
	return floorToStep(qty, i.exchangeStep())
}

// BaseQuantityFromOrder converts an exchange order quantity into base-asset
// quantity. For OKX this is contracts * contract value; for Binance it is qty.
func (i Instrument) BaseQuantityFromOrder(orderQty decimal.Decimal) decimal.Decimal {
	if orderQty.IsZero() {
		return decimal.Zero
	}
	return orderQty.Mul(i.contractUnit())
}

// PriceToTick floors a price to the exchange tick size.
func (i Instrument) PriceToTick(price decimal.Decimal) decimal.Decimal {
	if price.IsZero() {
		return decimal.Zero
	}
	return floorToStep(price, i.TickSize)
}

func (i Instrument) contractUnit() decimal.Decimal {
	unit := i.CtVal
	if unit.IsZero() {
		unit = decimal.NewFromInt(1)
	}
	if !i.CtMult.IsZero() {
		unit = unit.Mul(i.CtMult)
	}
	if unit.IsZero() {
		return decimal.NewFromInt(1)
	}
	return unit
}

func (i Instrument) exchangeStep() decimal.Decimal {
	if !i.StepSize.IsZero() {
		return i.StepSize
	}
	if !i.LotSize.IsZero() {
		return i.LotSize
	}
	if !i.MinQty.IsZero() {
		return i.MinQty
	}
	return decimal.Zero
}

func floorToStep(value, step decimal.Decimal) decimal.Decimal {
	if step.IsZero() {
		return value
	}
	rounded := value.Div(step).Floor().Mul(step)
	return rounded.RoundFloor(decimalPlaces(step))
}

func decimalPlaces(value decimal.Decimal) int32 {
	if value.Exponent() >= 0 {
		return 0
	}
	return -value.Exponent()
}

// Position represents unified perpetual contract position
type Position struct {
	Exchange         string
	Symbol           string
	PositionSide     string // "LONG", "SHORT", "BOTH"
	PositionAmt      decimal.Decimal
	EntryPrice       decimal.Decimal
	MarkPrice        decimal.Decimal
	UnRealizedPnL    decimal.Decimal
	LiquidationPrice decimal.Decimal
	Leverage         int
	UpdateTime       int64
}

// AccountBalance represents user's wallet balance
type AccountBalance struct {
	Exchange   string
	Asset      string // e.g., "USDT"
	Balance    decimal.Decimal
	Available  decimal.Decimal
	UpdateTime int64
}
