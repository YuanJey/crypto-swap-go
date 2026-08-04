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
