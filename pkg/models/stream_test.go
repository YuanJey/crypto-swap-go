package models

import (
	"testing"

	"github.com/shopspring/decimal"
)

func TestInstrumentOrderQuantityFromBaseBinance(t *testing.T) {
	instrument := Instrument{
		CtVal:    decimal.NewFromInt(1),
		CtMult:   decimal.NewFromInt(1),
		StepSize: decimal.RequireFromString("0.001"),
	}
	qty := instrument.OrderQuantityFromBase(decimal.RequireFromString("0.1234"))
	if qty.String() != "0.123" {
		t.Fatalf("OrderQuantityFromBase() = %s, want 0.123", qty)
	}
}

func TestInstrumentOrderQuantityFromFloat(t *testing.T) {
	instrument := Instrument{
		CtVal:    decimal.NewFromInt(1),
		CtMult:   decimal.NewFromInt(1),
		StepSize: decimal.RequireFromString("0.001"),
	}
	qty := instrument.OrderQuantityFromBase(decimal.NewFromFloat(2.1212))
	if qty.String() != "2.121" {
		t.Fatalf("OrderQuantityFromBase() = %s, want 2.121", qty)
	}
}

func TestInstrumentOrderQuantityFromBaseOKX(t *testing.T) {
	instrument := Instrument{
		CtVal:    decimal.RequireFromString("0.1"),
		CtMult:   decimal.NewFromInt(1),
		StepSize: decimal.RequireFromString("0.01"),
	}
	qty := instrument.OrderQuantityFromBase(decimal.RequireFromString("1.234"))
	if qty.String() != "12.34" {
		t.Fatalf("OrderQuantityFromBase() = %s, want 12.34", qty)
	}
}

func TestInstrumentBaseQuantityFromOrder(t *testing.T) {
	instrument := Instrument{
		CtVal:  decimal.RequireFromString("0.1"),
		CtMult: decimal.NewFromInt(1),
	}
	baseQty := instrument.BaseQuantityFromOrder(decimal.RequireFromString("12.34"))
	if baseQty.String() != "1.234" {
		t.Fatalf("BaseQuantityFromOrder() = %s, want 1.234", baseQty)
	}
}

func TestInstrumentPriceToTick(t *testing.T) {
	instrument := Instrument{
		TickSize: decimal.RequireFromString("0.01"),
	}
	price := instrument.PriceToTick(decimal.RequireFromString("1870.857941"))
	if price.String() != "1870.85" {
		t.Fatalf("PriceToTick() = %s, want 1870.85", price)
	}
}
