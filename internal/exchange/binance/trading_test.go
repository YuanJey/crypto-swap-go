package binance

import (
	"testing"

	"github.com/crypto-swap-go/pkg/models"
	"github.com/shopspring/decimal"
)

func TestMapBinanceOrderStatus(t *testing.T) {
	tests := map[string]models.OrderStatus{
		"NEW":              models.OrderStatusNew,
		"PARTIALLY_FILLED": models.OrderStatusPartiallyFilled,
		"FILLED":           models.OrderStatusFilled,
		"CANCELED":         models.OrderStatusCanceled,
		"EXPIRED":          models.OrderStatusCanceled,
		"REJECTED":         models.OrderStatusRejected,
	}

	for input, want := range tests {
		if got := mapBinanceOrderStatus(input); got != want {
			t.Fatalf("mapBinanceOrderStatus(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestBinanceDefaultPositionSide(t *testing.T) {
	tests := []struct {
		name string
		req  models.PlaceOrderReq
		want models.PositionSide
	}{
		{
			name: "buy defaults long",
			req:  models.PlaceOrderReq{Side: models.OrderSideBuy},
			want: models.PositionSideLong,
		},
		{
			name: "sell defaults short",
			req:  models.PlaceOrderReq{Side: models.OrderSideSell},
			want: models.PositionSideShort,
		},
		{
			name: "explicit side wins",
			req:  models.PlaceOrderReq{Side: models.OrderSideBuy, PositionSide: models.PositionSideShort},
			want: models.PositionSideShort,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := binanceDefaultPositionSide(tt.req); got != tt.want {
				t.Fatalf("binanceDefaultPositionSide() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSetBinanceLeverageRejectsInvalidValue(t *testing.T) {
	module := NewTradingModule("", "", true)
	if err := module.SetLeverage(t.Context(), "BTCUSDT", 0, models.PositionSideLong, models.MarginModeCross); err == nil {
		t.Fatal("SetLeverage() error = nil, want error")
	}
}

func TestBinanceTrailingCallbackSpreadToRate(t *testing.T) {
	rate, err := binanceTrailingCallbackRate(models.TrailingOrderReq{
		ActivationPrice: decimal.RequireFromString("10000"),
		CallbackSpread:  decimal.RequireFromString("500"),
	})
	if err != nil {
		t.Fatalf("binanceTrailingCallbackRate() error = %v", err)
	}
	if rate.String() != "5" {
		t.Fatalf("callback rate = %s, want 5", rate)
	}
}
