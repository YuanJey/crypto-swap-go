package okx

import (
	"testing"

	"github.com/YuanJey/crypto-swap-go/pkg/models"
)

func TestMapOKXOrderStatus(t *testing.T) {
	tests := map[string]models.OrderStatus{
		"live":             models.OrderStatusNew,
		"partially_filled": models.OrderStatusPartiallyFilled,
		"filled":           models.OrderStatusFilled,
		"canceled":         models.OrderStatusCanceled,
	}

	for input, want := range tests {
		if got := mapOKXOrderStatus(input); got != want {
			t.Fatalf("mapOKXOrderStatus(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestOKXDefaultPositionSide(t *testing.T) {
	tests := []struct {
		name string
		req  models.PlaceOrderReq
		want string
	}{
		{
			name: "buy defaults long",
			req:  models.PlaceOrderReq{Side: models.OrderSideBuy},
			want: "long",
		},
		{
			name: "sell defaults short",
			req:  models.PlaceOrderReq{Side: models.OrderSideSell},
			want: "short",
		},
		{
			name: "explicit side wins",
			req:  models.PlaceOrderReq{Side: models.OrderSideBuy, PositionSide: models.PositionSideShort},
			want: "short",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := okxDefaultPositionSide(tt.req); got != tt.want {
				t.Fatalf("okxDefaultPositionSide() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestSetOKXLeverageRejectsInvalidValue(t *testing.T) {
	module := NewTradingModule("", "", "", true)
	if err := module.SetLeverage(t.Context(), "BTC-USDT-SWAP", 0, models.PositionSideLong, models.MarginModeCross); err == nil {
		t.Fatal("SetLeverage() error = nil, want error")
	}
}
