package okx

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/YuanJey/crypto-swap-go/pkg/models"
)

func TestMarketModuleHandleTicker(t *testing.T) {
	tickers := make(chan *models.Ticker, 1)
	module := NewMarketModule(false)
	module.AttachMarketListener(marketListenerFunc(func(ticker *models.Ticker) {
		tickers <- ticker
	}))

	module.handleMessage([]byte(`{"arg":{"channel":"tickers","instId":"BTC-USDT-SWAP"},"data":[{"instId":"BTC-USDT-SWAP","bidPx":"63227.6","bidSz":"10","askPx":"63227.7","askSz":"11","ts":"1785662887347"}]}`))

	select {
	case ticker := <-tickers:
		if ticker.Symbol != "BTC-USDT-SWAP" {
			t.Fatalf("ticker.Symbol = %q, want BTC-USDT-SWAP", ticker.Symbol)
		}
		if ticker.BidPrice.String() != "63227.6" {
			t.Fatalf("ticker.BidPrice = %s, want 63227.6", ticker.BidPrice)
		}
	default:
		t.Fatal("handleMessage did not emit a ticker")
	}
}

func TestMarketModuleHandleCandle(t *testing.T) {
	candles := make(chan *models.Candle, 1)
	module := NewMarketModule(false)
	module.AttachCandleListener(candleListenerFunc(func(candle *models.Candle) {
		candles <- candle
	}))

	module.handleMessage([]byte(`{"arg":{"channel":"candle1m","instId":"BTC-USDT-SWAP"},"data":[["1785662820000","63200.1","63300.2","63100.3","63250.4","12.5","0.125","790630.0","1"]]}`))

	select {
	case candle := <-candles:
		if candle.Symbol != "BTC-USDT-SWAP" {
			t.Fatalf("candle.Symbol = %q, want BTC-USDT-SWAP", candle.Symbol)
		}
		if candle.Close.String() != "63250.4" {
			t.Fatalf("candle.Close = %s, want 63250.4", candle.Close)
		}
		if candle.Confirm != 1 {
			t.Fatalf("candle.Confirm = %d, want 1", candle.Confirm)
		}
	default:
		t.Fatal("handleMessage did not emit a candle")
	}
}

func TestMarketModuleGetOHLCV(t *testing.T) {
	module := NewMarketModule(true)
	module.baseURL = "https://example.test"
	module.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/api/v5/market/candles" {
			t.Fatalf("path = %q, want /api/v5/market/candles", r.URL.Path)
		}
		if r.URL.Query().Get("instId") != "BTC-USDT-SWAP" {
			t.Fatalf("instId query = %q, want BTC-USDT-SWAP", r.URL.Query().Get("instId"))
		}
		if r.URL.Query().Get("bar") != "1m" {
			t.Fatalf("bar query = %q, want 1m", r.URL.Query().Get("bar"))
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body: io.NopCloser(strings.NewReader(`{
				"code": "0",
				"data": [["1785662820000","63200.1","63300.2","63100.3","63250.4","12.5","0.125","790630.0","1"]]
			}`)),
		}, nil
	})}

	candles, err := module.GetOHLCV(context.Background(), "BTC-USDT-SWAP", "1m", 1)
	if err != nil {
		t.Fatalf("GetOHLCV() error = %v", err)
	}
	if len(candles) != 1 {
		t.Fatalf("len(candles) = %d, want 1", len(candles))
	}
	if candles[0].Close.String() != "63250.4" {
		t.Fatalf("Close = %s, want 63250.4", candles[0].Close)
	}
	if candles[0].VolCcy.String() != "0.125" {
		t.Fatalf("VolCcy = %s, want 0.125", candles[0].VolCcy)
	}
}

func TestMarketModuleGetInstrument(t *testing.T) {
	module := NewMarketModule(true)
	module.baseURL = "https://example.test"
	module.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/api/v5/public/instruments" {
			t.Fatalf("path = %q, want /api/v5/public/instruments", r.URL.Path)
		}
		if r.URL.Query().Get("instType") != "SWAP" {
			t.Fatalf("instType query = %q, want SWAP", r.URL.Query().Get("instType"))
		}
		if r.URL.Query().Get("instId") != "BTC-USDT-SWAP" {
			t.Fatalf("instId query = %q, want BTC-USDT-SWAP", r.URL.Query().Get("instId"))
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body: io.NopCloser(strings.NewReader(`{
			"code": "0",
			"data": [{
				"instId": "BTC-USDT-SWAP",
				"baseCcy": "BTC",
				"quoteCcy": "USDT",
				"instType": "SWAP",
				"ctVal": "0.01",
				"ctMult": "1",
				"lotSz": "0.01",
				"minSz": "0.01",
				"tickSz": "0.1"
			}]
		}`)),
		}, nil
	})}

	instrument, err := module.GetInstrument(context.Background(), "BTC-USDT-SWAP")
	if err != nil {
		t.Fatalf("GetInstrument() error = %v", err)
	}
	if instrument.Symbol != "BTC-USDT-SWAP" {
		t.Fatalf("Symbol = %q, want BTC-USDT-SWAP", instrument.Symbol)
	}
	if instrument.CtVal.String() != "0.01" {
		t.Fatalf("CtVal = %s, want 0.01", instrument.CtVal)
	}
	if instrument.LotSize.String() != "0.01" {
		t.Fatalf("LotSize = %s, want 0.01", instrument.LotSize)
	}
	if instrument.TickSize.String() != "0.1" {
		t.Fatalf("TickSize = %s, want 0.1", instrument.TickSize)
	}
	if instrument.BaseLotSize.String() != "0.0001" {
		t.Fatalf("BaseLotSize = %s, want 0.0001", instrument.BaseLotSize)
	}
	if instrument.BaseStepSize.String() != "0.0001" {
		t.Fatalf("BaseStepSize = %s, want 0.0001", instrument.BaseStepSize)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type candleListenerFunc func(*models.Candle)

func (f candleListenerFunc) OnCandle(candle *models.Candle) {
	f(candle)
}
