package binance

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/YuanJey/crypto-swap-go/pkg/models"
)

func TestMarketModuleHandleBookTicker(t *testing.T) {
	tickers := make(chan *models.Ticker, 1)
	module := NewMarketModule(false)
	module.AttachMarketListener(marketListenerFunc(func(ticker *models.Ticker) {
		tickers <- ticker
	}))

	module.handleMessage([]byte(`{"e":"bookTicker","u":1,"s":"BTCUSDT","b":"63227.60","B":"13.827","a":"63227.70","A":"22.052","T":1785662887347}`))

	select {
	case ticker := <-tickers:
		if ticker.Symbol != "BTCUSDT" {
			t.Fatalf("ticker.Symbol = %q, want BTCUSDT", ticker.Symbol)
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

	module.handleCandleMessage([]byte(`{"e":"kline","E":1785662887347,"s":"BTCUSDT","k":{"t":1785662820000,"i":"1m","o":"63200.1","h":"63300.2","l":"63100.3","c":"63250.4","v":"12.5","q":"790630.0","x":true}}`))

	select {
	case candle := <-candles:
		if candle.Symbol != "BTCUSDT" {
			t.Fatalf("candle.Symbol = %q, want BTCUSDT", candle.Symbol)
		}
		if candle.Close.String() != "63250.4" {
			t.Fatalf("candle.Close = %s, want 63250.4", candle.Close)
		}
		if candle.Confirm != 1 {
			t.Fatalf("candle.Confirm = %d, want 1", candle.Confirm)
		}
	default:
		t.Fatal("handleCandleMessage did not emit a candle")
	}
}

func TestMarketModuleGetOHLCV(t *testing.T) {
	module := NewMarketModule(true)
	module.baseURL = "https://example.test"
	module.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/fapi/v1/klines" {
			t.Fatalf("path = %q, want /fapi/v1/klines", r.URL.Path)
		}
		if r.URL.Query().Get("symbol") != "BTCUSDT" {
			t.Fatalf("symbol query = %q, want BTCUSDT", r.URL.Query().Get("symbol"))
		}
		if r.URL.Query().Get("interval") != "1m" {
			t.Fatalf("interval query = %q, want 1m", r.URL.Query().Get("interval"))
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body: io.NopCloser(strings.NewReader(`[
				[1785662820000,"63200.1","63300.2","63100.3","63250.4","12.5",1785662879999,"790630.0",10,"1","2","0"]
			]`)),
		}, nil
	})}

	candles, err := module.GetOHLCV(context.Background(), "BTC-USDT", "1m", 1)
	if err != nil {
		t.Fatalf("GetOHLCV() error = %v", err)
	}
	if len(candles) != 1 {
		t.Fatalf("len(candles) = %d, want 1", len(candles))
	}
	if candles[0].Close.String() != "63250.4" {
		t.Fatalf("Close = %s, want 63250.4", candles[0].Close)
	}
	if candles[0].VolCcyQuote.String() != "790630" {
		t.Fatalf("VolCcyQuote = %s, want 790630", candles[0].VolCcyQuote)
	}
}

func TestMarketModuleGetInstrument(t *testing.T) {
	module := NewMarketModule(true)
	module.baseURL = "https://example.test"
	module.client = &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Path != "/fapi/v1/exchangeInfo" {
			t.Fatalf("path = %q, want /fapi/v1/exchangeInfo", r.URL.Path)
		}
		if r.URL.Query().Get("symbol") != "BTCUSDT" {
			t.Fatalf("symbol query = %q, want BTCUSDT", r.URL.Query().Get("symbol"))
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Body: io.NopCloser(strings.NewReader(`{
			"symbols": [{
				"symbol": "ETHUSDT",
				"baseAsset": "ETH",
				"quoteAsset": "USDT",
				"contractType": "PERPETUAL",
				"filters": [
					{"filterType": "PRICE_FILTER", "tickSize": "0.01"},
					{"filterType": "LOT_SIZE", "minQty": "0.001", "stepSize": "0.001"}
				]
			}, {
				"symbol": "BTCUSDT",
				"baseAsset": "BTC",
				"quoteAsset": "USDT",
				"contractType": "PERPETUAL",
				"filters": [
					{"filterType": "PRICE_FILTER", "tickSize": "0.10"},
					{"filterType": "LOT_SIZE", "minQty": "0.001", "stepSize": "0.001"}
				]
			}]
		}`)),
		}, nil
	})}

	instrument, err := module.GetInstrument(context.Background(), "BTC-USDT")
	if err != nil {
		t.Fatalf("GetInstrument() error = %v", err)
	}
	if instrument.Symbol != "BTCUSDT" {
		t.Fatalf("Symbol = %q, want BTCUSDT", instrument.Symbol)
	}
	if instrument.LotSize.String() != "0.001" {
		t.Fatalf("LotSize = %s, want 0.001", instrument.LotSize)
	}
	if instrument.CtVal.String() != "1" {
		t.Fatalf("CtVal = %s, want 1", instrument.CtVal)
	}
	if instrument.TickSize.String() != "0.1" {
		t.Fatalf("TickSize = %s, want 0.1", instrument.TickSize)
	}
	if instrument.BaseLotSize.String() != "0.001" {
		t.Fatalf("BaseLotSize = %s, want 0.001", instrument.BaseLotSize)
	}
	if instrument.BaseStepSize.String() != "0.001" {
		t.Fatalf("BaseStepSize = %s, want 0.001", instrument.BaseStepSize)
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
