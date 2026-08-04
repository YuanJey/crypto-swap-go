package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/crypto-swap-go/internal/transport"
	"github.com/crypto-swap-go/pkg/models"
	"github.com/crypto-swap-go/pkg/modules"
	"github.com/shopspring/decimal"
)

type binanceBookTicker struct {
	StreamEvent string `json:"e"`
	EventTime   int64  `json:"E"`
	Symbol      string `json:"s"`
	BidPrice    string `json:"b"`
	BidQty      string `json:"B"`
	AskPrice    string `json:"a"`
	AskQty      string `json:"A"`
	Timestamp   int64  `json:"T"`
}

type binanceKlineEvent struct {
	StreamEvent string `json:"e"`
	EventTime   int64  `json:"E"`
	Symbol      string `json:"s"`
	Kline       struct {
		StartTime int64  `json:"t"`
		Interval  string `json:"i"`
		Open      string `json:"o"`
		High      string `json:"h"`
		Low       string `json:"l"`
		Close     string `json:"c"`
		Volume    string `json:"v"`
		Closed    bool   `json:"x"`
		QuoteVol  string `json:"q"`
	} `json:"k"`
}

type MarketModule struct {
	wsClient       *transport.WSClient
	candleWSClient *transport.WSClient
	listener       modules.MarketListener
	candleListener modules.CandleListener
	testnet        bool
	baseURL        string
	client         *http.Client
}

func NewMarketModule(testnet bool) *MarketModule {
	baseURL := "https://fapi.binance.com"
	if testnet {
		baseURL = "https://testnet.binancefuture.com"
	}
	return &MarketModule{
		testnet: testnet,
		baseURL: baseURL,
		client:  &http.Client{Timeout: 5 * time.Second},
	}
}

func (m *MarketModule) AttachMarketListener(listener modules.MarketListener) {
	m.listener = listener
}

func (m *MarketModule) AttachCandleListener(listener modules.CandleListener) {
	m.candleListener = listener
}

func (m *MarketModule) Start(ctx context.Context) error {
	return nil // WS started dynamically upon SubscribeTickers
}

func (m *MarketModule) Stop() {
	if m.wsClient != nil {
		m.wsClient.Stop()
	}
	if m.candleWSClient != nil {
		m.candleWSClient.Stop()
	}
}

func (m *MarketModule) SubscribeTickers(symbols []string) error {
	if len(symbols) == 0 {
		return nil
	}

	url := "wss://fstream.binance.com/ws"
	if m.testnet {
		url = "wss://stream.binancefuture.com/ws"
	}

	var streams []string
	for _, sym := range symbols {
		streams = append(streams, fmt.Sprintf("%s@bookTicker", strings.ToLower(sym)))
	}

	finalURL := fmt.Sprintf("%s/%s", url, streams[0])
	if len(streams) > 1 {
		finalURL = fmt.Sprintf("%s/stream?streams=%s", strings.TrimSuffix(url, "/ws"), strings.Join(streams, "/"))
	}

	if m.wsClient != nil {
		m.wsClient.Stop()
	}

	m.wsClient = transport.NewWSClient(finalURL, m.handleMessage)
	return m.wsClient.Start()
}

func (m *MarketModule) SubscribeCandles(symbols []string, timeframe string) error {
	if len(symbols) == 0 {
		return nil
	}
	if timeframe == "" {
		timeframe = "1m"
	}

	wsURL := "wss://fstream.binance.com/ws"
	if m.testnet {
		wsURL = "wss://stream.binancefuture.com/ws"
	}

	var streams []string
	for _, sym := range symbols {
		streams = append(streams, fmt.Sprintf("%s@kline_%s", strings.ToLower(strings.ReplaceAll(sym, "-", "")), timeframe))
	}

	finalURL := fmt.Sprintf("%s/%s", wsURL, streams[0])
	if len(streams) > 1 {
		finalURL = fmt.Sprintf("%s/stream?streams=%s", strings.TrimSuffix(wsURL, "/ws"), strings.Join(streams, "/"))
	}

	if m.candleWSClient != nil {
		m.candleWSClient.Stop()
	}

	m.candleWSClient = transport.NewWSClient(finalURL, m.handleCandleMessage)
	return m.candleWSClient.Start()
}

func (m *MarketModule) GetOHLCV(ctx context.Context, symbol, timeframe string, limit int) ([]models.Candle, error) {
	if timeframe == "" {
		timeframe = "1m"
	}
	symbol = strings.ReplaceAll(strings.ToUpper(symbol), "-", "")
	endpoint := fmt.Sprintf("%s/fapi/v1/klines?symbol=%s&interval=%s", m.baseURL, symbol, timeframe)
	if limit > 0 {
		endpoint += fmt.Sprintf("&limit=%d", limit)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	resp, err := m.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("binance get candles %s %s: HTTP %d: %s", symbol, timeframe, resp.StatusCode, string(body))
	}

	var rows [][]interface{}
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, err
	}
	candles := make([]models.Candle, 0, len(rows))
	for _, row := range rows {
		if len(row) < 8 {
			continue
		}
		candles = append(candles, models.Candle{
			Exchange:    "binance",
			Symbol:      symbol,
			Timeframe:   timeframe,
			Timestamp:   int64FromInterface(row[0]),
			Open:        decimalFromInterface(row[1]),
			High:        decimalFromInterface(row[2]),
			Low:         decimalFromInterface(row[3]),
			Close:       decimalFromInterface(row[4]),
			Volume:      decimalFromInterface(row[5]),
			VolCcyQuote: decimalFromInterface(row[7]),
			Confirm:     1,
		})
	}
	return candles, nil
}

func (m *MarketModule) GetInstrument(ctx context.Context, symbol string) (*models.Instrument, error) {
	symbol = strings.ReplaceAll(strings.ToUpper(symbol), "-", "")
	req, err := http.NewRequestWithContext(ctx, "GET", m.baseURL+"/fapi/v1/exchangeInfo?symbol="+symbol, nil)
	if err != nil {
		return nil, err
	}

	resp, err := m.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("binance get instrument %s: HTTP %d: %s", symbol, resp.StatusCode, string(body))
	}

	var data struct {
		Symbols []struct {
			Symbol       string `json:"symbol"`
			BaseAsset    string `json:"baseAsset"`
			QuoteAsset   string `json:"quoteAsset"`
			ContractType string `json:"contractType"`
			Filters      []struct {
				FilterType string `json:"filterType"`
				MinQty     string `json:"minQty"`
				StepSize   string `json:"stepSize"`
				TickSize   string `json:"tickSize"`
			} `json:"filters"`
		} `json:"symbols"`
	}
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, err
	}
	if len(data.Symbols) == 0 {
		return nil, fmt.Errorf("binance instrument %s not found", symbol)
	}

	item := data.Symbols[0]
	instrument := &models.Instrument{
		Exchange:     "binance",
		Symbol:       item.Symbol,
		BaseAsset:    item.BaseAsset,
		QuoteAsset:   item.QuoteAsset,
		ContractType: item.ContractType,
		CtVal:        decimal.NewFromInt(1),
		CtMult:       decimal.NewFromInt(1),
	}
	for _, filter := range item.Filters {
		switch filter.FilterType {
		case "LOT_SIZE", "MARKET_LOT_SIZE":
			if instrument.MinQty.IsZero() && filter.MinQty != "" {
				instrument.MinQty, _ = decimal.NewFromString(filter.MinQty)
				instrument.LotSize = instrument.MinQty
			}
			if instrument.StepSize.IsZero() && filter.StepSize != "" {
				instrument.StepSize, _ = decimal.NewFromString(filter.StepSize)
			}
		case "PRICE_FILTER":
			if filter.TickSize != "" {
				instrument.TickSize, _ = decimal.NewFromString(filter.TickSize)
			}
		}
	}
	fillBaseInstrumentQuantities(instrument)
	return instrument, nil
}

func fillBaseInstrumentQuantities(instrument *models.Instrument) {
	if instrument == nil {
		return
	}
	ctVal := instrument.CtVal
	if ctVal.IsZero() {
		ctVal = decimal.NewFromInt(1)
	}
	instrument.BaseLotSize = instrument.LotSize.Mul(ctVal)
	instrument.BaseMinQty = instrument.MinQty.Mul(ctVal)
	instrument.BaseStepSize = instrument.StepSize.Mul(ctVal)
}

func (m *MarketModule) handleMessage(msg []byte) {
	if m.listener == nil {
		return
	}

	var wrapped struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(msg, &wrapped); err == nil && len(wrapped.Data) > 0 {
		msg = wrapped.Data
	}

	var data binanceBookTicker
	if err := json.Unmarshal(msg, &data); err != nil {
		return
	}

	if data.StreamEvent != "" && data.StreamEvent != "bookTicker" {
		return
	}
	if data.Symbol == "" || data.BidPrice == "" || data.AskPrice == "" {
		return
	}

	bidPrice, _ := decimal.NewFromString(data.BidPrice)
	bidQty, _ := decimal.NewFromString(data.BidQty)
	askPrice, _ := decimal.NewFromString(data.AskPrice)
	askQty, _ := decimal.NewFromString(data.AskQty)

	m.listener.OnTicker(&models.Ticker{
		Exchange:  "binance",
		Symbol:    data.Symbol,
		BidPrice:  bidPrice,
		BidQty:    bidQty,
		AskPrice:  askPrice,
		AskQty:    askQty,
		Timestamp: data.Timestamp,
	})
}

func (m *MarketModule) handleCandleMessage(msg []byte) {
	if m.candleListener == nil {
		return
	}

	var wrapped struct {
		Data json.RawMessage `json:"data"`
	}
	if err := json.Unmarshal(msg, &wrapped); err == nil && len(wrapped.Data) > 0 {
		msg = wrapped.Data
	}

	var data binanceKlineEvent
	if err := json.Unmarshal(msg, &data); err != nil {
		return
	}
	if data.StreamEvent != "kline" || data.Symbol == "" {
		return
	}

	open, _ := decimal.NewFromString(data.Kline.Open)
	high, _ := decimal.NewFromString(data.Kline.High)
	low, _ := decimal.NewFromString(data.Kline.Low)
	closePrice, _ := decimal.NewFromString(data.Kline.Close)
	volume, _ := decimal.NewFromString(data.Kline.Volume)
	quoteVolume, _ := decimal.NewFromString(data.Kline.QuoteVol)
	confirm := 0
	if data.Kline.Closed {
		confirm = 1
	}

	m.candleListener.OnCandle(&models.Candle{
		Exchange:    "binance",
		Symbol:      data.Symbol,
		Timeframe:   data.Kline.Interval,
		Timestamp:   data.Kline.StartTime,
		Open:        open,
		High:        high,
		Low:         low,
		Close:       closePrice,
		Volume:      volume,
		VolCcyQuote: quoteVolume,
		Confirm:     confirm,
	})
}

func decimalFromInterface(value interface{}) decimal.Decimal {
	switch v := value.(type) {
	case string:
		d, _ := decimal.NewFromString(v)
		return d
	case float64:
		return decimal.NewFromFloat(v)
	default:
		return decimal.Zero
	}
}

func int64FromInterface(value interface{}) int64 {
	switch v := value.(type) {
	case float64:
		return int64(v)
	case string:
		var result int64
		fmt.Sscanf(v, "%d", &result)
		return result
	default:
		return 0
	}
}
