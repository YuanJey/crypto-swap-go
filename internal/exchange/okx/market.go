package okx

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/crypto-swap-go/internal/transport"
	"github.com/crypto-swap-go/pkg/models"
	"github.com/crypto-swap-go/pkg/modules"
	"github.com/shopspring/decimal"
)

type okxTickerResponse struct {
	Arg struct {
		Channel string `json:"channel"`
		InstId  string `json:"instId"`
	} `json:"arg"`
	Data []struct {
		InstId string `json:"instId"`
		Last   string `json:"last"`
		BidPx  string `json:"bidPx"`
		BidSz  string `json:"bidSz"`
		AskPx  string `json:"askPx"`
		AskSz  string `json:"askSz"`
		Ts     string `json:"ts"`
	} `json:"data"`
}

type okxCandleResponse struct {
	Arg struct {
		Channel string `json:"channel"`
		InstID  string `json:"instId"`
	} `json:"arg"`
	Data [][]string `json:"data"`
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
	baseURL := "https://www.okx.com"
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
	url := "wss://ws.okx.com:8443/ws/v5/public"
	if m.testnet {
		url = "wss://wspap.okx.com:8443/ws/v5/public?brokerId=9999"
	}

	m.wsClient = transport.NewWSClient(url, m.handleMessage)
	return m.wsClient.Start()
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
	if m.wsClient == nil {
		return fmt.Errorf("okx market module not started")
	}

	if len(symbols) == 0 {
		return nil
	}

	type args struct {
		Channel string `json:"channel"`
		InstId  string `json:"instId"`
	}

	var argsList []args
	for _, sym := range symbols {
		argsList = append(argsList, args{
			Channel: "tickers",
			InstId:  strings.ToUpper(sym),
		})
	}

	req := map[string]interface{}{
		"op":   "subscribe",
		"args": argsList,
	}

	payload, _ := json.Marshal(req)
	return m.wsClient.Send(payload)
}

func (m *MarketModule) SubscribeCandles(symbols []string, timeframe string) error {
	if len(symbols) == 0 {
		return nil
	}
	if timeframe == "" {
		timeframe = "1m"
	}
	if m.candleWSClient == nil {
		wsURL := "wss://ws.okx.com:8443/ws/v5/business"
		if m.testnet {
			wsURL = "wss://wspap.okx.com:8443/ws/v5/business?brokerId=9999"
		}
		m.candleWSClient = transport.NewWSClient(wsURL, m.handleMessage)
		if err := m.candleWSClient.Start(); err != nil {
			return err
		}
	}

	channel := "candle" + timeframe
	var argsList []map[string]string
	for _, sym := range symbols {
		argsList = append(argsList, map[string]string{
			"channel": channel,
			"instId":  strings.ToUpper(sym),
		})
	}
	payload, _ := json.Marshal(map[string]interface{}{
		"op":   "subscribe",
		"args": argsList,
	})
	return m.candleWSClient.Send(payload)
}

func (m *MarketModule) GetOHLCV(ctx context.Context, symbol, timeframe string, limit int) ([]models.Candle, error) {
	if timeframe == "" {
		timeframe = "1m"
	}
	params := url.Values{}
	params.Set("instId", strings.ToUpper(symbol))
	params.Set("bar", timeframe)
	if limit > 0 {
		params.Set("limit", fmt.Sprintf("%d", limit))
	}
	endpoint := m.baseURL + "/api/v5/market/candles?" + params.Encode()
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
		return nil, fmt.Errorf("okx get candles %s %s: HTTP %d: %s", symbol, timeframe, resp.StatusCode, string(body))
	}

	var data struct {
		Code string     `json:"code"`
		Msg  string     `json:"msg"`
		Data [][]string `json:"data"`
	}
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, err
	}
	if data.Code != "" && data.Code != "0" {
		return nil, fmt.Errorf("okx get candles %s %s: %s %s", symbol, timeframe, data.Code, data.Msg)
	}

	candles := make([]models.Candle, 0, len(data.Data))
	for i := len(data.Data) - 1; i >= 0; i-- {
		candle, ok := parseOKXCandle("okx", strings.ToUpper(symbol), timeframe, data.Data[i])
		if ok {
			candles = append(candles, candle)
		}
	}
	return candles, nil
}

func (m *MarketModule) GetInstrument(ctx context.Context, symbol string) (*models.Instrument, error) {
	symbol = strings.ToUpper(symbol)
	endpoint := m.baseURL + "/api/v5/public/instruments?instType=SWAP&instId=" + url.QueryEscape(symbol)
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
		return nil, fmt.Errorf("okx get instrument %s: HTTP %d: %s", symbol, resp.StatusCode, string(body))
	}

	var data struct {
		Code string `json:"code"`
		Msg  string `json:"msg"`
		Data []struct {
			InstID     string `json:"instId"`
			BaseCcy    string `json:"baseCcy"`
			QuoteCcy   string `json:"quoteCcy"`
			InstType   string `json:"instType"`
			CtVal      string `json:"ctVal"`
			CtMult     string `json:"ctMult"`
			LotSz      string `json:"lotSz"`
			MinSz      string `json:"minSz"`
			TickSz     string `json:"tickSz"`
			ContractCt string `json:"ctType"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, err
	}
	if data.Code != "" && data.Code != "0" {
		return nil, fmt.Errorf("okx get instrument %s: %s %s", symbol, data.Code, data.Msg)
	}
	if len(data.Data) == 0 {
		return nil, fmt.Errorf("okx instrument %s not found", symbol)
	}

	item := data.Data[0]
	ctVal, _ := decimal.NewFromString(item.CtVal)
	ctMult, _ := decimal.NewFromString(item.CtMult)
	lotSize, _ := decimal.NewFromString(item.LotSz)
	minQty, _ := decimal.NewFromString(item.MinSz)
	tickSize, _ := decimal.NewFromString(item.TickSz)
	if minQty.IsZero() {
		minQty = lotSize
	}
	if ctMult.IsZero() {
		ctMult = decimal.NewFromInt(1)
	}

	instrument := &models.Instrument{
		Exchange:     "okx",
		Symbol:       item.InstID,
		BaseAsset:    item.BaseCcy,
		QuoteAsset:   item.QuoteCcy,
		ContractType: item.InstType,
		CtVal:        ctVal,
		CtMult:       ctMult,
		LotSize:      lotSize,
		MinQty:       minQty,
		StepSize:     lotSize,
		TickSize:     tickSize,
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
	var header struct {
		Arg struct {
			Channel string `json:"channel"`
		} `json:"arg"`
	}
	if err := json.Unmarshal(msg, &header); err != nil {
		return
	}
	if strings.HasPrefix(header.Arg.Channel, "candle") {
		m.handleCandleMessage(msg)
		return
	}
	m.handleTickerMessage(msg)
}

func (m *MarketModule) handleTickerMessage(msg []byte) {
	if m.listener == nil {
		return
	}

	var resp okxTickerResponse
	if err := json.Unmarshal(msg, &resp); err != nil {
		return
	}
	if resp.Arg.Channel != "tickers" || len(resp.Data) == 0 {
		return
	}

	for _, d := range resp.Data {
		bidPrice, _ := decimal.NewFromString(d.BidPx)
		bidQty, _ := decimal.NewFromString(d.BidSz)
		askPrice, _ := decimal.NewFromString(d.AskPx)
		askQty, _ := decimal.NewFromString(d.AskSz)

		var ts int64
		fmt.Sscanf(d.Ts, "%d", &ts)

		m.listener.OnTicker(&models.Ticker{
			Exchange:  "okx",
			Symbol:    d.InstId,
			BidPrice:  bidPrice,
			BidQty:    bidQty,
			AskPrice:  askPrice,
			AskQty:    askQty,
			Timestamp: ts,
		})
	}
}

func (m *MarketModule) handleCandleMessage(msg []byte) {
	if m.candleListener == nil {
		return
	}

	var resp okxCandleResponse
	if err := json.Unmarshal(msg, &resp); err != nil {
		return
	}
	if !strings.HasPrefix(resp.Arg.Channel, "candle") || len(resp.Data) == 0 {
		return
	}
	timeframe := strings.TrimPrefix(resp.Arg.Channel, "candle")
	for _, row := range resp.Data {
		candle, ok := parseOKXCandle("okx", resp.Arg.InstID, timeframe, row)
		if ok {
			m.candleListener.OnCandle(&candle)
		}
	}
}

func parseOKXCandle(exchange, symbol, timeframe string, row []string) (models.Candle, bool) {
	if len(row) < 6 {
		return models.Candle{}, false
	}
	candle := models.Candle{
		Exchange:  exchange,
		Symbol:    symbol,
		Timeframe: timeframe,
	}
	fmt.Sscanf(row[0], "%d", &candle.Timestamp)
	candle.Open, _ = decimal.NewFromString(row[1])
	candle.High, _ = decimal.NewFromString(row[2])
	candle.Low, _ = decimal.NewFromString(row[3])
	candle.Close, _ = decimal.NewFromString(row[4])
	candle.Volume, _ = decimal.NewFromString(row[5])
	if len(row) > 6 {
		candle.VolCcy, _ = decimal.NewFromString(row[6])
	}
	if len(row) > 7 {
		candle.VolCcyQuote, _ = decimal.NewFromString(row[7])
	}
	if len(row) > 8 {
		fmt.Sscanf(row[8], "%d", &candle.Confirm)
	}
	return candle, true
}
