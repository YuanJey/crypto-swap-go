package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/YuanJey/crypto-swap-go/internal/events"
	"github.com/YuanJey/crypto-swap-go/internal/transport"
	"github.com/YuanJey/crypto-swap-go/pkg/models"
	"github.com/YuanJey/crypto-swap-go/pkg/modules"
	"github.com/YuanJey/crypto-swap-go/pkg/secure"
	"github.com/shopspring/decimal"
)

type TradingModule struct {
	apiKey           string
	apiSecret        string
	testnet          bool
	baseURL          string
	router           *events.Router
	accountListener  modules.AccountListener
	positionListener modules.PositionListener
	balanceListener  modules.BalanceListener
	wsClient         *transport.WSClient
	client           *http.Client
	keepAliveCancel  context.CancelFunc
	marginMode       models.MarginMode
}

func NewTradingModule(apiKey, apiSecret string, testnet bool) *TradingModule {
	baseURL := "https://fapi.binance.com"
	if testnet {
		baseURL = "https://testnet.binancefuture.com"
	}
	return &TradingModule{
		apiKey:     apiKey,
		apiSecret:  apiSecret,
		testnet:    testnet,
		baseURL:    baseURL,
		router:     events.NewRouter(),
		client:     &http.Client{Timeout: 5 * time.Second},
		marginMode: models.MarginModeCross,
	}
}

func (t *TradingModule) AttachTradingListener(listener modules.TradingListener) func() {
	return t.router.AttachOrder(listener)
}

func (t *TradingModule) AttachAlgoOrderListener(listener modules.AlgoOrderListener) func() {
	return t.router.AttachAlgoOrder(listener)
}

func (t *TradingModule) AttachAccountListener(listener modules.AccountListener) {
	t.accountListener = listener
}

func (t *TradingModule) AttachPositionListener(listener modules.PositionListener) func() {
	t.positionListener = listener
	return func() {
		t.positionListener = nil
	}
}

func (t *TradingModule) AttachBalanceListener(listener modules.BalanceListener) func() {
	t.balanceListener = listener
	return func() {
		t.balanceListener = nil
	}
}

func (t *TradingModule) OnceOrder(clientOrderID string, event models.OrderEvent, listener modules.TradingListener) func() {
	return t.router.OnceOrder(clientOrderID, event, listener)
}

func (t *TradingModule) OnceAlgoOrder(clientOrderID string, event models.OrderEvent, listener modules.AlgoOrderListener) func() {
	return t.router.OnceAlgoOrder(clientOrderID, event, listener)
}

func (t *TradingModule) WaitOrder(ctx context.Context, clientOrderID string, event models.OrderEvent) (*models.OrderUpdate, error) {
	return t.router.WaitOrder(ctx, clientOrderID, event)
}

func (t *TradingModule) WaitAlgoOrder(ctx context.Context, clientOrderID string, event models.OrderEvent) (*models.AlgoOrderUpdate, error) {
	return t.router.WaitAlgoOrder(ctx, clientOrderID, event)
}

func (t *TradingModule) getListenKey(ctx context.Context) (string, error) {
	req, err := http.NewRequestWithContext(ctx, "POST", t.baseURL+"/fapi/v1/listenKey", nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("X-MBX-APIKEY", t.apiKey)

	resp, err := t.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}

	var data struct {
		ListenKey string `json:"listenKey"`
	}
	if err := json.Unmarshal(body, &data); err != nil {
		return "", err
	}
	if data.ListenKey == "" {
		return "", fmt.Errorf("failed to get listenKey: %s", string(body))
	}
	return data.ListenKey, nil
}

func (t *TradingModule) Start(ctx context.Context) error {
	listenKey, err := t.getListenKey(ctx)
	if err != nil {
		return err
	}

	wsURL := "wss://fstream.binance.com/ws/" + listenKey
	if t.testnet {
		wsURL = "wss://stream.binancefuture.com/ws/" + listenKey
	}

	t.wsClient = transport.NewWSClient(wsURL, t.handleMessage)
	if err := t.wsClient.Start(); err != nil {
		return err
	}

	t.startListenKeyKeepAlive(ctx, listenKey)
	return nil
}

func (t *TradingModule) Stop() {
	if t.keepAliveCancel != nil {
		t.keepAliveCancel()
		t.keepAliveCancel = nil
	}
	if t.wsClient != nil {
		t.wsClient.Stop()
	}
}

func (t *TradingModule) startListenKeyKeepAlive(parent context.Context, listenKey string) {
	ctx, cancel := context.WithCancel(parent)
	t.keepAliveCancel = cancel

	go func() {
		ticker := time.NewTicker(25 * time.Minute)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				req, err := http.NewRequestWithContext(ctx, "PUT", t.baseURL+"/fapi/v1/listenKey", nil)
				if err != nil {
					continue
				}
				req.Header.Set("X-MBX-APIKEY", t.apiKey)
				q := req.URL.Query()
				q.Set("listenKey", listenKey)
				req.URL.RawQuery = q.Encode()

				resp, err := t.client.Do(req)
				if err != nil {
					continue
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
			}
		}
	}()
}

func (t *TradingModule) handleMessage(msg []byte) {
	var event struct {
		StreamEvent string `json:"e"`
		EventTime   int64  `json:"E"`
		Order       struct {
			Symbol        string `json:"s"`
			ClientOrderID string `json:"c"`
			ExchangeID    int64  `json:"i"`
			Side          string `json:"S"`
			Status        string `json:"X"`
			Price         string `json:"p"`
			Quantity      string `json:"q"`
			FilledQty     string `json:"z"`
			AvgPrice      string `json:"ap"`
			UpdateTime    int64  `json:"T"`
		} `json:"o"`
		Account struct {
			EventReason string `json:"m"`
			Balances    []struct {
				Asset              string `json:"a"`
				WalletBalance      string `json:"wb"`
				CrossWalletBalance string `json:"cw"`
				BalanceChange      string `json:"bc"`
			} `json:"B"`
			Positions []struct {
				Symbol              string `json:"s"`
				PositionAmt         string `json:"pa"`
				EntryPrice          string `json:"ep"`
				BreakEvenPrice      string `json:"bep"`
				AccumulatedRealized string `json:"cr"`
				UnrealizedPNL       string `json:"up"`
				MarginType          string `json:"mt"`
				IsolatedWallet      string `json:"iw"`
				PositionSide        string `json:"ps"`
			} `json:"P"`
		} `json:"a"`
	}
	if err := json.Unmarshal(msg, &event); err != nil {
		return
	}

	if event.StreamEvent == "ACCOUNT_UPDATE" {
		t.handleAccountUpdate(event.EventTime, event.Account.Balances, event.Account.Positions)
		return
	}

	if event.StreamEvent != "ORDER_TRADE_UPDATE" {
		return
	}

	price, _ := decimal.NewFromString(event.Order.Price)
	qty, _ := decimal.NewFromString(event.Order.Quantity)
	filledQty, _ := decimal.NewFromString(event.Order.FilledQty)
	avgPrice, _ := decimal.NewFromString(event.Order.AvgPrice)

	t.router.DispatchOrder(&models.OrderUpdate{
		Exchange:      "binance",
		Symbol:        event.Order.Symbol,
		ClientOrderID: event.Order.ClientOrderID,
		ExchangeID:    strconv.FormatInt(event.Order.ExchangeID, 10),
		Side:          models.OrderSide(event.Order.Side),
		Status:        mapBinanceOrderStatus(event.Order.Status),
		Price:         price,
		Quantity:      qty,
		FilledQty:     filledQty,
		AvgPrice:      avgPrice,
		UpdateTime:    event.Order.UpdateTime,
	})
}

func (t *TradingModule) handleAccountUpdate(eventTime int64, balances []struct {
	Asset              string `json:"a"`
	WalletBalance      string `json:"wb"`
	CrossWalletBalance string `json:"cw"`
	BalanceChange      string `json:"bc"`
}, positions []struct {
	Symbol              string `json:"s"`
	PositionAmt         string `json:"pa"`
	EntryPrice          string `json:"ep"`
	BreakEvenPrice      string `json:"bep"`
	AccumulatedRealized string `json:"cr"`
	UnrealizedPNL       string `json:"up"`
	MarginType          string `json:"mt"`
	IsolatedWallet      string `json:"iw"`
	PositionSide        string `json:"ps"`
}) {
	if t.accountListener == nil && t.balanceListener == nil && t.positionListener == nil {
		return
	}
	for _, item := range balances {
		balance, _ := decimal.NewFromString(item.WalletBalance)
		available, _ := decimal.NewFromString(item.CrossWalletBalance)
		t.dispatchBalanceUpdate(&models.AccountBalance{
			Exchange:   "binance",
			Asset:      item.Asset,
			Balance:    balance,
			Available:  available,
			UpdateTime: eventTime,
		})
	}
	for _, item := range positions {
		amt, _ := decimal.NewFromString(item.PositionAmt)
		entry, _ := decimal.NewFromString(item.EntryPrice)
		pnl, _ := decimal.NewFromString(item.UnrealizedPNL)
		t.dispatchPositionUpdate(&models.Position{
			Exchange:      "binance",
			Symbol:        item.Symbol,
			PositionSide:  item.PositionSide,
			PositionAmt:   amt,
			EntryPrice:    entry,
			UnRealizedPnL: pnl,
			UpdateTime:    eventTime,
		})
	}
}

func (t *TradingModule) dispatchBalanceUpdate(balance *models.AccountBalance) {
	if t.accountListener != nil {
		t.accountListener.OnBalanceUpdate(balance)
	}
	if t.balanceListener != nil {
		t.balanceListener.OnBalanceUpdate(balance)
	}
}

func (t *TradingModule) dispatchPositionUpdate(position *models.Position) {
	if t.accountListener != nil {
		t.accountListener.OnPositionUpdate(position)
	}
	if t.positionListener != nil {
		t.positionListener.OnPositionUpdate(position)
	}
}

func mapBinanceOrderStatus(status string) models.OrderStatus {
	switch status {
	case "NEW":
		return models.OrderStatusNew
	case "PARTIALLY_FILLED":
		return models.OrderStatusPartiallyFilled
	case "FILLED":
		return models.OrderStatusFilled
	case "CANCELED", "EXPIRED":
		return models.OrderStatusCanceled
	case "REJECTED":
		return models.OrderStatusRejected
	default:
		return models.OrderStatus(status)
	}
}

func (t *TradingModule) SetPositionMode(ctx context.Context, mode models.PositionMode) error {
	params := url.Values{}
	params.Set("dualSidePosition", strconv.FormatBool(mode == models.PositionModeHedge))

	_, err := t.sendRequest(ctx, "POST", "/fapi/v1/positionSide/dual", params)
	return err
}

func (t *TradingModule) GetPositionMode(ctx context.Context) (models.PositionMode, error) {
	body, err := t.sendRequest(ctx, "GET", "/fapi/v1/positionSide/dual", url.Values{})
	if err != nil {
		return "", err
	}

	var resp struct {
		DualSidePosition bool `json:"dualSidePosition"`
	}
	if err := json.Unmarshal(body, &resp); err != nil {
		return "", err
	}
	if resp.DualSidePosition {
		return models.PositionModeHedge, nil
	}
	return models.PositionModeOneWay, nil
}

func (t *TradingModule) ConfigureTradingAccount(ctx context.Context, cfg models.TradingAccountConfig) error {
	positionMode := cfg.PositionMode
	if positionMode == "" {
		positionMode = models.PositionModeHedge
	}
	marginMode := cfg.MarginMode
	if marginMode == "" {
		marginMode = models.MarginModeCross
	}
	leverage := cfg.Leverage
	if leverage == 0 {
		leverage = 3
	}
	if leverage < 0 {
		return fmt.Errorf("leverage must be positive")
	}

	currentMode, err := t.GetPositionMode(ctx)
	if err != nil {
		return err
	}
	if currentMode != positionMode {
		if err := t.SetPositionMode(ctx, positionMode); err != nil {
			return err
		}
	}
	t.marginMode = marginMode

	for _, symbol := range cfg.Symbols {
		if err := t.SetMarginMode(ctx, symbol, marginMode); err != nil {
			return err
		}
		if leverage > 0 {
			if err := t.SetLeverage(ctx, symbol, leverage, models.PositionSideBoth, marginMode); err != nil {
				return err
			}
		}
	}
	return nil
}

func (t *TradingModule) SetMarginMode(ctx context.Context, symbol string, mode models.MarginMode) error {
	if mode == "" {
		mode = models.MarginModeCross
	}
	if mode != models.MarginModeCross && mode != models.MarginModeIsolated {
		return fmt.Errorf("unsupported margin mode: %s", mode)
	}

	params := url.Values{}
	params.Set("symbol", strings.ReplaceAll(symbol, "-", ""))
	params.Set("marginType", binanceMarginType(mode))

	_, err := t.sendRequest(ctx, "POST", "/fapi/v1/marginType", params)
	if err != nil {
		return err
	}
	t.marginMode = mode
	return nil
}

func (t *TradingModule) SetLeverage(ctx context.Context, symbol string, leverage int, positionSide models.PositionSide, marginMode models.MarginMode) error {
	if leverage <= 0 {
		return fmt.Errorf("leverage must be positive")
	}

	params := url.Values{}
	params.Set("symbol", strings.ReplaceAll(symbol, "-", ""))
	params.Set("leverage", strconv.Itoa(leverage))

	_, err := t.sendRequest(ctx, "POST", "/fapi/v1/leverage", params)
	return err
}

func binanceMarginType(mode models.MarginMode) string {
	if mode == models.MarginModeIsolated {
		return "ISOLATED"
	}
	return "CROSSED"
}

func (t *TradingModule) sendRequest(ctx context.Context, method, endpoint string, params url.Values) ([]byte, error) {
	if params == nil {
		params = url.Values{}
	}
	timestamp := time.Now().UnixMilli()
	params.Set("timestamp", strconv.FormatInt(timestamp, 10))
	params.Set("recvWindow", "2000")
	payload := params.Encode()
	signature := secure.HmacSha256Hex(t.apiSecret, payload)

	fullURL := fmt.Sprintf("%s%s?%s&signature=%s", t.baseURL, endpoint, payload, signature)

	req, err := http.NewRequestWithContext(ctx, method, fullURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-MBX-APIKEY", t.apiKey)

	resp, err := t.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		if endpoint == "/fapi/v1/positionSide/dual" && strings.Contains(string(body), `"code":-4059`) {
			return body, nil
		}
		if endpoint == "/fapi/v1/marginType" && strings.Contains(string(body), `"code":-4046`) {
			return body, nil
		}
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(body))
	}
	return body, nil
}

func (t *TradingModule) PlaceOrder(ctx context.Context, req models.PlaceOrderReq) (string, error) {
	return t.placeOrder(ctx, req, "LIMIT")
}

func (t *TradingModule) PlaceMarketOrder(ctx context.Context, req models.PlaceOrderReq) (string, error) {
	return t.placeOrder(ctx, req, "MARKET")
}

func (t *TradingModule) placeOrder(ctx context.Context, req models.PlaceOrderReq, orderType string) (string, error) {
	side := "BUY"
	if req.Side == models.OrderSideSell {
		side = "SELL"
	}
	timeInForce := req.TimeInForce
	if timeInForce == "" {
		timeInForce = "GTC"
	}

	params := url.Values{}
	params.Set("symbol", strings.ReplaceAll(req.Symbol, "-", ""))
	params.Set("side", side)
	params.Set("type", orderType)
	params.Set("quantity", req.Quantity.String())
	params.Set("newClientOrderId", req.ClientOrderID)
	params.Set("positionSide", string(binanceDefaultPositionSide(req)))
	if req.ReduceOnly && binanceDefaultPositionSide(req) == models.PositionSideBoth {
		params.Set("reduceOnly", "true")
	}
	if orderType == "LIMIT" {
		params.Set("timeInForce", timeInForce)
		params.Set("price", req.Price.String())
	}

	body, err := t.sendRequest(ctx, "POST", "/fapi/v1/order", params)
	if err != nil {
		return "", err
	}

	var data struct {
		OrderId int64 `json:"orderId"`
	}
	if err := json.Unmarshal(body, &data); err != nil {
		return "", err
	}

	return strconv.FormatInt(data.OrderId, 10), nil
}

func (t *TradingModule) GetOpenOrders(ctx context.Context, symbol string) ([]models.OpenOrder, error) {
	params := url.Values{}
	if symbol != "" {
		params.Set("symbol", strings.ReplaceAll(symbol, "-", ""))
	}
	body, err := t.sendRequest(ctx, "GET", "/fapi/v1/openOrders", params)
	if err != nil {
		return nil, err
	}

	var rows []struct {
		Symbol        string `json:"symbol"`
		OrderID       int64  `json:"orderId"`
		ClientOrderID string `json:"clientOrderId"`
		Side          string `json:"side"`
		PositionSide  string `json:"positionSide"`
		Status        string `json:"status"`
		Price         string `json:"price"`
		OrigQty       string `json:"origQty"`
		ExecutedQty   string `json:"executedQty"`
		AvgPrice      string `json:"avgPrice"`
		UpdateTime    int64  `json:"updateTime"`
	}
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, err
	}

	orders := make([]models.OpenOrder, 0, len(rows))
	for _, row := range rows {
		price, _ := decimal.NewFromString(row.Price)
		qty, _ := decimal.NewFromString(row.OrigQty)
		filled, _ := decimal.NewFromString(row.ExecutedQty)
		avgPrice, _ := decimal.NewFromString(row.AvgPrice)
		orders = append(orders, models.OpenOrder{
			Exchange:      "binance",
			Symbol:        row.Symbol,
			ClientOrderID: row.ClientOrderID,
			ExchangeID:    strconv.FormatInt(row.OrderID, 10),
			Side:          models.OrderSide(row.Side),
			PositionSide:  models.PositionSide(row.PositionSide),
			Status:        mapBinanceOrderStatus(row.Status),
			Price:         price,
			Quantity:      qty,
			FilledQty:     filled,
			AvgPrice:      avgPrice,
			UpdateTime:    row.UpdateTime,
		})
	}
	return orders, nil
}

func (t *TradingModule) AmendOrder(ctx context.Context, req models.AmendOrderReq) (string, error) {
	if req.Symbol == "" {
		return "", fmt.Errorf("symbol is required")
	}
	params := url.Values{}
	params.Set("symbol", strings.ReplaceAll(req.Symbol, "-", ""))
	if req.ExchangeID != "" {
		params.Set("orderId", req.ExchangeID)
	} else if req.ClientOrderID != "" {
		params.Set("origClientOrderId", req.ClientOrderID)
	} else {
		return "", fmt.Errorf("exchange ID or client order ID is required")
	}
	if req.NewClientOrderID != "" {
		params.Set("newClientOrderId", req.NewClientOrderID)
	}
	side := req.Side
	if side == "" {
		openOrders, err := t.GetOpenOrders(ctx, req.Symbol)
		if err != nil {
			return "", err
		}
		for _, order := range openOrders {
			if (req.ClientOrderID != "" && order.ClientOrderID == req.ClientOrderID) || (req.ExchangeID != "" && order.ExchangeID == req.ExchangeID) {
				side = order.Side
				break
			}
		}
	}
	if side == "" {
		return "", fmt.Errorf("side is required")
	}
	params.Set("side", string(side))
	if !req.Price.IsZero() {
		params.Set("price", req.Price.String())
	}
	if !req.Quantity.IsZero() {
		params.Set("quantity", req.Quantity.String())
	}

	body, err := t.sendRequest(ctx, "PUT", "/fapi/v1/order", params)
	if err != nil {
		return "", err
	}
	var data struct {
		OrderID int64 `json:"orderId"`
	}
	if err := json.Unmarshal(body, &data); err != nil {
		return "", err
	}
	if data.OrderID == 0 {
		return "", fmt.Errorf("binance amend order returned no orderId")
	}
	return strconv.FormatInt(data.OrderID, 10), nil
}

func (t *TradingModule) PlaceTPSL(ctx context.Context, req models.TPSLReq) (models.TPSLOrder, error) {
	var order models.TPSLOrder
	closeSide := "SELL"
	if req.Side == models.OrderSideSell {
		closeSide = "BUY"
	}
	positionSide := req.PositionSide
	if positionSide == "" {
		positionSide = models.PositionSideLong
		if req.Side == models.OrderSideSell {
			positionSide = models.PositionSideShort
		}
	}

	if !req.TakeProfit.IsZero() {
		clientOrderID := req.ClientOrderID + "tp"
		id, err := t.placeConditionalCloseOrder(ctx, req.Symbol, clientOrderID, closeSide, positionSide, "TAKE_PROFIT_MARKET", req.TakeProfit)
		if err != nil {
			return order, err
		}
		order.TakeProfitClientOrderID = clientOrderID
		order.TakeProfitOrderID = id
		t.dispatchAlgoOrderUpdate(req, clientOrderID, id, models.OrderSide(closeSide), positionSide, req.TakeProfit, models.OrderStatusNew)
	}
	if !req.StopLoss.IsZero() {
		clientOrderID := req.ClientOrderID + "sl"
		id, err := t.placeConditionalCloseOrder(ctx, req.Symbol, clientOrderID, closeSide, positionSide, "STOP_MARKET", req.StopLoss)
		if err != nil {
			if order.TakeProfitOrderID != "" {
				t.CancelTPSL(ctx, req.Symbol, models.TPSLOrder{
					TakeProfitClientOrderID: order.TakeProfitClientOrderID,
					TakeProfitOrderID:       order.TakeProfitOrderID,
				})
			}
			return order, err
		}
		order.StopLossClientOrderID = clientOrderID
		order.StopLossOrderID = id
		t.dispatchAlgoOrderUpdate(req, clientOrderID, id, models.OrderSide(closeSide), positionSide, req.StopLoss, models.OrderStatusNew)
	}
	return order, nil
}

func (t *TradingModule) PlaceTrailingOrder(ctx context.Context, req models.TrailingOrderReq) (models.TrailingOrder, error) {
	callbackRate, err := binanceTrailingCallbackRate(req)
	if err != nil {
		return models.TrailingOrder{}, err
	}
	side := "BUY"
	if req.Side == models.OrderSideSell {
		side = "SELL"
	}
	positionSide := req.PositionSide
	if positionSide == "" {
		positionSide = models.PositionSideLong
		if req.Side == models.OrderSideSell {
			positionSide = models.PositionSideShort
		}
	}

	params := url.Values{}
	params.Set("symbol", strings.ReplaceAll(req.Symbol, "-", ""))
	params.Set("side", side)
	params.Set("algoType", "CONDITIONAL")
	params.Set("type", "TRAILING_STOP_MARKET")
	params.Set("quantity", req.Quantity.String())
	params.Set("clientAlgoId", req.ClientOrderID)
	params.Set("positionSide", string(positionSide))
	params.Set("callbackRate", callbackRate.String())
	if !req.ActivationPrice.IsZero() {
		params.Set("activationPrice", req.ActivationPrice.String())
	}
	if req.ReduceOnly && positionSide == models.PositionSideBoth {
		params.Set("reduceOnly", "true")
	}

	body, err := t.sendRequest(ctx, "POST", "/fapi/v1/algoOrder", params)
	if err != nil {
		return models.TrailingOrder{}, err
	}
	var data struct {
		AlgoID int64 `json:"algoId"`
	}
	if err := json.Unmarshal(body, &data); err != nil {
		return models.TrailingOrder{}, err
	}
	orderID := strconv.FormatInt(data.AlgoID, 10)
	t.dispatchAlgoOrderUpdate(models.TPSLReq{
		Symbol:   req.Symbol,
		Quantity: req.Quantity,
	}, req.ClientOrderID, orderID, models.OrderSide(side), positionSide, req.ActivationPrice, models.OrderStatusNew)
	return models.TrailingOrder{
		ClientOrderID: req.ClientOrderID,
		OrderID:       orderID,
	}, nil
}

func binanceTrailingCallbackRate(req models.TrailingOrderReq) (decimal.Decimal, error) {
	if !req.CallbackSpread.IsZero() {
		if req.ActivationPrice.IsZero() {
			return decimal.Zero, fmt.Errorf("activation price is required when callback spread is set")
		}
		return req.CallbackSpread.Div(req.ActivationPrice).Mul(decimal.NewFromInt(100)).Round(1), nil
	}
	if !req.CallbackRatio.IsZero() {
		return req.CallbackRatio.Mul(decimal.NewFromInt(100)).Round(1), nil
	}
	return decimal.Zero, fmt.Errorf("callback spread or callback ratio is required")
}

func (t *TradingModule) placeConditionalCloseOrder(ctx context.Context, symbol, clientOrderID, side string, positionSide models.PositionSide, orderType string, stopPrice decimal.Decimal) (string, error) {
	params := url.Values{}
	params.Set("algoType", "CONDITIONAL")
	params.Set("symbol", strings.ReplaceAll(symbol, "-", ""))
	params.Set("side", side)
	params.Set("type", orderType)
	params.Set("positionSide", string(positionSide))
	params.Set("triggerPrice", stopPrice.String())
	params.Set("closePosition", "true")
	params.Set("workingType", "MARK_PRICE")
	params.Set("clientAlgoId", clientOrderID)

	body, err := t.sendRequest(ctx, "POST", "/fapi/v1/algoOrder", params)
	if err != nil {
		return "", err
	}
	var data struct {
		AlgoID int64 `json:"algoId"`
	}
	if err := json.Unmarshal(body, &data); err != nil {
		return "", err
	}
	return strconv.FormatInt(data.AlgoID, 10), nil
}

func (t *TradingModule) CancelTPSL(ctx context.Context, symbol string, order models.TPSLOrder) error {
	if order.TakeProfitOrderID != "" {
		if err := t.cancelAlgoOrderID(ctx, order.TakeProfitOrderID); err != nil {
			return err
		}
		t.dispatchAlgoOrderUpdate(models.TPSLReq{Symbol: symbol}, order.TakeProfitClientOrderID, order.TakeProfitOrderID, "", "", decimal.Zero, models.OrderStatusCanceled)
	}
	if order.StopLossOrderID != "" {
		if err := t.cancelAlgoOrderID(ctx, order.StopLossOrderID); err != nil {
			return err
		}
		t.dispatchAlgoOrderUpdate(models.TPSLReq{Symbol: symbol}, order.StopLossClientOrderID, order.StopLossOrderID, "", "", decimal.Zero, models.OrderStatusCanceled)
	}
	return nil
}

func (t *TradingModule) GetOpenAlgoOrders(ctx context.Context, symbol string) ([]models.OpenAlgoOrder, error) {
	params := url.Values{}
	if symbol != "" {
		params.Set("symbol", strings.ReplaceAll(symbol, "-", ""))
	}
	body, err := t.sendRequest(ctx, "GET", "/fapi/v1/openAlgoOrders", params)
	if err != nil {
		return nil, err
	}

	var rows []struct {
		Symbol       string `json:"symbol"`
		AlgoID       int64  `json:"algoId"`
		ClientAlgoID string `json:"clientAlgoId"`
		Side         string `json:"side"`
		PositionSide string `json:"positionSide"`
		Status       string `json:"status"`
		Type         string `json:"type"`
		TriggerPrice string `json:"triggerPrice"`
		Price        string `json:"price"`
		OrigQty      string `json:"origQty"`
		UpdateTime   int64  `json:"updateTime"`
	}
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, err
	}

	orders := make([]models.OpenAlgoOrder, 0, len(rows))
	for _, row := range rows {
		trigger, _ := decimal.NewFromString(row.TriggerPrice)
		price, _ := decimal.NewFromString(row.Price)
		qty, _ := decimal.NewFromString(row.OrigQty)
		orders = append(orders, models.OpenAlgoOrder{
			Exchange:      "binance",
			Symbol:        row.Symbol,
			ClientOrderID: row.ClientAlgoID,
			ExchangeID:    strconv.FormatInt(row.AlgoID, 10),
			Side:          models.OrderSide(row.Side),
			PositionSide:  models.PositionSide(row.PositionSide),
			Status:        mapBinanceOrderStatus(row.Status),
			TriggerPrice:  trigger,
			OrderPrice:    price,
			Quantity:      qty,
			UpdateTime:    row.UpdateTime,
		})
	}
	return orders, nil
}

func (t *TradingModule) dispatchAlgoOrderUpdate(req models.TPSLReq, clientOrderID, exchangeID string, side models.OrderSide, positionSide models.PositionSide, trigger decimal.Decimal, status models.OrderStatus) {
	if clientOrderID == "" {
		return
	}
	t.router.DispatchAlgoOrder(&models.AlgoOrderUpdate{
		Exchange:      "binance",
		Symbol:        req.Symbol,
		ClientOrderID: clientOrderID,
		ExchangeID:    exchangeID,
		Side:          side,
		PositionSide:  positionSide,
		Status:        status,
		TriggerPrice:  trigger,
		Quantity:      req.Quantity,
		UpdateTime:    time.Now().UnixMilli(),
	})
}

func (t *TradingModule) UpdateTPSL(ctx context.Context, old models.TPSLOrder, req models.TPSLReq) (models.TPSLOrder, error) {
	if err := t.CancelTPSL(ctx, req.Symbol, old); err != nil {
		return models.TPSLOrder{}, err
	}
	return t.PlaceTPSL(ctx, req)
}

func (t *TradingModule) GetPosition(ctx context.Context, symbol string, side models.PositionSide) (*models.Position, error) {
	params := url.Values{}
	params.Set("symbol", strings.ReplaceAll(symbol, "-", ""))
	body, err := t.sendRequest(ctx, "GET", "/fapi/v3/positionRisk", params)
	if err != nil {
		return nil, err
	}

	var rows []struct {
		Symbol           string `json:"symbol"`
		PositionAmt      string `json:"positionAmt"`
		EntryPrice       string `json:"entryPrice"`
		MarkPrice        string `json:"markPrice"`
		UnrealizedPNL    string `json:"unRealizedProfit"`
		LiquidationPrice string `json:"liquidationPrice"`
		Leverage         string `json:"leverage"`
		PositionSide     string `json:"positionSide"`
		UpdateTime       int64  `json:"updateTime"`
	}
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, err
	}
	wantSide := string(side)
	if side == "" {
		wantSide = string(models.PositionSideLong)
	}
	for _, row := range rows {
		if row.PositionSide != wantSide {
			continue
		}
		amt, _ := decimal.NewFromString(row.PositionAmt)
		entry, _ := decimal.NewFromString(row.EntryPrice)
		mark, _ := decimal.NewFromString(row.MarkPrice)
		pnl, _ := decimal.NewFromString(row.UnrealizedPNL)
		liq, _ := decimal.NewFromString(row.LiquidationPrice)
		leverage, _ := strconv.Atoi(row.Leverage)
		return &models.Position{
			Exchange:         "binance",
			Symbol:           row.Symbol,
			PositionSide:     row.PositionSide,
			PositionAmt:      amt,
			EntryPrice:       entry,
			MarkPrice:        mark,
			UnRealizedPnL:    pnl,
			LiquidationPrice: liq,
			Leverage:         leverage,
			UpdateTime:       row.UpdateTime,
		}, nil
	}
	return nil, fmt.Errorf("position not found for %s %s", symbol, wantSide)
}

func (t *TradingModule) ClosePosition(ctx context.Context, symbol string, side models.PositionSide) (string, error) {
	position, err := t.GetPosition(ctx, symbol, side)
	if err != nil {
		return "", err
	}
	qty := position.PositionAmt.Abs()
	if qty.IsZero() {
		return "", fmt.Errorf("no open position for %s %s", symbol, side)
	}
	orderSide := models.OrderSideSell
	if side == models.PositionSideShort {
		orderSide = models.OrderSideBuy
	}
	return t.PlaceMarketOrder(ctx, models.PlaceOrderReq{
		Symbol:        symbol,
		ClientOrderID: fmt.Sprintf("csclose%d", time.Now().UnixNano()),
		Side:          orderSide,
		PositionSide:  side,
		Quantity:      qty,
		ReduceOnly:    true,
	})
}

func (t *TradingModule) defaultMarginMode(req models.PlaceOrderReq) models.MarginMode {
	if req.MarginMode != "" {
		return req.MarginMode
	}
	if t.marginMode != "" {
		return t.marginMode
	}
	return models.MarginModeCross
}

func binanceDefaultPositionSide(req models.PlaceOrderReq) models.PositionSide {
	if req.PositionSide != "" {
		return req.PositionSide
	}
	if req.Side == models.OrderSideSell {
		return models.PositionSideShort
	}
	return models.PositionSideLong
}

func (t *TradingModule) CancelOrder(ctx context.Context, symbol, clientOrderID string) error {
	params := url.Values{}
	params.Set("symbol", strings.ReplaceAll(symbol, "-", ""))
	params.Set("origClientOrderId", clientOrderID)

	_, err := t.sendRequest(ctx, "DELETE", "/fapi/v1/order", params)
	return err
}

func (t *TradingModule) cancelOrderID(ctx context.Context, symbol, orderID string) error {
	params := url.Values{}
	params.Set("symbol", strings.ReplaceAll(symbol, "-", ""))
	params.Set("orderId", orderID)

	_, err := t.sendRequest(ctx, "DELETE", "/fapi/v1/order", params)
	return err
}

func (t *TradingModule) cancelAlgoOrderID(ctx context.Context, algoID string) error {
	params := url.Values{}
	params.Set("algoId", algoID)

	_, err := t.sendRequest(ctx, "DELETE", "/fapi/v1/algoOrder", params)
	return err
}
