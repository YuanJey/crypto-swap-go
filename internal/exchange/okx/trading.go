package okx

import (
	"bytes"
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
	passphrase       string
	testnet          bool
	baseURL          string
	router           *events.Router
	accountListener  modules.AccountListener
	positionListener modules.PositionListener
	balanceListener  modules.BalanceListener
	wsClient         *transport.WSClient
	client           *http.Client
	marginMode       models.MarginMode
	loginAck         chan error
	ordersAck        chan error
	accountAck       chan error
	positionsAck     chan error
}

func NewTradingModule(apiKey, apiSecret, passphrase string, testnet bool) *TradingModule {
	baseURL := "https://www.okx.com"
	if testnet {
		baseURL = "https://www.okx.com"
	}
	return &TradingModule{
		apiKey:     apiKey,
		apiSecret:  apiSecret,
		passphrase: passphrase,
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

func (t *TradingModule) Start(ctx context.Context) error {
	wsURL := "wss://ws.okx.com:8443/ws/v5/private"
	if t.testnet {
		wsURL = "wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999"
	}

	t.wsClient = transport.NewWSClient(wsURL, t.handleMessage)
	if err := t.wsClient.Start(); err != nil {
		return err
	}
	t.loginAck = make(chan error, 1)
	t.ordersAck = make(chan error, 1)
	t.accountAck = make(chan error, 1)
	t.positionsAck = make(chan error, 1)

	timestamp := fmt.Sprintf("%d", time.Now().Unix())
	sign := secure.HmacSha256Base64(t.apiSecret, timestamp+"GET/users/self/verify")

	loginReq := map[string]interface{}{
		"op": "login",
		"args": []map[string]string{
			{
				"apiKey":     t.apiKey,
				"passphrase": t.passphrase,
				"timestamp":  timestamp,
				"sign":       sign,
			},
		},
	}
	payload, _ := json.Marshal(loginReq)
	if err := t.wsClient.Send(payload); err != nil {
		return err
	}
	if err := waitOKXAck(ctx, t.loginAck, "login"); err != nil {
		t.Stop()
		return err
	}

	subReq := map[string]interface{}{
		"op": "subscribe",
		"args": []map[string]string{
			{
				"channel":  "orders",
				"instType": "SWAP",
			},
			{
				"channel": "account",
			},
			{
				"channel":  "positions",
				"instType": "ANY",
			},
		},
	}
	subPayload, _ := json.Marshal(subReq)
	if err := t.wsClient.Send(subPayload); err != nil {
		return err
	}
	if err := waitOKXAck(ctx, t.ordersAck, "orders subscription"); err != nil {
		t.Stop()
		return err
	}
	if err := waitOKXAck(ctx, t.accountAck, "account subscription"); err != nil {
		t.Stop()
		return err
	}
	if err := waitOKXAck(ctx, t.positionsAck, "positions subscription"); err != nil {
		t.Stop()
		return err
	}

	return nil
}

func (t *TradingModule) Stop() {
	if t.wsClient != nil {
		t.wsClient.Stop()
	}
}

func (t *TradingModule) handleMessage(msg []byte) {
	if t.handleControlMessage(msg) {
		return
	}

	var event struct {
		Arg struct {
			Channel string `json:"channel"`
		} `json:"arg"`
		Data []struct {
			Symbol        string `json:"instId"`
			ClientOrderID string `json:"clOrdId"`
			ExchangeID    string `json:"ordId"`
			Side          string `json:"side"`
			State         string `json:"state"`
			Price         string `json:"px"`
			Quantity      string `json:"sz"`
			FilledQty     string `json:"fillSz"`
			AvgPrice      string `json:"avgPx"`
			UpdateTime    string `json:"uTime"`
		} `json:"data"`
	}
	if err := json.Unmarshal(msg, &event); err != nil {
		return
	}

	if event.Arg.Channel != "orders" {
		switch event.Arg.Channel {
		case "account":
			t.handleBalanceMessage(msg)
		case "positions":
			t.handlePositionMessage(msg)
		}
		return
	}

	for _, item := range event.Data {
		price, _ := decimal.NewFromString(item.Price)
		qty, _ := decimal.NewFromString(item.Quantity)
		filledQty, _ := decimal.NewFromString(item.FilledQty)
		avgPrice, _ := decimal.NewFromString(item.AvgPrice)

		var updateTime int64
		fmt.Sscanf(item.UpdateTime, "%d", &updateTime)

		sideMapped := models.OrderSideBuy
		if item.Side == "sell" {
			sideMapped = models.OrderSideSell
		}

		t.router.DispatchOrder(&models.OrderUpdate{
			Exchange:      "okx",
			Symbol:        item.Symbol,
			ClientOrderID: item.ClientOrderID,
			ExchangeID:    item.ExchangeID,
			Side:          sideMapped,
			Status:        mapOKXOrderStatus(item.State),
			Price:         price,
			Quantity:      qty,
			FilledQty:     filledQty,
			AvgPrice:      avgPrice,
			UpdateTime:    updateTime,
		})
	}
}

func (t *TradingModule) handleBalanceMessage(msg []byte) {
	if t.accountListener == nil && t.balanceListener == nil {
		return
	}
	var event struct {
		Arg struct {
			Channel string `json:"channel"`
		} `json:"arg"`
		Data []struct {
			UTime   string `json:"uTime"`
			Details []struct {
				CCY      string `json:"ccy"`
				Eq       string `json:"eq"`
				CashBal  string `json:"cashBal"`
				AvailBal string `json:"availBal"`
				AvailEq  string `json:"availEq"`
				UTime    string `json:"uTime"`
			} `json:"details"`
		} `json:"data"`
	}
	if err := json.Unmarshal(msg, &event); err != nil || event.Arg.Channel != "account" {
		return
	}

	for _, data := range event.Data {
		for _, item := range data.Details {
			balanceStr := firstNonEmpty(item.Eq, item.CashBal)
			availableStr := firstNonEmpty(item.AvailEq, item.AvailBal)
			balance, _ := decimal.NewFromString(balanceStr)
			available, _ := decimal.NewFromString(availableStr)
			updateTime := parseInt64(item.UTime)
			if updateTime == 0 {
				updateTime = parseInt64(data.UTime)
			}
			t.dispatchBalanceUpdate(&models.AccountBalance{
				Exchange:   "okx",
				Asset:      item.CCY,
				Balance:    balance,
				Available:  available,
				UpdateTime: updateTime,
			})
		}
	}
}

func (t *TradingModule) handlePositionMessage(msg []byte) {
	if t.accountListener == nil && t.positionListener == nil {
		return
	}
	var event struct {
		Arg struct {
			Channel string `json:"channel"`
		} `json:"arg"`
		Data []struct {
			InstID           string `json:"instId"`
			PosSide          string `json:"posSide"`
			Pos              string `json:"pos"`
			AvgPx            string `json:"avgPx"`
			MarkPx           string `json:"markPx"`
			Upl              string `json:"upl"`
			Lever            string `json:"lever"`
			LiquidationPrice string `json:"liqPx"`
			UTime            string `json:"uTime"`
		} `json:"data"`
	}
	if err := json.Unmarshal(msg, &event); err != nil || event.Arg.Channel != "positions" {
		return
	}

	for _, item := range event.Data {
		amt, _ := decimal.NewFromString(item.Pos)
		entry, _ := decimal.NewFromString(item.AvgPx)
		mark, _ := decimal.NewFromString(item.MarkPx)
		pnl, _ := decimal.NewFromString(item.Upl)
		liq, _ := decimal.NewFromString(item.LiquidationPrice)
		leverage, _ := strconv.Atoi(item.Lever)
		updateTime := parseInt64(item.UTime)
		t.dispatchPositionUpdate(&models.Position{
			Exchange:         "okx",
			Symbol:           item.InstID,
			PositionSide:     strings.ToUpper(item.PosSide),
			PositionAmt:      amt,
			EntryPrice:       entry,
			MarkPrice:        mark,
			UnRealizedPnL:    pnl,
			LiquidationPrice: liq,
			Leverage:         leverage,
			UpdateTime:       updateTime,
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

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return "0"
}

func (t *TradingModule) handleControlMessage(msg []byte) bool {
	var event struct {
		Event string `json:"event"`
		Code  string `json:"code"`
		Msg   string `json:"msg"`
		Arg   struct {
			Channel string `json:"channel"`
		} `json:"arg"`
	}
	if err := json.Unmarshal(msg, &event); err != nil || event.Event == "" {
		return false
	}

	err := error(nil)
	if event.Code != "" && event.Code != "0" {
		err = fmt.Errorf("okx %s error: code=%s msg=%s", event.Event, event.Code, event.Msg)
	}
	switch event.Event {
	case "login":
		sendOKXAck(t.loginAck, err)
	case "subscribe":
		if event.Arg.Channel == "orders" {
			sendOKXAck(t.ordersAck, err)
		}
		if event.Arg.Channel == "account" {
			sendOKXAck(t.accountAck, err)
		}
		if event.Arg.Channel == "positions" {
			sendOKXAck(t.positionsAck, err)
		}
	}
	return true
}

func sendOKXAck(ch chan error, err error) {
	if ch == nil {
		return
	}
	select {
	case ch <- err:
	default:
	}
}

func waitOKXAck(ctx context.Context, ch <-chan error, name string) error {
	timer := time.NewTimer(10 * time.Second)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return fmt.Errorf("timed out waiting for okx %s ack", name)
	case err := <-ch:
		return err
	}
}

func parseInt64(value string) int64 {
	parsed, _ := strconv.ParseInt(value, 10, 64)
	return parsed
}

func mapOKXOrderStatus(state string) models.OrderStatus {
	switch state {
	case "live":
		return models.OrderStatusNew
	case "partially_filled":
		return models.OrderStatusPartiallyFilled
	case "filled":
		return models.OrderStatusFilled
	case "canceled":
		return models.OrderStatusCanceled
	default:
		return models.OrderStatus(strings.ToUpper(state))
	}
}

func okxOrderSide(side string) models.OrderSide {
	if side == "sell" {
		return models.OrderSideSell
	}
	return models.OrderSideBuy
}

func firstDecimal(values ...string) decimal.Decimal {
	for _, value := range values {
		if value == "" || value == "-1" {
			continue
		}
		parsed, _ := decimal.NewFromString(value)
		return parsed
	}
	return decimal.Zero
}

func (t *TradingModule) SetPositionMode(ctx context.Context, mode models.PositionMode) error {
	posMode := "net_mode"
	if mode == models.PositionModeHedge {
		posMode = "long_short_mode"
	}

	body, _ := json.Marshal(map[string]string{
		"posMode": posMode,
	})
	respBytes, err := t.sendRequest(ctx, "POST", "/api/v5/account/set-position-mode", body)
	if err != nil {
		return err
	}

	var resp struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		return err
	}
	if resp.Code != "0" {
		return fmt.Errorf("okx set position mode error: %s", string(respBytes))
	}
	return nil
}

func (t *TradingModule) GetPositionMode(ctx context.Context) (models.PositionMode, error) {
	respBytes, err := t.sendRequest(ctx, "GET", "/api/v5/account/config", nil)
	if err != nil {
		return "", err
	}

	var resp struct {
		Code string `json:"code"`
		Data []struct {
			PositionMode string `json:"posMode"`
		} `json:"data"`
	}
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		return "", err
	}
	if resp.Code != "0" {
		return "", fmt.Errorf("okx get position mode error: %s", string(respBytes))
	}
	if len(resp.Data) == 0 {
		return "", fmt.Errorf("okx get position mode returned no data")
	}
	if resp.Data[0].PositionMode == "long_short_mode" {
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
	t.marginMode = mode
	return nil
}

func (t *TradingModule) SetLeverage(ctx context.Context, symbol string, leverage int, positionSide models.PositionSide, marginMode models.MarginMode) error {
	if leverage <= 0 {
		return fmt.Errorf("leverage must be positive")
	}
	if marginMode == "" {
		marginMode = t.marginMode
	}
	if marginMode == "" {
		marginMode = models.MarginModeCross
	}

	sides := []models.PositionSide{positionSide}
	if positionSide == "" || positionSide == models.PositionSideBoth {
		sides = []models.PositionSide{models.PositionSideLong, models.PositionSideShort}
	}

	for _, side := range sides {
		body, _ := json.Marshal(map[string]string{
			"instId":  symbol,
			"lever":   strconv.Itoa(leverage),
			"mgnMode": okxMarginMode(marginMode),
			"posSide": strings.ToLower(string(side)),
		})
		respBytes, err := t.sendRequest(ctx, "POST", "/api/v5/account/set-leverage", body)
		if err != nil {
			return err
		}

		var resp struct {
			Code string `json:"code"`
		}
		if err := json.Unmarshal(respBytes, &resp); err != nil {
			return err
		}
		if resp.Code != "0" {
			return fmt.Errorf("okx set leverage error: %s", string(respBytes))
		}
	}
	return nil
}

func okxMarginMode(mode models.MarginMode) string {
	if mode == models.MarginModeIsolated {
		return "isolated"
	}
	return "cross"
}

func (t *TradingModule) sendRequest(ctx context.Context, method, endpoint string, body []byte) ([]byte, error) {
	timestamp := time.Now().UTC().Format("2006-01-02T15:04:05.000Z")
	payload := timestamp + method + endpoint + string(body)
	signature := secure.HmacSha256Base64(t.apiSecret, payload)

	req, err := http.NewRequestWithContext(ctx, method, t.baseURL+endpoint, bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("OK-ACCESS-KEY", t.apiKey)
	req.Header.Set("OK-ACCESS-SIGN", signature)
	req.Header.Set("OK-ACCESS-TIMESTAMP", timestamp)
	req.Header.Set("OK-ACCESS-PASSPHRASE", t.passphrase)
	req.Header.Set("Content-Type", "application/json")
	if t.testnet {
		req.Header.Set("x-simulated-trading", "1")
	}

	resp, err := t.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, string(respBody))
	}
	return respBody, nil
}

func (t *TradingModule) GetBalance(ctx context.Context, asset string) (*models.AccountBalance, error) {
	wantAsset := strings.ToUpper(asset)
	if wantAsset == "" {
		wantAsset = "USDT"
	}
	endpoint := "/api/v5/account/balance?ccy=" + url.QueryEscape(wantAsset)
	respBytes, err := t.sendRequest(ctx, "GET", endpoint, nil)
	if err != nil {
		return nil, err
	}

	var resp struct {
		Code string `json:"code"`
		Msg  string `json:"msg"`
		Data []struct {
			UTime   string `json:"uTime"`
			Details []struct {
				CCY      string `json:"ccy"`
				Eq       string `json:"eq"`
				CashBal  string `json:"cashBal"`
				AvailBal string `json:"availBal"`
				AvailEq  string `json:"availEq"`
				UTime    string `json:"uTime"`
			} `json:"details"`
		} `json:"data"`
	}
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		return nil, err
	}
	if resp.Code != "" && resp.Code != "0" {
		return nil, fmt.Errorf("okx get balance error: code=%s msg=%s", resp.Code, resp.Msg)
	}
	for _, data := range resp.Data {
		for _, item := range data.Details {
			if strings.ToUpper(item.CCY) != wantAsset {
				continue
			}
			balance, _ := decimal.NewFromString(firstNonEmpty(item.Eq, item.CashBal))
			available, _ := decimal.NewFromString(firstNonEmpty(item.AvailEq, item.AvailBal))
			updateTime := parseInt64(item.UTime)
			if updateTime == 0 {
				updateTime = parseInt64(data.UTime)
			}
			return &models.AccountBalance{
				Exchange:   "okx",
				Asset:      item.CCY,
				Balance:    balance,
				Available:  available,
				UpdateTime: updateTime,
			}, nil
		}
	}
	return nil, fmt.Errorf("balance not found for %s", wantAsset)
}

func (t *TradingModule) PlaceOrder(ctx context.Context, req models.PlaceOrderReq) (string, error) {
	return t.placeOrder(ctx, req, "limit")
}

func (t *TradingModule) PlaceMarketOrder(ctx context.Context, req models.PlaceOrderReq) (string, error) {
	return t.placeOrder(ctx, req, "market")
}

func (t *TradingModule) placeOrder(ctx context.Context, req models.PlaceOrderReq, ordType string) (string, error) {
	quantity, err := t.resolveOrderQuantity(ctx, req.Symbol, req.Quantity, req.BaseQuantity)
	if err != nil {
		return "", err
	}
	side := "buy"
	if req.Side == models.OrderSideSell {
		side = "sell"
	}
	if ordType == "limit" {
		switch strings.ToUpper(req.TimeInForce) {
		case "IOC":
			ordType = "ioc"
		case "FOK":
			ordType = "fok"
		}
	}

	bodyMap := map[string]interface{}{
		"instId":  req.Symbol,
		"tdMode":  okxMarginMode(t.defaultMarginMode(req)),
		"side":    side,
		"posSide": okxDefaultPositionSide(req),
		"ordType": ordType,
		"sz":      quantity.String(),
		"clOrdId": req.ClientOrderID,
	}
	if ordType != "market" {
		price, err := t.resolveOrderPrice(ctx, req.Symbol, req.Price)
		if err != nil {
			return "", err
		}
		bodyMap["px"] = price.String()
	}
	if req.ReduceOnly {
		bodyMap["reduceOnly"] = true
	}

	body, _ := json.Marshal(bodyMap)
	respBytes, err := t.sendRequest(ctx, "POST", "/api/v5/trade/order", body)
	if err != nil {
		return "", err
	}

	var resp struct {
		Code string `json:"code"`
		Data []struct {
			OrderID string `json:"ordId"`
		} `json:"data"`
	}
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		return "", err
	}

	if resp.Code != "0" {
		return "", fmt.Errorf("okx place order error: %s", string(respBytes))
	}

	if len(resp.Data) > 0 {
		return resp.Data[0].OrderID, nil
	}

	return "", fmt.Errorf("no ordId returned")
}

func (t *TradingModule) GetOpenOrders(ctx context.Context, symbol string) ([]models.OpenOrder, error) {
	endpoint := "/api/v5/trade/orders-pending?instType=SWAP"
	if symbol != "" {
		endpoint += "&instId=" + url.QueryEscape(symbol)
	}
	respBytes, err := t.sendRequest(ctx, "GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	var resp struct {
		Code string `json:"code"`
		Data []struct {
			InstID     string `json:"instId"`
			OrderID    string `json:"ordId"`
			ClientID   string `json:"clOrdId"`
			Side       string `json:"side"`
			PosSide    string `json:"posSide"`
			State      string `json:"state"`
			Price      string `json:"px"`
			Quantity   string `json:"sz"`
			FilledQty  string `json:"accFillSz"`
			AvgPrice   string `json:"avgPx"`
			UpdateTime string `json:"uTime"`
		} `json:"data"`
	}
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		return nil, err
	}
	if resp.Code != "0" {
		return nil, fmt.Errorf("okx get open orders error: %s", string(respBytes))
	}

	orders := make([]models.OpenOrder, 0, len(resp.Data))
	for _, row := range resp.Data {
		price, _ := decimal.NewFromString(row.Price)
		qty, _ := decimal.NewFromString(row.Quantity)
		filled, _ := decimal.NewFromString(row.FilledQty)
		avgPrice, _ := decimal.NewFromString(row.AvgPrice)
		orders = append(orders, models.OpenOrder{
			Exchange:      "okx",
			Symbol:        row.InstID,
			ClientOrderID: row.ClientID,
			ExchangeID:    row.OrderID,
			Side:          okxOrderSide(row.Side),
			PositionSide:  models.PositionSide(strings.ToUpper(row.PosSide)),
			Status:        mapOKXOrderStatus(row.State),
			Price:         price,
			Quantity:      qty,
			FilledQty:     filled,
			AvgPrice:      avgPrice,
			UpdateTime:    parseInt64(row.UpdateTime),
		})
	}
	return orders, nil
}

func (t *TradingModule) AmendOrder(ctx context.Context, req models.AmendOrderReq) (string, error) {
	if req.Symbol == "" {
		return "", fmt.Errorf("symbol is required")
	}
	bodyMap := map[string]interface{}{
		"instId": req.Symbol,
	}
	if req.ExchangeID != "" {
		bodyMap["ordId"] = req.ExchangeID
	} else if req.ClientOrderID != "" {
		bodyMap["clOrdId"] = req.ClientOrderID
	} else {
		return "", fmt.Errorf("exchange ID or client order ID is required")
	}
	if req.NewClientOrderID != "" {
		bodyMap["newClOrdId"] = req.NewClientOrderID
	}
	if !req.Price.IsZero() {
		price, err := t.resolveOrderPrice(ctx, req.Symbol, req.Price)
		if err != nil {
			return "", err
		}
		bodyMap["newPx"] = price.String()
	}
	quantity, err := t.resolveOrderQuantity(ctx, req.Symbol, req.Quantity, req.BaseQuantity)
	if err != nil {
		return "", err
	}
	if !quantity.IsZero() {
		bodyMap["newSz"] = quantity.String()
	}

	body, _ := json.Marshal(bodyMap)
	respBytes, err := t.sendRequest(ctx, "POST", "/api/v5/trade/amend-order", body)
	if err != nil {
		return "", err
	}
	var resp struct {
		Code string `json:"code"`
		Data []struct {
			OrderID string `json:"ordId"`
		} `json:"data"`
	}
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		return "", err
	}
	if resp.Code != "0" {
		return "", fmt.Errorf("okx amend order error: %s", string(respBytes))
	}
	if len(resp.Data) == 0 {
		return "", fmt.Errorf("okx amend order returned no ordId")
	}
	return resp.Data[0].OrderID, nil
}

func (t *TradingModule) PlaceTPSL(ctx context.Context, req models.TPSLReq) (models.TPSLOrder, error) {
	var order models.TPSLOrder
	quantity, err := t.resolveOrderQuantity(ctx, req.Symbol, req.Quantity, req.BaseQuantity)
	if err != nil {
		return order, err
	}
	req.Quantity = quantity
	closeSide := models.OrderSideSell
	if req.Side == models.OrderSideSell {
		closeSide = models.OrderSideBuy
	}
	positionSide := req.PositionSide
	if positionSide == "" {
		positionSide = models.PositionSideLong
		if req.Side == models.OrderSideSell {
			positionSide = models.PositionSideShort
		}
	}
	marginMode := req.MarginMode
	if marginMode == "" {
		marginMode = t.marginMode
	}
	if marginMode == "" {
		marginMode = models.MarginModeCross
	}

	if !req.TakeProfit.IsZero() {
		takeProfit, err := t.resolveOrderPrice(ctx, req.Symbol, req.TakeProfit)
		if err != nil {
			return order, err
		}
		req.TakeProfit = takeProfit
		clientOrderID := req.ClientOrderID + "tp"
		id, err := t.placeAlgoCloseOrder(ctx, req.Symbol, clientOrderID, closeSide, positionSide, marginMode, req.Quantity, "conditional", req.TakeProfit, true)
		if err != nil {
			return order, err
		}
		order.TakeProfitClientOrderID = clientOrderID
		order.TakeProfitOrderID = id
		t.dispatchAlgoOrderUpdate(req, clientOrderID, id, closeSide, positionSide, req.TakeProfit, models.OrderStatusNew)
	}
	if !req.StopLoss.IsZero() {
		stopLoss, err := t.resolveOrderPrice(ctx, req.Symbol, req.StopLoss)
		if err != nil {
			return order, err
		}
		req.StopLoss = stopLoss
		clientOrderID := req.ClientOrderID + "sl"
		id, err := t.placeAlgoCloseOrder(ctx, req.Symbol, clientOrderID, closeSide, positionSide, marginMode, req.Quantity, "conditional", req.StopLoss, false)
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
		t.dispatchAlgoOrderUpdate(req, clientOrderID, id, closeSide, positionSide, req.StopLoss, models.OrderStatusNew)
	}
	return order, nil
}

func (t *TradingModule) PlaceTrailingOrder(ctx context.Context, req models.TrailingOrderReq) (models.TrailingOrder, error) {
	if req.CallbackSpread.IsZero() && req.CallbackRatio.IsZero() {
		return models.TrailingOrder{}, fmt.Errorf("callback spread or callback ratio is required")
	}
	quantity, err := t.resolveOrderQuantity(ctx, req.Symbol, req.Quantity, req.BaseQuantity)
	if err != nil {
		return models.TrailingOrder{}, err
	}
	positionSide := req.PositionSide
	if positionSide == "" {
		positionSide = models.PositionSideLong
		if req.Side == models.OrderSideSell {
			positionSide = models.PositionSideShort
		}
	}
	marginMode := req.MarginMode
	if marginMode == "" {
		marginMode = t.marginMode
	}
	if marginMode == "" {
		marginMode = models.MarginModeCross
	}

	bodyMap := map[string]interface{}{
		"instId":      req.Symbol,
		"tdMode":      okxMarginMode(marginMode),
		"side":        strings.ToLower(string(req.Side)),
		"posSide":     strings.ToLower(string(positionSide)),
		"ordType":     "move_order_stop",
		"sz":          quantity.String(),
		"algoClOrdId": req.ClientOrderID,
	}
	if !req.ActivationPrice.IsZero() {
		activationPrice, err := t.resolveOrderPrice(ctx, req.Symbol, req.ActivationPrice)
		if err != nil {
			return models.TrailingOrder{}, err
		}
		req.ActivationPrice = activationPrice
		bodyMap["activePx"] = req.ActivationPrice.String()
	}
	if !req.CallbackRatio.IsZero() {
		bodyMap["callbackRatio"] = req.CallbackRatio.String()
	}
	if !req.CallbackSpread.IsZero() {
		bodyMap["callbackSpread"] = req.CallbackSpread.String()
	}
	if req.ReduceOnly {
		bodyMap["reduceOnly"] = true
	}

	body, _ := json.Marshal(bodyMap)
	respBytes, err := t.sendRequest(ctx, "POST", "/api/v5/trade/order-algo", body)
	if err != nil {
		return models.TrailingOrder{}, err
	}
	var resp struct {
		Code string `json:"code"`
		Data []struct {
			AlgoID string `json:"algoId"`
		} `json:"data"`
	}
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		return models.TrailingOrder{}, err
	}
	if resp.Code != "0" {
		return models.TrailingOrder{}, fmt.Errorf("okx place trailing order error: %s", string(respBytes))
	}
	if len(resp.Data) == 0 {
		return models.TrailingOrder{}, fmt.Errorf("okx place trailing order returned no algoId")
	}
	orderID := resp.Data[0].AlgoID
	t.dispatchAlgoOrderUpdate(models.TPSLReq{
		Symbol:   req.Symbol,
		Quantity: quantity,
	}, req.ClientOrderID, orderID, req.Side, positionSide, req.ActivationPrice, models.OrderStatusNew)
	return models.TrailingOrder{
		ClientOrderID: req.ClientOrderID,
		OrderID:       orderID,
	}, nil
}

func (t *TradingModule) resolveOrderQuantity(ctx context.Context, symbol string, quantity, baseQuantity decimal.Decimal) (decimal.Decimal, error) {
	instrument, err := NewMarketModule(t.testnet).GetInstrument(ctx, symbol)
	if err != nil {
		return decimal.Zero, err
	}
	if baseQuantity.IsZero() {
		return instrument.OrderQuantityFromBase(instrument.BaseQuantityFromOrder(quantity)), nil
	}
	orderQty := instrument.OrderQuantityFromBase(baseQuantity)
	if orderQty.IsZero() {
		return decimal.Zero, fmt.Errorf("base quantity %s is below exchange step for %s", baseQuantity, symbol)
	}
	if !instrument.MinQty.IsZero() && orderQty.LessThan(instrument.MinQty) {
		return decimal.Zero, fmt.Errorf("base quantity %s converts to %s, below minimum order quantity %s for %s", baseQuantity, orderQty, instrument.MinQty, symbol)
	}
	return orderQty, nil
}

func (t *TradingModule) resolveOrderPrice(ctx context.Context, symbol string, price decimal.Decimal) (decimal.Decimal, error) {
	if price.IsZero() {
		return decimal.Zero, nil
	}
	instrument, err := NewMarketModule(t.testnet).GetInstrument(ctx, symbol)
	if err != nil {
		return decimal.Zero, err
	}
	return instrument.PriceToTick(price), nil
}

func (t *TradingModule) placeAlgoCloseOrder(ctx context.Context, symbol, clientOrderID string, side models.OrderSide, positionSide models.PositionSide, marginMode models.MarginMode, qty decimal.Decimal, ordType string, trigger decimal.Decimal, takeProfit bool) (string, error) {
	bodyMap := map[string]interface{}{
		"instId":      symbol,
		"tdMode":      okxMarginMode(marginMode),
		"side":        strings.ToLower(string(side)),
		"posSide":     strings.ToLower(string(positionSide)),
		"ordType":     ordType,
		"sz":          qty.String(),
		"algoClOrdId": clientOrderID,
	}
	if takeProfit {
		bodyMap["tpTriggerPx"] = trigger.String()
		bodyMap["tpOrdPx"] = "-1"
	} else {
		bodyMap["slTriggerPx"] = trigger.String()
		bodyMap["slOrdPx"] = "-1"
	}
	body, _ := json.Marshal(bodyMap)
	respBytes, err := t.sendRequest(ctx, "POST", "/api/v5/trade/order-algo", body)
	if err != nil {
		return "", err
	}

	var resp struct {
		Code string `json:"code"`
		Data []struct {
			AlgoID string `json:"algoId"`
		} `json:"data"`
	}
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		return "", err
	}
	if resp.Code != "0" {
		return "", fmt.Errorf("okx place algo order error: %s", string(respBytes))
	}
	if len(resp.Data) == 0 {
		return "", fmt.Errorf("okx place algo order returned no algoId")
	}
	return resp.Data[0].AlgoID, nil
}

func (t *TradingModule) CancelTPSL(ctx context.Context, symbol string, order models.TPSLOrder) error {
	var args []map[string]string
	if order.TakeProfitOrderID != "" {
		args = append(args, map[string]string{"instId": symbol, "algoId": order.TakeProfitOrderID})
	}
	if order.StopLossOrderID != "" {
		args = append(args, map[string]string{"instId": symbol, "algoId": order.StopLossOrderID})
	}
	if len(args) == 0 {
		return nil
	}

	body, _ := json.Marshal(args)
	respBytes, err := t.sendRequest(ctx, "POST", "/api/v5/trade/cancel-algos", body)
	if err != nil {
		return err
	}
	var resp struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		return err
	}
	if resp.Code != "0" {
		return fmt.Errorf("okx cancel algo order error: %s", string(respBytes))
	}
	if order.TakeProfitOrderID != "" {
		t.dispatchAlgoOrderUpdate(models.TPSLReq{Symbol: symbol}, order.TakeProfitClientOrderID, order.TakeProfitOrderID, "", "", decimal.Zero, models.OrderStatusCanceled)
	}
	if order.StopLossOrderID != "" {
		t.dispatchAlgoOrderUpdate(models.TPSLReq{Symbol: symbol}, order.StopLossClientOrderID, order.StopLossOrderID, "", "", decimal.Zero, models.OrderStatusCanceled)
	}
	return nil
}

func (t *TradingModule) GetOpenAlgoOrders(ctx context.Context, symbol string) ([]models.OpenAlgoOrder, error) {
	var all []models.OpenAlgoOrder
	for _, ordType := range []string{"conditional", "trigger", "oco", "move_order_stop"} {
		endpoint := "/api/v5/trade/orders-algo-pending?instType=SWAP&ordType=" + url.QueryEscape(ordType)
		if symbol != "" {
			endpoint += "&instId=" + url.QueryEscape(symbol)
		}
		respBytes, err := t.sendRequest(ctx, "GET", endpoint, nil)
		if err != nil {
			return nil, err
		}
		var resp struct {
			Code string `json:"code"`
			Data []struct {
				InstID       string `json:"instId"`
				AlgoID       string `json:"algoId"`
				ClientAlgoID string `json:"algoClOrdId"`
				Side         string `json:"side"`
				PosSide      string `json:"posSide"`
				State        string `json:"state"`
				Quantity     string `json:"sz"`
				TpTriggerPx  string `json:"tpTriggerPx"`
				TpOrdPx      string `json:"tpOrdPx"`
				SlTriggerPx  string `json:"slTriggerPx"`
				SlOrdPx      string `json:"slOrdPx"`
				UpdateTime   string `json:"uTime"`
			} `json:"data"`
		}
		if err := json.Unmarshal(respBytes, &resp); err != nil {
			return nil, err
		}
		if resp.Code != "0" {
			return nil, fmt.Errorf("okx get open algo orders error: %s", string(respBytes))
		}
		for _, row := range resp.Data {
			qty, _ := decimal.NewFromString(row.Quantity)
			all = append(all, models.OpenAlgoOrder{
				Exchange:      "okx",
				Symbol:        row.InstID,
				ClientOrderID: row.ClientAlgoID,
				ExchangeID:    row.AlgoID,
				Side:          okxOrderSide(row.Side),
				PositionSide:  models.PositionSide(strings.ToUpper(row.PosSide)),
				Status:        mapOKXOrderStatus(row.State),
				TriggerPrice:  firstDecimal(row.TpTriggerPx, row.SlTriggerPx),
				OrderPrice:    firstDecimal(row.TpOrdPx, row.SlOrdPx),
				Quantity:      qty,
				UpdateTime:    parseInt64(row.UpdateTime),
			})
		}
	}
	return all, nil
}

func (t *TradingModule) dispatchAlgoOrderUpdate(req models.TPSLReq, clientOrderID, exchangeID string, side models.OrderSide, positionSide models.PositionSide, trigger decimal.Decimal, status models.OrderStatus) {
	if clientOrderID == "" {
		return
	}
	t.router.DispatchAlgoOrder(&models.AlgoOrderUpdate{
		Exchange:      "okx",
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
	endpoint := "/api/v5/account/positions?instId=" + url.QueryEscape(symbol)
	respBytes, err := t.sendRequest(ctx, "GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	var resp struct {
		Code string `json:"code"`
		Data []struct {
			InstID  string `json:"instId"`
			PosSide string `json:"posSide"`
			Pos     string `json:"pos"`
			AvgPx   string `json:"avgPx"`
			MarkPx  string `json:"markPx"`
			Upl     string `json:"upl"`
			Lever   string `json:"lever"`
			LiqPx   string `json:"liqPx"`
			UTime   string `json:"uTime"`
		} `json:"data"`
	}
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		return nil, err
	}
	if resp.Code != "0" {
		return nil, fmt.Errorf("okx get position error: %s", string(respBytes))
	}
	wantSide := strings.ToLower(string(side))
	if wantSide == "" {
		wantSide = "long"
	}
	for _, row := range resp.Data {
		if row.PosSide != wantSide {
			continue
		}
		amt, _ := decimal.NewFromString(row.Pos)
		entry, _ := decimal.NewFromString(row.AvgPx)
		mark, _ := decimal.NewFromString(row.MarkPx)
		pnl, _ := decimal.NewFromString(row.Upl)
		liq, _ := decimal.NewFromString(row.LiqPx)
		leverage, _ := strconv.Atoi(row.Lever)
		var updateTime int64
		fmt.Sscanf(row.UTime, "%d", &updateTime)
		return &models.Position{
			Exchange:         "okx",
			Symbol:           row.InstID,
			PositionSide:     strings.ToUpper(row.PosSide),
			PositionAmt:      amt,
			EntryPrice:       entry,
			MarkPrice:        mark,
			UnRealizedPnL:    pnl,
			LiquidationPrice: liq,
			Leverage:         leverage,
			UpdateTime:       updateTime,
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
		MarginMode:    t.marginMode,
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

func okxDefaultPositionSide(req models.PlaceOrderReq) string {
	if req.PositionSide != "" {
		return strings.ToLower(string(req.PositionSide))
	}
	if req.Side == models.OrderSideSell {
		return "short"
	}
	return "long"
}

func (t *TradingModule) CancelOrder(ctx context.Context, symbol, clientOrderID string) error {
	bodyMap := map[string]interface{}{
		"instId":  symbol,
		"clOrdId": clientOrderID,
	}

	body, _ := json.Marshal(bodyMap)
	respBytes, err := t.sendRequest(ctx, "POST", "/api/v5/trade/cancel-order", body)
	if err != nil {
		return err
	}

	var resp struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(respBytes, &resp); err != nil {
		return err
	}

	if resp.Code != "0" {
		return fmt.Errorf("okx cancel order error: %s", string(respBytes))
	}

	return nil
}
