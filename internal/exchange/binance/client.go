package binance

import (
	"context"

	"github.com/crypto-swap-go/pkg/modules"
)

type BinanceClient struct {
	marketModule  *MarketModule
	tradingModule *TradingModule
}

func NewBinanceClient(apiKey, apiSecret string, testnet bool) *BinanceClient {
	return &BinanceClient{
		marketModule:  NewMarketModule(testnet),
		tradingModule: NewTradingModule(apiKey, apiSecret, testnet),
	}
}

func (c *BinanceClient) MarketModule() modules.MarketModule {
	return c.marketModule
}

func (c *BinanceClient) TradingModule() modules.TradingModule {
	return c.tradingModule
}

func (c *BinanceClient) Start(ctx context.Context) error {
	if err := c.marketModule.Start(ctx); err != nil {
		return err
	}
	if err := c.tradingModule.Start(ctx); err != nil {
		return err
	}
	return nil
}

func (c *BinanceClient) Stop() error {
	c.marketModule.Stop()
	c.tradingModule.Stop()
	return nil
}
