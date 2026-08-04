package okx

import (
	"context"

	"github.com/crypto-swap-go/pkg/modules"
)

type OKXClient struct {
	marketModule  *MarketModule
	tradingModule *TradingModule
}

func NewOKXClient(apiKey, apiSecret, passphrase string, testnet bool) *OKXClient {
	return &OKXClient{
		marketModule:  NewMarketModule(testnet),
		tradingModule: NewTradingModule(apiKey, apiSecret, passphrase, testnet),
	}
}

func (c *OKXClient) MarketModule() modules.MarketModule {
	return c.marketModule
}

func (c *OKXClient) TradingModule() modules.TradingModule {
	return c.tradingModule
}

func (c *OKXClient) Start(ctx context.Context) error {
	if err := c.marketModule.Start(ctx); err != nil {
		return err
	}
	if err := c.tradingModule.Start(ctx); err != nil {
		return err
	}
	return nil
}

func (c *OKXClient) Stop() error {
	c.marketModule.Stop()
	c.tradingModule.Stop()
	return nil
}
