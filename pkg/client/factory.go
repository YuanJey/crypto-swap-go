package client

import (
	"errors"

	"github.com/crypto-swap-go/internal/exchange/binance"
	"github.com/crypto-swap-go/internal/exchange/okx"
	"github.com/crypto-swap-go/pkg/modules"
)

type ExchangeType string

const (
	ExchangeBinance ExchangeType = "binance"
	ExchangeOKX     ExchangeType = "okx"
)

type Credentials struct {
	APIKey     string
	APISecret  string
	Passphrase string // specifically for OKX
}

// Config defines SDK initialization parameters
type Config struct {
	Exchange   ExchangeType
	Testnet    bool
	Creds      Credentials
}

// NewClient initializes and returns an exchange-specific unified client
func NewClient(cfg Config) (modules.ExchangeClient, error) {
	switch cfg.Exchange {
	case ExchangeBinance:
		return binance.NewBinanceClient(cfg.Creds.APIKey, cfg.Creds.APISecret, cfg.Testnet), nil
	case ExchangeOKX:
		return okx.NewOKXClient(cfg.Creds.APIKey, cfg.Creds.APISecret, cfg.Creds.Passphrase, cfg.Testnet), nil
	default:
		return nil, errors.New("unsupported exchange type")
	}
}
