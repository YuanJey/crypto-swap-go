package secure

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
)

// HmacSha256Hex returns HMAC-SHA256 signature as hex string (used by Binance)
func HmacSha256Hex(secret, payload string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil))
}

// HmacSha256Base64 returns HMAC-SHA256 signature as Base64 string (used by OKX)
func HmacSha256Base64(secret, payload string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload))
	return base64.StdEncoding.EncodeToString(mac.Sum(nil))
}
