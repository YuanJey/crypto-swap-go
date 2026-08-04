package secure

import "testing"

func TestHmacSha256Hex(t *testing.T) {
	got := HmacSha256Hex("secret", "payload")
	want := "b82fcb791acec57859b989b430a826488ce2e479fdf92326bd0a2e8375a42ba4"
	if got != want {
		t.Fatalf("HmacSha256Hex() = %q, want %q", got, want)
	}
}

func TestHmacSha256Base64(t *testing.T) {
	got := HmacSha256Base64("secret", "payload")
	want := "uC/LeRrOxXhZuYm0MKgmSIzi5Hn9+SMmvQoug3WkK6Q="
	if got != want {
		t.Fatalf("HmacSha256Base64() = %q, want %q", got, want)
	}
}
