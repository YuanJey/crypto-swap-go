package integration

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// LoadDotEnvForTest loads simple KEY=VALUE pairs from the repository .env file.
// Existing process environment variables take precedence.
func LoadDotEnvForTest(t *testing.T) {
	t.Helper()

	path := filepath.Join(repoRoot(t), ".env")
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("open .env: %v", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		key, value, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		value = strings.Trim(strings.TrimSpace(value), `"'`)
		if key == "" || os.Getenv(key) != "" {
			continue
		}
		t.Setenv(key, value)
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("read .env: %v", err)
	}
}

func RequireIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv("CRYPTO_SWAP_INTEGRATION") != "1" {
		t.Skip("set CRYPTO_SWAP_INTEGRATION=1 to run exchange integration tests")
	}
	LoadDotEnvForTest(t)
}

func RequireEnv(t *testing.T, key string) string {
	t.Helper()
	value := os.Getenv(key)
	if value == "" {
		t.Fatalf("%s is required", key)
	}
	return value
}

func repoRoot(t *testing.T) string {
	t.Helper()

	dir, err := os.Getwd()
	if err != nil {
		t.Fatalf("getwd: %v", err)
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("could not find repository root")
		}
		dir = parent
	}
}
