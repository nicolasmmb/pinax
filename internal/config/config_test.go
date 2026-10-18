package config

import (
	"os"
	"strings"
	"testing"
)

func clearConfigEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{"PINAX_DOCUMENT_ROOT", "PINAX_DATABASE_PATH", "EMBED_PROVIDER", "EMBED_URL", "EMBED_MODEL", "EMBED_API_KEY"} {
		value, exists := os.LookupEnv(key)
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if exists {
				_ = os.Setenv(key, value)
			} else {
				_ = os.Unsetenv(key)
			}
		})
	}
	t.Setenv("PINAX_DOCUMENT_ROOT", ".")
	t.Setenv("PINAX_DATABASE_PATH", "./data/pinax.db")
}

func TestLoadDefaultsOllama(t *testing.T) {
	clearConfigEnv(t)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.EmbedProvider != EmbedProviderOllama || cfg.EmbedURL != "http://127.0.0.1:11434" || cfg.EmbedModel != "embeddinggemma-2:740m" {
		t.Fatalf("unexpected Ollama defaults: %+v", cfg)
	}
}

func TestLoadRejectsInvalidProvider(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("EMBED_PROVIDER", "invalid")
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "not in enum") {
		t.Fatalf("expected invalid provider enum error, got %v", err)
	}
}

func TestLoadOpenAIDefaultURLRequiresModel(t *testing.T) {
	clearConfigEnv(t)
	t.Setenv("EMBED_PROVIDER", "openai")
	_, err := Load()
	if err == nil || !strings.Contains(err.Error(), "EMBED_MODEL cannot be empty") {
		t.Fatalf("expected missing model error, got %v", err)
	}
}
