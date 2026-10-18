package config

import (
	"fmt"
	"path/filepath"

	"github.com/nicolasmmb/envx"
)

type EmbedProvider string

const (
	EmbedProviderOllama EmbedProvider = "ollama"
	EmbedProviderOpenAI EmbedProvider = "openai"
)

func (EmbedProvider) EnumValues() []string {
	return []string{string(EmbedProviderOllama), string(EmbedProviderOpenAI)}
}

type Config struct {
	DocumentRoot  string        `envx:"name=PINAX_DOCUMENT_ROOT,required=true"`
	DatabasePath  string        `envx:"name=PINAX_DATABASE_PATH,default=./data/pinax.db"`
	EmbedProvider EmbedProvider `envx:"name=EMBED_PROVIDER,default=ollama"`
	EmbedURL      string        `envx:"name=EMBED_URL"`
	EmbedModel    string        `envx:"name=EMBED_MODEL"`
	EmbedAPIKey   string        `envx:"name=EMBED_API_KEY"`
}

func Load() (Config, error) {
	cfg, err := envx.Load[Config]()
	if err != nil {
		return Config{}, fmt.Errorf("load configuration: %w", err)
	}

	switch cfg.EmbedProvider {
	case EmbedProviderOllama:
		if cfg.EmbedURL == "" {
			cfg.EmbedURL = "http://127.0.0.1:11434"
		}
		if cfg.EmbedModel == "" {
			cfg.EmbedModel = "embeddinggemma-2:740m"
		}
	case EmbedProviderOpenAI:
		if cfg.EmbedURL == "" {
			cfg.EmbedURL = "https://api.openai.com/v1"
		}
		if cfg.EmbedModel == "" {
			return Config{}, fmt.Errorf("EMBED_MODEL cannot be empty")
		}
	}
	root, err := filepath.Abs(cfg.DocumentRoot)
	if err != nil {
		return Config{}, fmt.Errorf("resolve document root: %w", err)
	}
	cfg.DocumentRoot = root
	db, err := filepath.Abs(cfg.DatabasePath)
	if err != nil {
		return Config{}, fmt.Errorf("resolve database path: %w", err)
	}
	cfg.DatabasePath = db
	return *cfg, nil
}
