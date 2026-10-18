package cli

import (
	"context"
	"fmt"
	"time"

	"github.com/nicolasmmb/pinax/internal/adapter/files"
	"github.com/nicolasmmb/pinax/internal/adapter/ollama"
	"github.com/nicolasmmb/pinax/internal/adapter/openai"
	"github.com/nicolasmmb/pinax/internal/adapter/turso"
	"github.com/nicolasmmb/pinax/internal/app/ingest"
	"github.com/nicolasmmb/pinax/internal/config"
)

type embedder interface {
	Model() string
	EmbedDocuments(context.Context, []string) ([][]float32, error)
	EmbedQuery(context.Context, string) ([]float32, error)
}

type serveDeps struct {
	Config    config.Config
	Embedder  embedder
	Validator *ingest.Validator
}

type workerDeps struct {
	Config config.Config
	Store  *turso.Store
	Ingest *ingest.Service
}

type statusDeps struct {
	Config config.Config
}

func newServeDeps(cfg config.Config) (serveDeps, error) {
	embeddings, err := newEmbedder(cfg)
	if err != nil {
		return serveDeps{}, err
	}
	validator, err := ingest.NewValidator(cfg.DocumentRoot, []ingest.Extractor{files.Markdown{}, files.PDF{}})
	if err != nil {
		return serveDeps{}, err
	}
	return serveDeps{Config: cfg, Embedder: embeddings, Validator: validator}, nil
}

func newWorkerDeps(ctx context.Context, cfg config.Config) (workerDeps, func(), error) {
	embeddings, err := newEmbedder(cfg)
	if err != nil {
		return workerDeps{}, nil, err
	}
	store, err := turso.OpenWithOptions(ctx, turso.Options{
		Path:         cfg.DatabasePath,
		MaxOpenConns: cfg.IngestWorkers + 4,
		BusyTimeout:  10 * time.Second,
	})
	if err != nil {
		return workerDeps{}, nil, err
	}
	ingestor, err := ingest.New(cfg.DocumentRoot, []ingest.Extractor{files.Markdown{}, files.PDF{}}, embeddings, store)
	if err != nil {
		_ = store.Close()
		return workerDeps{}, nil, err
	}
	ingestor.SetEmbedBatchSize(cfg.EmbedBatchSize)
	return workerDeps{Config: cfg, Store: store, Ingest: ingestor}, func() { _ = store.Close() }, nil
}

func newEmbedder(cfg config.Config) (embedder, error) {
	switch cfg.EmbedProvider {
	case config.EmbedProviderOllama:
		return ollama.NewEmbedder(ollama.New(cfg.EmbedURL), cfg.EmbedModel), nil
	case config.EmbedProviderOpenAI:
		return openai.New(cfg.EmbedURL, cfg.EmbedAPIKey, cfg.EmbedModel), nil
	default:
		return nil, fmt.Errorf("unsupported embedding provider %q", cfg.EmbedProvider)
	}
}
