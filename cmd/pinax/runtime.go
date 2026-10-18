package main

import (
	"context"
	"fmt"
	"time"

	"github.com/nicolasmmb/pinax/internal/adapter/files"
	"github.com/nicolasmmb/pinax/internal/adapter/ipc"
	"github.com/nicolasmmb/pinax/internal/adapter/ollama"
	"github.com/nicolasmmb/pinax/internal/adapter/openai"
	"github.com/nicolasmmb/pinax/internal/adapter/turso"
	"github.com/nicolasmmb/pinax/internal/app/ingest"
	"github.com/nicolasmmb/pinax/internal/app/jobs"
	"github.com/nicolasmmb/pinax/internal/config"
)

type embedder interface {
	Model() string
	EmbedDocuments(context.Context, []string) ([][]float32, error)
	EmbedQuery(context.Context, string) ([]float32, error)
}

type mcpDeps struct {
	Embedder  embedder
	Client    *ipc.Client
	Validator *ingest.Validator
}

type indexDeps struct {
	Config    config.Config
	Model     string
	Validator *ingest.Validator
}

type workerDeps struct {
	Config config.Config
	Store  *turso.Store
	Ingest *ingest.Service
}

func newMCPDeps(cfg config.Config) (mcpDeps, error) {
	embeddings, err := newEmbedder(cfg)
	if err != nil {
		return mcpDeps{}, err
	}
	validator, err := newValidator(cfg)
	if err != nil {
		return mcpDeps{}, err
	}
	return mcpDeps{
		Embedder: embeddings,
		Client:   ipc.NewClient(ipc.SocketPath(cfg.DatabasePath)), Validator: validator,
	}, nil
}

func newIndexDeps(cfg config.Config) (indexDeps, error) {
	validator, err := newValidator(cfg)
	if err != nil {
		return indexDeps{}, err
	}
	return indexDeps{Config: cfg, Model: cfg.EmbedModel, Validator: validator}, nil
}

func newValidator(cfg config.Config) (*ingest.Validator, error) {
	return ingest.NewValidator(cfg.DocumentRoot, []ingest.Extractor{files.Markdown{}, files.PDF{}})
}

func newWorkerDeps(ctx context.Context, cfg config.Config) (workerDeps, func(), error) {
	embeddings, err := newEmbedder(cfg)
	if err != nil {
		return workerDeps{}, nil, err
	}
	store, err := turso.OpenWithOptions(ctx, turso.Options{
		Path: cfg.DatabasePath, MaxOpenConns: cfg.WorkerConcurrency + 4, BusyTimeout: 10 * time.Second,
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

func jobSnapshot(model string) jobs.SnapshotConfig {
	version, size, overlap := ingest.ChunkerDefaults()
	return jobs.SnapshotConfig{EmbeddingModel: model, ChunkerVersion: version, ChunkSize: size, ChunkOverlap: overlap}
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
