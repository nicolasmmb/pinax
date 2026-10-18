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
	"github.com/nicolasmmb/pinax/internal/app/jobs"
	"github.com/nicolasmmb/pinax/internal/app/query"
	"github.com/nicolasmmb/pinax/internal/config"
)

type embedder interface {
	Model() string
	EmbedDocuments(context.Context, []string) ([][]float32, error)
	EmbedQuery(context.Context, string) ([]float32, error)
}

func newRuntime(ctx context.Context, cfg config.Config) (Runtime, func(), error) {
	embeddings, err := newEmbedder(cfg)
	if err != nil {
		return Runtime{}, nil, err
	}
	store, err := turso.OpenWithOptions(ctx, turso.Options{
		Path:         cfg.DatabasePath,
		MaxOpenConns: cfg.IngestWorkers + 4,
		BusyTimeout:  10 * time.Second,
	})
	if err != nil {
		return Runtime{}, nil, err
	}
	ingestor, err := ingest.New(cfg.DocumentRoot, []ingest.Extractor{files.Markdown{}, files.PDF{}}, embeddings, store)
	if err != nil {
		_ = store.Close()
		return Runtime{}, nil, err
	}
	ingestor.SetEmbedBatchSize(cfg.EmbedBatchSize)
	version, size, overlap := ingestor.Chunker()
	heartbeat := cfg.WorkerHeartbeat
	leaseTTL := max(6*heartbeat, 30*time.Second)
	jobService := jobs.NewService(store, ingestor, jobs.SnapshotConfig{
		EmbeddingModel: ingestor.Model(),
		ChunkerVersion: version,
		ChunkSize:      size,
		ChunkOverlap:   overlap,
	}, jobs.ServiceOptions{LeaseTTL: leaseTTL, StaleAfter: leaseTTL})
	return Runtime{
		Config: cfg,
		Ingest: ingestor,
		Query:  query.New(embeddings, store),
		Jobs:   jobService,
		Store:  store,
	}, func() { _ = store.Close() }, nil
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
