//go:build e2e

package jobs_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nicolasmmb/pinax/internal/adapter/files"
	"github.com/nicolasmmb/pinax/internal/adapter/ollama"
	"github.com/nicolasmmb/pinax/internal/adapter/turso"
	"github.com/nicolasmmb/pinax/internal/app/ingest"
	"github.com/nicolasmmb/pinax/internal/app/jobs"
	"github.com/nicolasmmb/pinax/internal/app/query"
	"github.com/nicolasmmb/pinax/internal/domain"
)

// TestE2EIngestAndSearch runs against a real embedding provider and a real
// document directory. Enable with:
//
//	PINAX_TEST_DOCUMENTS=/path/to/documents \
//	EMBED_URL=http://127.0.0.1:11434 EMBED_MODEL=embeddinggemma-2:740m \
//	go test -tags e2e ./internal/app/jobs -run TestE2E -v
func TestE2EIngestAndSearch(t *testing.T) {
	root := os.Getenv("PINAX_TEST_DOCUMENTS")
	if root == "" {
		t.Skip("PINAX_TEST_DOCUMENTS is not set")
	}
	embedURL := os.Getenv("EMBED_URL")
	if embedURL == "" {
		embedURL = "http://127.0.0.1:11434"
	}
	model := os.Getenv("EMBED_MODEL")
	if model == "" {
		model = "embeddinggemma-2:740m"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	store, err := turso.OpenWithOptions(ctx, turso.Options{
		Path:         filepath.Join(t.TempDir(), "pinax-e2e.db"),
		MaxOpenConns: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	embedder := ollama.NewEmbedder(ollama.New(embedURL), model)
	ingestor, err := ingest.New(root, []ingest.Extractor{files.Markdown{}, files.PDF{}}, embedder, store)
	if err != nil {
		t.Fatal(err)
	}
	version, size, overlap := ingestor.Chunker()
	jobService := jobs.NewService(store, ingestor, jobs.SnapshotConfig{
		EmbeddingModel: ingestor.Model(),
		ChunkerVersion: version,
		ChunkSize:      size,
		ChunkOverlap:   overlap,
	})
	job, _, err := jobService.Enqueue(ctx, root)
	if err != nil {
		t.Fatal(err)
	}
	worker, err := jobs.NewWorker(store, ingestor, jobs.WorkerConfig{Workers: 4, JobSlots: 1, Once: true})
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := worker.Run(ctx); err != nil {
		t.Fatal(err)
	}
	stored, err := store.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status != domain.JobCompleted && stored.Status != domain.JobCompletedWithErrors {
		t.Fatalf("job status = %s, error = %s", stored.Status, stored.Error)
	}
	progress, err := store.JobProgress(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if progress.Indexed == 0 {
		failures, _, _ := store.Failures(ctx, job.ID, 20)
		t.Fatalf("no documents indexed: %#v", failures)
	}
	t.Logf("indexed %d files, %d chunks in %s", progress.Indexed, progress.Chunks, time.Since(started))

	results, err := query.New(embedder, store).Search(ctx, "proposta comercial", 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) == 0 {
		t.Fatal("search returned no results")
	}
	t.Logf("search returned %d results, first = %s page %d", len(results), results[0].Path, results[0].Page)
}
