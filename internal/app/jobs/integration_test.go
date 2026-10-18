package jobs_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nicolasmmb/pinax/internal/adapter/turso"
	"github.com/nicolasmmb/pinax/internal/app/ingest"
	"github.com/nicolasmmb/pinax/internal/app/jobs"
	"github.com/nicolasmmb/pinax/internal/domain"
)

type integrationExtractor struct{}

func (integrationExtractor) Supports(string) bool { return true }
func (integrationExtractor) Extract(_ context.Context, path string) ([]domain.Page, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return []domain.Page{{Number: 1, Text: string(data)}}, nil
}

type slowEmbedder struct {
	delay time.Duration
}

func (slowEmbedder) Model() string { return "integration-model" }
func (e slowEmbedder) EmbedDocuments(ctx context.Context, texts []string) ([][]float32, error) {
	select {
	case <-time.After(e.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	vectors := make([][]float32, len(texts))
	for i := range vectors {
		vectors[i] = []float32{1, 0}
	}
	return vectors, nil
}

func TestSearchRemainsResponsiveDuringIngestion(t *testing.T) {
	ctx := context.Background()
	root := t.TempDir()
	const fileCount = 12
	for i := 0; i < fileCount; i++ {
		path := filepath.Join(root, fmt.Sprintf("doc-%02d.md", i))
		if err := os.WriteFile(path, []byte(fmt.Sprintf("conteudo do documento %d", i)), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	dbPath := filepath.Join(t.TempDir(), "pinax.db")
	store, err := turso.OpenWithOptions(ctx, turso.Options{Path: dbPath, MaxOpenConns: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	embedder := slowEmbedder{delay: 15 * time.Millisecond}
	ingestor, err := ingest.New(root, []ingest.Extractor{integrationExtractor{}}, embedder, store)
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
	worker, err := jobs.NewWorker(store, ingestor, jobs.WorkerConfig{Concurrency: 4})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- worker.Run(runCtx) }()

	var (
		searches   atomic.Int64
		maxResults atomic.Int64
	)
	stopReader := make(chan struct{})
	readerErr := make(chan error, 1)
	go func() {
		for {
			select {
			case <-stopReader:
				return
			default:
			}
			results, err := store.Search(ctx, []float32{1, 0}, "integration-model", 5)
			if err != nil {
				select {
				case readerErr <- err:
				default:
				}
				return
			}
			searches.Add(1)
			if int64(len(results)) > maxResults.Load() {
				maxResults.Store(int64(len(results)))
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	waitFor(t, 20*time.Second, "job completion", func() bool {
		stored, err := store.GetJob(ctx, job.ID)
		return err == nil && stored.Status == domain.JobCompleted
	})
	close(stopReader)
	select {
	case err := <-readerErr:
		t.Fatalf("search during ingestion failed: %v", err)
	default:
	}
	if searches.Load() == 0 {
		t.Fatal("search did not run during ingestion")
	}
	if maxResults.Load() == 0 {
		t.Fatal("search never observed indexed documents")
	}
	progress, err := store.JobProgress(ctx, job.ID)
	if err != nil || progress.Indexed != fileCount {
		t.Fatalf("progress = %#v, %v", progress, err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("worker did not stop")
	}
}
