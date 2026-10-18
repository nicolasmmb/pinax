package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/nicolasmmb/pinax/internal/adapter/turso"
	"github.com/nicolasmmb/pinax/internal/app/ingest"
	"github.com/nicolasmmb/pinax/internal/app/jobs"
	"github.com/nicolasmmb/pinax/internal/app/query"
	"github.com/nicolasmmb/pinax/internal/config"
	"github.com/nicolasmmb/pinax/internal/domain"
)

type cliExtractor struct{}

func (cliExtractor) Supports(string) bool { return true }
func (cliExtractor) Extract(_ context.Context, path string) ([]domain.Page, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return []domain.Page{{Number: 1, Text: string(data)}}, nil
}

type cliEmbedder struct{}

func (cliEmbedder) Model() string { return "cli-model" }
func (cliEmbedder) EmbedDocuments(_ context.Context, texts []string) ([][]float32, error) {
	vectors := make([][]float32, len(texts))
	for i := range vectors {
		vectors[i] = []float32{1, 0}
	}
	return vectors, nil
}
func (cliEmbedder) EmbedQuery(context.Context, string) ([]float32, error) {
	return []float32{1, 0}, nil
}

func testRuntime(t *testing.T) Runtime {
	t.Helper()
	root := t.TempDir()
	if err := os.WriteFile(filepath.Join(root, "notes.md"), []byte("conteudo de teste"), 0o600); err != nil {
		t.Fatal(err)
	}
	store, err := turso.Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	embedder := cliEmbedder{}
	ingestor, err := ingest.New(root, []ingest.Extractor{cliExtractor{}}, embedder, store)
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
	return Runtime{
		Config: config.Config{
			DocumentRoot:    root,
			DatabasePath:    ":memory:",
			IngestWorkers:   2,
			WorkerJobSlots:  2,
			EmbedBatchSize:  64,
			WorkerHeartbeat: time.Second,
		},
		Ingest: ingestor,
		Query:  query.New(embedder, store),
		Jobs:   jobService,
		Store:  store,
	}
}

func TestWorkerCommandDrainsQueue(t *testing.T) {
	ctx := context.Background()
	runtime := testRuntime(t)
	job, _, err := runtime.Jobs.Enqueue(ctx, runtime.Config.DocumentRoot)
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	runCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := Run(runCtx, []string{"worker", "--once"}, os.Stdout, &stderr, runtime); err != nil {
		t.Fatalf("worker run: %v\n%s", err, stderr.String())
	}
	status, err := runtime.Jobs.Status(ctx, job.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Jobs) != 1 || status.Jobs[0].Status != domain.JobCompleted {
		t.Fatalf("job status = %#v", status.Jobs)
	}
	if status.Jobs[0].Progress.Indexed != 1 {
		t.Fatalf("progress = %#v", status.Jobs[0].Progress)
	}
}

func TestUsageForInvalidCommand(t *testing.T) {
	runtime := testRuntime(t)
	var stderr bytes.Buffer
	if err := Run(context.Background(), []string{"bogus"}, os.Stdout, &stderr, runtime); err == nil {
		t.Fatal("expected invalid command to fail")
	}
	if stderr.Len() == 0 {
		t.Fatal("expected usage output")
	}
}

func TestWorkerRejectsExtraArguments(t *testing.T) {
	runtime := testRuntime(t)
	var stderr bytes.Buffer
	if err := Run(context.Background(), []string{"worker", "extra"}, os.Stdout, &stderr, runtime); err == nil {
		t.Fatal("expected extra arguments to fail")
	}
}
