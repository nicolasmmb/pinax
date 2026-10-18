package cli

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/nicolasmmb/pinax/internal/adapter/ipc"
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

func TestStatusCommandPrintsJobs(t *testing.T) {
	ctx := context.Background()
	store, err := turso.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	job, _, err := store.EnqueueJob(ctx, domain.EnqueueRequest{
		InputPath:      "/docs",
		InputKind:      domain.InputDir,
		EmbeddingModel: "model",
		ChunkerVersion: "rune-window-v2",
		ChunkSize:      1200,
		ChunkOverlap:   150,
	})
	if err != nil {
		t.Fatal(err)
	}
	socketPath := ipc.SocketPath(":memory:")
	server, err := ipc.Listen(socketPath, store, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	go func() { _ = server.Serve(ctx) }()

	runtime := Runtime{Config: config.Config{DatabasePath: ":memory:"}}
	var stdout, stderr bytes.Buffer
	if err := Run(ctx, []string{"status"}, &stdout, &stderr, runtime); err != nil {
		t.Fatalf("status: %v\n%s", err, stderr.String())
	}
	output := stdout.String()
	if !strings.Contains(output, shortID(job.ID)) || !strings.Contains(output, "queued") || !strings.Contains(output, "/docs") {
		t.Fatalf("unexpected status output:\n%s", output)
	}
}

func TestRenderStatusShowsActiveFilesAndFailures(t *testing.T) {
	status := domain.StatusResult{
		Worker: domain.WorkerStatus{
			WorkerLease: domain.WorkerLease{
				WorkerID:    "worker-1",
				PID:         42,
				HeartbeatAt: time.Now(),
			},
			Online: true,
		},
		Jobs: []domain.JobSnapshot{{
			Job: domain.Job{
				ID:        "abcdef1234567890",
				InputPath: "/docs",
				Status:    domain.JobRunning,
			},
			Progress: domain.JobProgress{FilesTotal: 2, FilesDone: 1, Percent: 50, Chunks: 10},
			ActiveFiles: []domain.JobFile{{
				Path:   "/docs/a.pdf",
				Status: domain.FileRunning,
				Stage:  domain.StageEmbedding,
				Chunks: 7,
			}},
			Failures: []domain.JobFile{{
				Path:  "/docs/b.pdf",
				Error: "broken",
			}},
		}},
	}
	var output bytes.Buffer
	renderStatus(&output, status)
	text := output.String()
	for _, want := range []string{"worker: online=true", "abcdef123456", "running", "embedding", "/docs/a.pdf", "broken"} {
		if !strings.Contains(text, want) {
			t.Fatalf("status output missing %q:\n%s", want, text)
		}
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
