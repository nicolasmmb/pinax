package main

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

	"github.com/nicolasmmb/pinax/internal/adapter/cli"
	"github.com/nicolasmmb/pinax/internal/adapter/ipc"
	"github.com/nicolasmmb/pinax/internal/adapter/turso"
	"github.com/nicolasmmb/pinax/internal/app/ingest"
	"github.com/nicolasmmb/pinax/internal/app/jobs"
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

func testWorkerDeps(t *testing.T) (workerDeps, *jobs.Service) {
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
	ingestor, err := ingest.New(root, []ingest.Extractor{cliExtractor{}}, cliEmbedder{}, store)
	if err != nil {
		t.Fatal(err)
	}
	version, size, overlap := ingestor.Chunker()
	service := jobs.NewService(store, ingestor, jobs.SnapshotConfig{
		EmbeddingModel: ingestor.Model(),
		ChunkerVersion: version,
		ChunkSize:      size,
		ChunkOverlap:   overlap,
	})
	cfg := config.Config{
		DocumentRoot:      root,
		DatabasePath:      ":memory:",
		WorkerConcurrency: 2,
		EmbedBatchSize:    64,
		WorkerHeartbeat:   time.Second,
	}
	return workerDeps{Config: cfg, Store: store, Ingest: ingestor}, service
}

func TestWorkerCommandDrainsQueue(t *testing.T) {
	ctx := context.Background()
	deps, service := testWorkerDeps(t)
	job, _, err := service.Enqueue(ctx, deps.Config.DocumentRoot)
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	runCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := runWorker(runCtx, deps, true, &stderr); err != nil {
		t.Fatalf("worker run: %v\n%s", err, stderr.String())
	}
	status, err := service.Status(ctx, job.ID, 1)
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

	deps := config.Config{DatabasePath: ":memory:"}
	var stdout, stderr bytes.Buffer
	cmd, err := cli.Parse([]string{"status"}, &stderr)
	if err != nil {
		t.Fatal(err)
	}
	if err := runStatus(ctx, deps, statusOptions{jobID: cmd.StatusJob, watch: cmd.Watch, limit: cmd.Limit}, &stdout); err != nil {
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
			WorkerInfo: domain.WorkerInfo{
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

func testIndexEnv(t *testing.T) (indexDeps, *turso.Store) {
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

	cfg := config.Config{
		DocumentRoot:  root,
		DatabasePath:  filepath.Join(root, "db"),
		EmbedProvider: config.EmbedProviderOllama,
		EmbedModel:    "test-model",
	}
	deps, err := newIndexDeps(cfg)
	if err != nil {
		t.Fatal(err)
	}
	server, err := ipc.Listen(ipc.SocketPath(cfg.DatabasePath), store, store, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = server.Close() })
	go func() { _ = server.Serve(context.Background()) }()
	return deps, store
}

func TestIndexCommandEnqueues(t *testing.T) {
	ctx := context.Background()
	deps, store := testIndexEnv(t)
	path := filepath.Join(deps.Config.DocumentRoot, "notes.md")
	resolved, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}

	var stdout bytes.Buffer
	if err := runIndex(ctx, deps, path, &stdout); err != nil {
		t.Fatal(err)
	}
	output := stdout.String()
	for _, want := range []string{"job=", "status=queued", "deduplicated=false", "path=" + resolved} {
		if !strings.Contains(output, want) {
			t.Fatalf("output missing %q:\n%s", want, output)
		}
	}
	list, err := store.ListJobs(ctx, 10)
	if err != nil || len(list) != 1 {
		t.Fatalf("jobs = %#v, %v", list, err)
	}
	if list[0].InputPath != resolved || list[0].InputKind != domain.InputFile {
		t.Fatalf("unexpected job: %#v", list[0])
	}

	stdout.Reset()
	if err := runIndex(ctx, deps, path, &stdout); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "deduplicated=true") {
		t.Fatalf("expected deduplicated job:\n%s", stdout.String())
	}
	list, err = store.ListJobs(ctx, 10)
	if err != nil || len(list) != 1 {
		t.Fatalf("duplicate job created: %#v, %v", list, err)
	}
}

func TestIndexCommandDirectory(t *testing.T) {
	ctx := context.Background()
	deps, store := testIndexEnv(t)
	dir := filepath.Join(deps.Config.DocumentRoot, "sub")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "a.md"), []byte("a"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := runIndex(ctx, deps, dir, io.Discard); err != nil {
		t.Fatal(err)
	}
	list, err := store.ListJobs(ctx, 10)
	if err != nil || len(list) != 1 || list[0].InputKind != domain.InputDir {
		t.Fatalf("jobs = %#v, %v", list, err)
	}
}

func TestIndexCommandRejectsOutsideRoot(t *testing.T) {
	deps, _ := testIndexEnv(t)
	outside := filepath.Join(t.TempDir(), "outside.md")
	if err := os.WriteFile(outside, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := runIndex(context.Background(), deps, outside, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "outside configured document root") {
		t.Fatalf("expected root validation error, got %v", err)
	}
}

func TestIndexCommandWorkerOffline(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "notes.md")
	if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	deps, err := newIndexDeps(config.Config{
		DocumentRoot:  root,
		DatabasePath:  filepath.Join(root, "missing"),
		EmbedProvider: config.EmbedProviderOllama,
		EmbedModel:    "test-model",
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	err = runIndex(ctx, deps, path, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "worker offline") {
		t.Fatalf("expected offline error, got %v", err)
	}
}

func TestMCPDependenciesDoNotRequireWorker(t *testing.T) {
	root := t.TempDir()
	deps, err := newMCPDeps(config.Config{
		DocumentRoot: root, DatabasePath: filepath.Join(root, "missing"),
		EmbedProvider: config.EmbedProviderOllama, EmbedModel: "test-model",
	})
	if err != nil {
		t.Fatalf("create MCP dependencies without worker: %v", err)
	}
	defer deps.Client.Close()
	if _, err := deps.Client.Status(context.Background(), "", 10); err == nil || !strings.Contains(err.Error(), "worker offline") {
		t.Fatalf("expected clear worker-offline error, got %v", err)
	}
}
