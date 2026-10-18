package jobs_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nicolasmmb/pinax/internal/adapter/turso"
	"github.com/nicolasmmb/pinax/internal/app/ingest"
	"github.com/nicolasmmb/pinax/internal/app/jobs"
	"github.com/nicolasmmb/pinax/internal/domain"
)

type fakeIngester struct {
	mu          sync.Mutex
	started     chan string
	planStarted chan string
	planRelease chan struct{}
	release     chan struct{}
	active      int
	maxActive   int
	paths       map[string][]string
	blocking    bool
	delay       time.Duration
	fail        map[string]bool
}

func newFakeIngester(paths map[string][]string) *fakeIngester {
	return &fakeIngester{
		started: make(chan string, 64),
		paths:   paths,
	}
}

func (f *fakeIngester) Model() string { return "test-model" }
func (f *fakeIngester) Chunker() (string, int, int) {
	return "rune-window-v2", 1200, 150
}
func (f *fakeIngester) ResolveInput(path string) (string, domain.InputKind, error) {
	return path, domain.InputDir, nil
}
func (f *fakeIngester) Plan(ctx context.Context, path string) ([]string, error) {
	if f.planStarted != nil {
		select {
		case f.planStarted <- path:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		select {
		case <-f.planRelease:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.paths[path]...), nil
}
func (f *fakeIngester) IndexFile(ctx context.Context, path string, hooks ingest.ProgressHooks) (ingest.FileResult, error) {
	if f.delay > 0 {
		select {
		case <-time.After(f.delay):
		case <-ctx.Done():
			return ingest.FileResult{Status: domain.FileCanceled, Err: ctx.Err()}, ctx.Err()
		}
	}
	f.mu.Lock()
	f.active++
	if f.active > f.maxActive {
		f.maxActive = f.active
	}
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.active--
		f.mu.Unlock()
	}()
	hooks.OnStage(domain.StageEmbedding)
	hooks.OnChunks(2)
	f.started <- path
	if f.blocking {
		select {
		case <-f.release:
		case <-ctx.Done():
			return ingest.FileResult{Status: domain.FileCanceled, Err: ctx.Err()}, ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return ingest.FileResult{Status: domain.FileCanceled, Err: err}, err
	}
	f.mu.Lock()
	failing := f.fail[path]
	f.mu.Unlock()
	if failing {
		err := errors.New("forced failure")
		return ingest.FileResult{Status: domain.FileFailed, Err: err}, err
	}
	return ingest.FileResult{Status: domain.FileIndexed, Chunks: 2}, nil
}

func (f *fakeIngester) max() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.maxActive
}

func testWorkerStore(t *testing.T) *turso.Store {
	t.Helper()
	store, err := turso.Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func enqueueWorkerJob(t *testing.T, store *turso.Store, path string, paths []string, model string) domain.Job {
	t.Helper()
	ctx := context.Background()
	job, _, err := store.EnqueueJob(ctx, domain.EnqueueRequest{
		InputPath:      path,
		InputKind:      domain.InputDir,
		EmbeddingModel: model,
		ChunkerVersion: "rune-window-v2",
		ChunkSize:      1200,
		ChunkOverlap:   150,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.PlanJobFiles(ctx, job.ID, paths); err != nil {
		t.Fatal(err)
	}
	return job
}

func startWorker(t *testing.T, store *turso.Store, ingester jobs.Ingester, cfg jobs.WorkerConfig) (context.CancelFunc, <-chan error) {
	t.Helper()
	worker, err := jobs.NewWorker(store, ingester, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("worker did not stop")
		}
	})
	return cancel, done
}

func waitFor(t *testing.T, timeout time.Duration, message string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", message)
}

func TestWorkerProcessesFilesInParallel(t *testing.T) {
	ctx := context.Background()
	store := testWorkerStore(t)
	paths := []string{"/docs/a.md", "/docs/b.md", "/docs/c.md", "/docs/d.md", "/docs/e.md", "/docs/f.md"}
	job := enqueueWorkerJob(t, store, "/docs", paths, "test-model")
	ingester := newFakeIngester(map[string][]string{"/docs": paths})
	ingester.blocking = true
	ingester.release = make(chan struct{})
	startWorker(t, store, ingester, jobs.WorkerConfig{Concurrency: 4})

	for i := 0; i < 4; i++ {
		select {
		case <-ingester.started:
		case <-time.After(5 * time.Second):
			t.Fatal("worker did not start 4 files in parallel")
		}
	}
	if got := ingester.max(); got < 4 {
		t.Fatalf("max concurrency = %d, want 4", got)
	}
	files, err := store.JobFiles(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	running := 0
	for _, file := range files {
		if file.Status == domain.FileRunning {
			running++
			if file.Stage != domain.StageEmbedding || file.Chunks != 2 {
				t.Fatalf("progress not persisted for %s: %#v", file.Path, file)
			}
		}
	}
	if running != 4 {
		t.Fatalf("running files = %d, want 4", running)
	}
	close(ingester.release)
	waitFor(t, 10*time.Second, "job completion", func() bool {
		stored, err := store.GetJob(ctx, job.ID)
		return err == nil && stored.Status == domain.JobCompleted
	})
}

func TestWorkerRunsMultipleJobs(t *testing.T) {
	ctx := context.Background()
	store := testWorkerStore(t)
	first := enqueueWorkerJob(t, store, "/one", []string{"/one/a.md"}, "test-model")
	second := enqueueWorkerJob(t, store, "/two", []string{"/two/a.md"}, "test-model")
	ingester := newFakeIngester(map[string][]string{
		"/one": {"/one/a.md"},
		"/two": {"/two/a.md"},
	})
	ingester.blocking = true
	ingester.release = make(chan struct{})
	startWorker(t, store, ingester, jobs.WorkerConfig{Concurrency: 2})

	seen := map[string]bool{}
	for i := 0; i < 2; i++ {
		select {
		case path := <-ingester.started:
			seen[path] = true
		case <-time.After(5 * time.Second):
			t.Fatalf("only saw paths %v", seen)
		}
	}
	if !seen["/one/a.md"] || !seen["/two/a.md"] {
		t.Fatalf("jobs did not run concurrently: %v", seen)
	}
	close(ingester.release)
	for _, job := range []domain.Job{first, second} {
		jobID := job.ID
		waitFor(t, 10*time.Second, "job completion", func() bool {
			stored, err := store.GetJob(ctx, jobID)
			return err == nil && stored.Status == domain.JobCompleted
		})
	}
}

func TestWorkerCancelsRunningJob(t *testing.T) {
	ctx := context.Background()
	store := testWorkerStore(t)
	paths := []string{"/docs/a.md", "/docs/b.md", "/docs/c.md"}
	job := enqueueWorkerJob(t, store, "/docs", paths, "test-model")
	ingester := newFakeIngester(map[string][]string{"/docs": paths})
	ingester.blocking = true
	ingester.release = make(chan struct{})
	startWorker(t, store, ingester, jobs.WorkerConfig{Concurrency: 2})

	select {
	case <-ingester.started:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not start")
	}
	if _, err := store.RequestCancel(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "job cancellation", func() bool {
		stored, err := store.GetJob(ctx, job.ID)
		return err == nil && stored.Status == domain.JobCanceled
	})
	files, err := store.JobFiles(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if file.Status != domain.FileCanceled {
			t.Fatalf("file %s status = %s, want canceled", file.Path, file.Status)
		}
	}
}

func TestWorkerIsolatesFileFailures(t *testing.T) {
	ctx := context.Background()
	store := testWorkerStore(t)
	paths := []string{"/docs/a.md", "/docs/b.md", "/docs/c.md"}
	job := enqueueWorkerJob(t, store, "/docs", paths, "test-model")
	ingester := newFakeIngester(map[string][]string{"/docs": paths})
	ingester.fail = map[string]bool{"/docs/b.md": true}
	startWorker(t, store, ingester, jobs.WorkerConfig{Concurrency: 2})
	waitFor(t, 10*time.Second, "job with errors", func() bool {
		stored, err := store.GetJob(ctx, job.ID)
		return err == nil && stored.Status == domain.JobCompletedWithErrors
	})
	progress, err := store.JobProgress(ctx, job.ID)
	if err != nil || progress.Indexed != 2 || progress.Failed != 1 {
		t.Fatalf("progress = %#v, %v", progress, err)
	}
	failures, total, err := store.Failures(ctx, job.ID, 10)
	if err != nil || total != 1 || failures[0].Error != "forced failure" {
		t.Fatalf("failures = %#v total=%d err=%v", failures, total, err)
	}
}

func TestWorkerFailsOnConfigMismatch(t *testing.T) {
	ctx := context.Background()
	store := testWorkerStore(t)
	job := enqueueWorkerJob(t, store, "/docs", []string{"/docs/a.md"}, "other-model")
	ingester := newFakeIngester(map[string][]string{"/docs": {"/docs/a.md"}})
	startWorker(t, store, ingester, jobs.WorkerConfig{Concurrency: 1})
	waitFor(t, 5*time.Second, "job failure", func() bool {
		stored, err := store.GetJob(ctx, job.ID)
		return err == nil && stored.Status == domain.JobFailed
	})
	stored, err := store.GetJob(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Error == "" {
		t.Fatal("expected config mismatch error")
	}
}

func TestWorkerRecoversStaleJob(t *testing.T) {
	ctx := context.Background()
	store := testWorkerStore(t)
	paths := []string{"/docs/a.md", "/docs/b.md"}
	job := enqueueWorkerJob(t, store, "/docs", paths, "test-model")
	stale := time.Now().UTC().Add(-time.Hour)
	if claimed, err := store.ClaimNextJob(ctx, "dead-worker", stale); err != nil || claimed == nil {
		t.Fatalf("claim stale = %#v, %v", claimed, err)
	}
	if _, err := store.StartJobFile(ctx, job.ID, paths[0]); err != nil {
		t.Fatal(err)
	}
	ingester := newFakeIngester(map[string][]string{"/docs": paths})
	startWorker(t, store, ingester, jobs.WorkerConfig{Concurrency: 2})
	waitFor(t, 10*time.Second, "recovered job completion", func() bool {
		stored, err := store.GetJob(ctx, job.ID)
		return err == nil && stored.Status == domain.JobCompleted
	})
	progress, err := store.JobProgress(ctx, job.ID)
	if err != nil || progress.Indexed != 2 {
		t.Fatalf("progress = %#v, %v", progress, err)
	}
}

func TestWorkerRequeuesRunningFilesOnShutdown(t *testing.T) {
	ctx := context.Background()
	store := testWorkerStore(t)
	paths := []string{"/docs/a.md", "/docs/b.md"}
	job := enqueueWorkerJob(t, store, "/docs", paths, "test-model")
	ingester := newFakeIngester(map[string][]string{"/docs": paths})
	ingester.blocking = true
	ingester.release = make(chan struct{})
	worker, err := jobs.NewWorker(store, ingester, jobs.WorkerConfig{Concurrency: 1})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan error, 1)
	go func() { done <- worker.Run(runCtx) }()
	select {
	case <-ingester.started:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not start")
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
	stored, err := store.GetJob(ctx, job.ID)
	if err != nil || stored.Status != domain.JobQueued {
		t.Fatalf("job after shutdown = %#v, %v", stored, err)
	}
	files, err := store.JobFiles(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if file.Status != domain.FileQueued {
			t.Fatalf("file after shutdown = %s, want queued", file.Status)
		}
	}

	fast := newFakeIngester(map[string][]string{"/docs": paths})
	second, err := jobs.NewWorker(store, fast, jobs.WorkerConfig{Concurrency: 2, Once: true})
	if err != nil {
		t.Fatal(err)
	}
	onceCtx, onceCancel := context.WithTimeout(ctx, 10*time.Second)
	defer onceCancel()
	if err := second.Run(onceCtx); err != nil {
		t.Fatal(err)
	}
	final, err := store.GetJob(ctx, job.ID)
	if err != nil || final.Status != domain.JobCompleted {
		t.Fatalf("job after resume = %#v, %v", final, err)
	}
}

func TestWorkerOnceDrainsQueue(t *testing.T) {
	ctx := context.Background()
	store := testWorkerStore(t)
	paths := []string{"/docs/a.md", "/docs/b.md"}
	job := enqueueWorkerJob(t, store, "/docs", paths, "test-model")
	ingester := newFakeIngester(map[string][]string{"/docs": paths})
	worker, err := jobs.NewWorker(store, ingester, jobs.WorkerConfig{Concurrency: 2, Once: true})
	if err != nil {
		t.Fatal(err)
	}
	runCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := worker.Run(runCtx); err != nil {
		t.Fatal(err)
	}
	stored, err := store.GetJob(ctx, job.ID)
	if err != nil || stored.Status != domain.JobCompleted {
		t.Fatalf("job = %#v, %v", stored, err)
	}
}

func TestCancelDuringPlanning(t *testing.T) {
	ctx := context.Background()
	store := testWorkerStore(t)
	paths := []string{"/docs/a.md", "/docs/b.md"}
	job := enqueueWorkerJob(t, store, "/docs", paths, "test-model")
	ingester := newFakeIngester(map[string][]string{"/docs": paths})
	ingester.planStarted = make(chan string, 1)
	ingester.planRelease = make(chan struct{})
	defer close(ingester.planRelease)
	startWorker(t, store, ingester, jobs.WorkerConfig{Concurrency: 2})

	select {
	case <-ingester.planStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not start planning")
	}
	if _, err := store.RequestCancel(ctx, job.ID); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 5*time.Second, "job cancellation", func() bool {
		stored, err := store.GetJob(ctx, job.ID)
		return err == nil && stored.Status == domain.JobCanceled
	})
	files, err := store.JobFiles(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range files {
		if file.Status != domain.FileCanceled {
			t.Fatalf("file %s status = %s, want canceled", file.Path, file.Status)
		}
	}
}

func TestSmallJobIsNotStarved(t *testing.T) {
	ctx := context.Background()
	store := testWorkerStore(t)
	largePaths := make([]string, 20)
	for i := range largePaths {
		largePaths[i] = fmt.Sprintf("/large/%02d.md", i)
	}
	large := enqueueWorkerJob(t, store, "/large", largePaths, "test-model")
	small := enqueueWorkerJob(t, store, "/small", []string{"/small/one.md"}, "test-model")
	ingester := newFakeIngester(map[string][]string{
		"/large": largePaths,
		"/small": {"/small/one.md"},
	})
	ingester.delay = 15 * time.Millisecond
	startWorker(t, store, ingester, jobs.WorkerConfig{Concurrency: 4})

	waitFor(t, 10*time.Second, "small job completion", func() bool {
		stored, err := store.GetJob(ctx, small.ID)
		return err == nil && stored.Status == domain.JobCompleted
	})
	stored, err := store.GetJob(ctx, large.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Status == domain.JobCompleted {
		t.Fatal("large job completed before the small job could progress")
	}
	waitFor(t, 20*time.Second, "large job completion", func() bool {
		stored, err := store.GetJob(ctx, large.ID)
		return err == nil && stored.Status == domain.JobCompleted
	})
}

func TestResumePreservesTerminalFiles(t *testing.T) {
	ctx := context.Background()
	store := testWorkerStore(t)
	paths := []string{"/docs/a.md", "/docs/b.md"}
	job := enqueueWorkerJob(t, store, "/docs", paths, "test-model")
	claimed, err := store.ClaimNextJob(ctx, "old-worker", time.Now().UTC())
	if err != nil || claimed == nil {
		t.Fatalf("claim = %#v, %v", claimed, err)
	}
	if _, err := store.StartJobFile(ctx, job.ID, paths[0]); err != nil {
		t.Fatal(err)
	}
	if err := store.FinishJobFile(ctx, job.ID, paths[0], domain.FileIndexed, 3, ""); err != nil {
		t.Fatal(err)
	}
	if err := store.RequeueJob(ctx, job.ID, "old-worker"); err != nil {
		t.Fatal(err)
	}

	ingester := newFakeIngester(map[string][]string{"/docs": paths})
	worker, err := jobs.NewWorker(store, ingester, jobs.WorkerConfig{Concurrency: 2, Once: true})
	if err != nil {
		t.Fatal(err)
	}
	if err := worker.Run(ctx); err != nil {
		t.Fatal(err)
	}
	stored, err := store.GetJob(ctx, job.ID)
	if err != nil || stored.Status != domain.JobCompleted {
		t.Fatalf("job = %#v, %v", stored, err)
	}
	seen := map[string]bool{}
	for {
		select {
		case path := <-ingester.started:
			seen[path] = true
			continue
		default:
		}
		break
	}
	if seen[paths[0]] {
		t.Fatalf("terminal file was indexed again: %v", seen)
	}
	if !seen[paths[1]] {
		t.Fatalf("queued file was not indexed: %v", seen)
	}
}
