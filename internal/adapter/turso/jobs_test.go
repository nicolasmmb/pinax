package turso

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/nicolasmmb/pinax/internal/app/jobs"
	"github.com/nicolasmmb/pinax/internal/domain"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	store, err := Open(context.Background(), ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func enqueueTestJob(t *testing.T, store *Store, path string, kind domain.InputKind) domain.Job {
	t.Helper()
	job, _, err := store.EnqueueJob(context.Background(), domain.EnqueueRequest{
		InputPath:      path,
		InputKind:      kind,
		EmbeddingModel: "test-model",
		ChunkerVersion: "rune-window-v2",
		ChunkSize:      1200,
		ChunkOverlap:   150,
	})
	if err != nil {
		t.Fatal(err)
	}
	return job
}

func TestEnqueueDeduplicatesActiveInput(t *testing.T) {
	store := openTestStore(t)
	first := enqueueTestJob(t, store, "/docs", domain.InputDir)
	second, deduplicated, err := store.EnqueueJob(context.Background(), domain.EnqueueRequest{
		InputPath:      "/docs",
		InputKind:      domain.InputDir,
		EmbeddingModel: "test-model",
		ChunkerVersion: "rune-window-v2",
		ChunkSize:      1200,
		ChunkOverlap:   150,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !deduplicated || second.ID != first.ID {
		t.Fatalf("expected deduplicated job %s, got %#v deduplicated=%v", first.ID, second, deduplicated)
	}
	if _, err := store.GetJob(context.Background(), "missing"); !errors.Is(err, jobs.ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}
}

func TestPlanProgressAndFinalize(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	job := enqueueTestJob(t, store, "/docs", domain.InputDir)
	paths := []string{"/docs/a.md", "/docs/b.md", "/docs/c.md"}
	if err := store.PlanJobFiles(ctx, job.ID, paths); err != nil {
		t.Fatal(err)
	}
	planned, err := store.GetJob(ctx, job.ID)
	if err != nil || !planned.PlanningDone || planned.PlannedAt == nil {
		t.Fatalf("planning not persisted: %#v, %v", planned, err)
	}
	progress, err := store.JobProgress(ctx, job.ID)
	if err != nil || progress.FilesTotal != 3 || progress.FilesDone != 0 {
		t.Fatalf("unexpected progress: %#v, %v", progress, err)
	}
	if queued, err := store.QueuedFiles(ctx, job.ID); err != nil || len(queued) != 3 {
		t.Fatalf("queued files = %#v, %v", queued, err)
	}

	started, err := store.StartJobFile(ctx, job.ID, paths[0])
	if err != nil || !started {
		t.Fatalf("start file = %v, %v", started, err)
	}
	if startedAgain, err := store.StartJobFile(ctx, job.ID, paths[0]); err != nil || startedAgain {
		t.Fatalf("file started twice: %v, %v", startedAgain, err)
	}
	if err := store.SetJobFileStage(ctx, job.ID, paths[0], domain.StageEmbedding); err != nil {
		t.Fatal(err)
	}
	if err := store.SetJobFileChunks(ctx, job.ID, paths[0], 7); err != nil {
		t.Fatal(err)
	}
	active, err := store.ActiveFiles(ctx, job.ID)
	if err != nil || len(active) != 1 || active[0].Stage != domain.StageEmbedding {
		t.Fatalf("active files = %#v, %v", active, err)
	}
	if err := store.FinishJobFile(ctx, job.ID, paths[0], domain.FileIndexed, 7, ""); err != nil {
		t.Fatal(err)
	}
	if _, finalized, err := store.FinalizeJob(ctx, job.ID); err != nil || finalized {
		t.Fatalf("job finalized prematurely: %v, %v", finalized, err)
	}

	if _, err := store.StartJobFile(ctx, job.ID, paths[1]); err != nil {
		t.Fatal(err)
	}
	if err := store.FinishJobFile(ctx, job.ID, paths[1], domain.FileSkipped, 0, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartJobFile(ctx, job.ID, paths[2]); err != nil {
		t.Fatal(err)
	}
	if err := store.FinishJobFile(ctx, job.ID, paths[2], domain.FileFailed, 0, "broken"); err != nil {
		t.Fatal(err)
	}
	finalized, done, err := store.FinalizeJob(ctx, job.ID)
	if err != nil || !done {
		t.Fatalf("finalize = %v, %v", done, err)
	}
	if finalized.Status != domain.JobCompletedWithErrors {
		t.Fatalf("status = %s, want %s", finalized.Status, domain.JobCompletedWithErrors)
	}
	progress, err = store.JobProgress(ctx, job.ID)
	if err != nil || progress.Percent != 100 || progress.Failed != 1 || progress.Indexed != 1 || progress.Skipped != 1 || progress.Chunks != 7 {
		t.Fatalf("unexpected final progress: %#v, %v", progress, err)
	}
	failures, total, err := store.Failures(ctx, job.ID, 10)
	if err != nil || total != 1 || len(failures) != 1 || failures[0].Error != "broken" {
		t.Fatalf("failures = %#v total=%d err=%v", failures, total, err)
	}
}

func TestClaimIsExclusive(t *testing.T) {
	store := openTestStore(t)
	job := enqueueTestJob(t, store, "/docs", domain.InputDir)
	const workers = 8
	var wg sync.WaitGroup
	claimed := make(chan *domain.Job, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			claimedJob, err := store.ClaimNextJob(context.Background(), "worker-"+string(rune('a'+index)), time.Now().UTC())
			if err != nil {
				t.Errorf("claim: %v", err)
				return
			}
			if claimedJob != nil {
				claimed <- claimedJob
			}
		}(i)
	}
	wg.Wait()
	close(claimed)
	var got []*domain.Job
	for item := range claimed {
		got = append(got, item)
	}
	if len(got) != 1 || got[0].ID != job.ID {
		t.Fatalf("claimed %d jobs, want exactly one", len(got))
	}
	if err := store.HeartbeatJob(context.Background(), job.ID, "wrong-worker", time.Now()); !errors.Is(err, jobs.ErrJobNotOwned) {
		t.Fatalf("expected ErrJobNotOwned, got %v", err)
	}
}

func TestWorkerHeartbeatAndStaleRecovery(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	now := time.Now().UTC()
	info := domain.WorkerInfo{WorkerID: "worker-a", PID: 1, Hostname: "host", StartedAt: now, HeartbeatAt: now}
	if err := store.TouchWorker(ctx, info); err != nil {
		t.Fatal(err)
	}
	stored, err := store.WorkerInfo(ctx)
	if err != nil || stored == nil || stored.WorkerID != "worker-a" {
		t.Fatalf("worker info = %#v, %v", stored, err)
	}
	if err := store.ClearWorker(ctx); err != nil {
		t.Fatal(err)
	}
	if stored, err := store.WorkerInfo(ctx); err != nil || stored != nil {
		t.Fatalf("worker info after clear = %#v, %v", stored, err)
	}

	job := enqueueTestJob(t, store, "/docs", domain.InputDir)
	if err := store.PlanJobFiles(ctx, job.ID, []string{"/docs/a.md"}); err != nil {
		t.Fatal(err)
	}
	claimed, err := store.ClaimNextJob(ctx, "worker-b", now)
	if err != nil || claimed == nil {
		t.Fatalf("claim = %#v, %v", claimed, err)
	}
	if _, err := store.StartJobFile(ctx, job.ID, "/docs/a.md"); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecoverStaleJobs(ctx, now.Add(time.Minute), 30*time.Second); err != nil {
		t.Fatal(err)
	}
	recovered, err := store.GetJob(ctx, job.ID)
	if err != nil || recovered.Status != domain.JobQueued || recovered.WorkerID != "" {
		t.Fatalf("job not recovered: %#v, %v", recovered, err)
	}
	files, err := store.JobFiles(ctx, job.ID)
	if err != nil || len(files) != 1 || files[0].Status != domain.FileQueued || files[0].Stage != "" {
		t.Fatalf("files not recovered: %#v, %v", files, err)
	}
}

func TestCancelQueuedAndRunning(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	queued := enqueueTestJob(t, store, "/one", domain.InputFile)
	canceled, err := store.RequestCancel(ctx, queued.ID)
	if err != nil || canceled.Status != domain.JobCanceled || !canceled.CancelRequested {
		t.Fatalf("queued cancel = %#v, %v", canceled, err)
	}
	running := enqueueTestJob(t, store, "/two", domain.InputFile)
	if err := store.PlanJobFiles(ctx, running.ID, []string{"/two"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNextJob(ctx, "worker", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	requested, err := store.RequestCancel(ctx, running.ID)
	if err != nil || !requested.CancelRequested {
		t.Fatalf("running cancel = %#v, %v", requested, err)
	}
	flag, err := store.CancelRequested(ctx, running.ID)
	if err != nil || !flag {
		t.Fatalf("cancel flag = %v, %v", flag, err)
	}
	if err := store.CancelQueuedFiles(ctx, running.ID); err != nil {
		t.Fatal(err)
	}
	finalized, done, err := store.FinalizeJob(ctx, running.ID)
	if err != nil || !done || finalized.Status != domain.JobCanceled {
		t.Fatalf("finalize canceled = %#v done=%v err=%v", finalized, done, err)
	}
}

func TestRequeueAndPurge(t *testing.T) {
	ctx := context.Background()
	store := openTestStore(t)
	job := enqueueTestJob(t, store, "/docs", domain.InputDir)
	if err := store.PlanJobFiles(ctx, job.ID, []string{"/docs/a.md"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimNextJob(ctx, "worker", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartJobFile(ctx, job.ID, "/docs/a.md"); err != nil {
		t.Fatal(err)
	}
	if err := store.RequeueJob(ctx, job.ID, "worker"); err != nil {
		t.Fatal(err)
	}
	requeued, err := store.GetJob(ctx, job.ID)
	if err != nil || requeued.Status != domain.JobQueued {
		t.Fatalf("requeued job = %#v, %v", requeued, err)
	}
	files, err := store.JobFiles(ctx, job.ID)
	if err != nil || files[0].Status != domain.FileQueued {
		t.Fatalf("requeued files = %#v, %v", files, err)
	}

	if _, err := store.ClaimNextJob(ctx, "worker", time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartJobFile(ctx, job.ID, "/docs/a.md"); err != nil {
		t.Fatal(err)
	}
	if err := store.FinishJobFile(ctx, job.ID, "/docs/a.md", domain.FileIndexed, 2, ""); err != nil {
		t.Fatal(err)
	}
	if _, done, err := store.FinalizeJob(ctx, job.ID); err != nil || !done {
		t.Fatalf("finalize = %v, %v", done, err)
	}
	active := enqueueTestJob(t, store, "/active", domain.InputFile)
	purged, err := store.PurgeJobs(ctx, time.Now().UTC().Add(time.Hour))
	if err != nil || purged != 1 {
		t.Fatalf("purged = %d, %v", purged, err)
	}
	if _, err := store.GetJob(ctx, job.ID); !errors.Is(err, jobs.ErrJobNotFound) {
		t.Fatalf("expected purged job to be gone, got %v", err)
	}
	if _, err := store.GetJob(ctx, active.ID); err != nil {
		t.Fatalf("active job was purged: %v", err)
	}
}

func TestJobsPersistAcrossStores(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "pinax.db")
	first, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	job := enqueueTestJob(t, first, "/docs", domain.InputDir)
	second, err := Open(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	found, err := second.GetJob(ctx, job.ID)
	if err != nil || found.ID != job.ID {
		t.Fatalf("job not visible in second store: %#v, %v", found, err)
	}
	claimed, err := second.ClaimNextJob(ctx, "worker", time.Now().UTC())
	if err != nil || claimed == nil || claimed.ID != job.ID {
		t.Fatalf("claim from second store = %#v, %v", claimed, err)
	}
}
