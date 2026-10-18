package jobs_test

import (
	"context"
	"testing"
	"time"

	"github.com/nicolasmmb/pinax/internal/adapter/turso"
	"github.com/nicolasmmb/pinax/internal/app/jobs"
	"github.com/nicolasmmb/pinax/internal/domain"
)

type testResolver struct{}

func (testResolver) ResolveInput(path string) (string, domain.InputKind, error) {
	return "/resolved" + path, domain.InputDir, nil
}

func TestServiceEnqueueStatusCancel(t *testing.T) {
	ctx := context.Background()
	store, err := turso.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service := jobs.NewService(store, testResolver{}, jobs.SnapshotConfig{
		EmbeddingModel: "test-model",
		ChunkerVersion: "rune-window-v2",
		ChunkSize:      1200,
		ChunkOverlap:   150,
	})

	job, deduplicated, err := service.Enqueue(ctx, "/docs")
	if err != nil || deduplicated {
		t.Fatalf("enqueue = %#v deduplicated=%v err=%v", job, deduplicated, err)
	}
	if job.InputPath != "/resolved/docs" || job.EmbeddingModel != "test-model" {
		t.Fatalf("unexpected job: %#v", job)
	}
	again, deduplicated, err := service.Enqueue(ctx, "/docs")
	if err != nil || !deduplicated || again.ID != job.ID {
		t.Fatalf("dedupe = %#v deduplicated=%v err=%v", again, deduplicated, err)
	}

	if err := store.PlanJobFiles(ctx, job.ID, []string{"/resolved/docs/a.md"}); err != nil {
		t.Fatal(err)
	}
	if _, err := store.StartJobFile(ctx, job.ID, "/resolved/docs/a.md"); err != nil {
		t.Fatal(err)
	}
	if err := store.SetJobFileChunks(ctx, job.ID, "/resolved/docs/a.md", 5); err != nil {
		t.Fatal(err)
	}
	status, err := service.Status(ctx, job.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(status.Jobs) != 1 || status.Jobs[0].ID != job.ID {
		t.Fatalf("unexpected status: %#v", status)
	}
	if status.Jobs[0].Progress.FilesTotal != 1 || len(status.Jobs[0].ActiveFiles) != 1 {
		t.Fatalf("unexpected snapshot: %#v", status.Jobs[0])
	}

	canceled, err := service.Cancel(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !canceled.CancelRequested || !canceled.Status.Terminal() && canceled.Status != domain.JobRunning {
		t.Fatalf("unexpected cancel state: %#v", canceled.Job)
	}

	list, err := service.Status(ctx, "", 10)
	if err != nil || len(list.Jobs) != 1 {
		t.Fatalf("list status = %#v, %v", list, err)
	}
	if list.Worker.Online {
		t.Fatalf("expected worker offline, got %#v", list.Worker)
	}
}

func TestServiceWorkerOnline(t *testing.T) {
	ctx := context.Background()
	store, err := turso.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	service := jobs.NewService(store, testResolver{}, jobs.SnapshotConfig{
		EmbeddingModel: "test-model",
		ChunkerVersion: "rune-window-v2",
		ChunkSize:      1200,
		ChunkOverlap:   150,
	})
	now := time.Now().UTC()
	lease := domain.WorkerLease{Name: "ingest", WorkerID: "worker", PID: 42, Hostname: "host", StartedAt: now, HeartbeatAt: now}
	if acquired, err := store.AcquireLease(ctx, lease, 30*time.Second); err != nil || !acquired {
		t.Fatalf("acquire lease = %v, %v", acquired, err)
	}
	status, err := service.Status(ctx, "", 10)
	if err != nil {
		t.Fatal(err)
	}
	if !status.Worker.Online || status.Worker.WorkerID != "worker" {
		t.Fatalf("unexpected worker status: %#v", status.Worker)
	}
}
