package ipc

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"github.com/nicolasmmb/pinax/internal/adapter/turso"
	"github.com/nicolasmmb/pinax/internal/app/jobs"
	"github.com/nicolasmmb/pinax/internal/domain"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestClientServerRoundTrip(t *testing.T) {
	ctx := context.Background()
	store, err := turso.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	socketPath := SocketPath(filepath.Join(t.TempDir(), "pinax.db"))
	server, err := Listen(socketPath, store, store, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer server.Close()
	go func() { _ = server.Serve(ctx) }()

	client, err := DialWithRetry(ctx, socketPath, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	job, deduplicated, err := client.EnqueueJob(ctx, domain.EnqueueRequest{
		InputPath:      "/docs",
		InputKind:      domain.InputDir,
		EmbeddingModel: "model",
		ChunkerVersion: "v2",
		ChunkSize:      1200,
		ChunkOverlap:   150,
	})
	if err != nil || deduplicated {
		t.Fatalf("enqueue = %#v deduplicated=%v err=%v", job, deduplicated, err)
	}
	status, err := client.Status(ctx, job.ID, 10)
	if err != nil || len(status.Jobs) != 1 || status.Jobs[0].ID != job.ID {
		t.Fatalf("status = %#v, %v", status, err)
	}
	if _, err := client.Status(ctx, "missing", 10); !errors.Is(err, jobs.ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound, got %v", err)
	}
	canceled, err := client.Cancel(ctx, job.ID)
	if err != nil || canceled.Status != domain.JobCanceled {
		t.Fatalf("cancel = %#v, %v", canceled, err)
	}

	doc := domain.Document{
		Path:           "/docs/a.md",
		Format:         "md",
		SHA256:         "hash",
		EmbeddingModel: "model",
		ChunkerVersion: "v2",
		ChunkSize:      1200,
		ChunkOverlap:   150,
		IndexedAt:      time.Now().UTC(),
	}
	chunk := domain.Chunk{
		ID:          "chunk",
		DocumentID:  "/docs/a.md",
		Ordinal:     0,
		Page:        1,
		Text:        "texto",
		Hash:        "chunk-hash",
		StartOffset: 0,
		EndOffset:   5,
		CharCount:   5,
		Dimensions:  2,
		Embedding:   []float32{1, 0},
	}
	if err := store.ReplaceDocument(ctx, doc, []domain.Chunk{chunk}); err != nil {
		t.Fatal(err)
	}
	results, err := client.Search(ctx, []float32{1, 0}, "model", 5)
	if err != nil || len(results) != 1 || results[0].Text != "texto" {
		t.Fatalf("search = %#v, %v", results, err)
	}
}

func TestClientReconnectsAfterWorkerRestart(t *testing.T) {
	ctx := context.Background()
	store, err := turso.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	socketPath := SocketPath(filepath.Join(t.TempDir(), "pinax.db"))
	first, err := Listen(socketPath, store, store, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		_ = first.Serve(ctx)
	}()
	client, err := DialWithRetry(ctx, socketPath, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, err := client.Status(ctx, "", 10); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	<-serveDone
	if _, err := client.Status(ctx, "", 10); err == nil {
		t.Fatal("expected call to fail while worker is down")
	}

	second, err := Listen(socketPath, store, store, testLogger())
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	go func() { _ = second.Serve(ctx) }()
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := client.Status(ctx, "", 10)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("client did not reconnect: %v", err)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
