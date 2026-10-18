package turso

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nicolasmmb/pinax/internal/domain"
)

func testDocument(path string, chunks int) (domain.Document, []domain.Chunk) {
	doc := domain.Document{
		Path:           path,
		Format:         "md",
		SHA256:         path,
		EmbeddingModel: "test",
		ChunkerVersion: "rune-window-v2",
		ChunkSize:      1200,
		ChunkOverlap:   150,
		IndexedAt:      time.Now().UTC(),
	}
	result := make([]domain.Chunk, chunks)
	for i := range result {
		result[i] = domain.Chunk{
			ID:          fmt.Sprintf("%s-%d", path, i),
			DocumentID:  path,
			Ordinal:     i,
			Page:        1,
			Text:        fmt.Sprintf("chunk %d of %s", i, path),
			Hash:        fmt.Sprintf("%s-%d", path, i),
			StartOffset: i,
			EndOffset:   i + 1,
			CharCount:   1,
			Dimensions:  2,
			Embedding:   []float32{1, 0},
		}
	}
	return doc, result
}

// TestConcurrentReadWriteSameFile guards the multi-process design: the serve
// process reads while the worker writes, both on the same embedded database.
func TestConcurrentReadWriteSameFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pinax.db")
	ctx := context.Background()
	writer, err := OpenWithOptions(ctx, Options{Path: path, MaxOpenConns: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	reader, err := OpenWithOptions(ctx, Options{Path: path, MaxOpenConns: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	const writes = 40
	const chunksPerWrite = 40
	var (
		wg       sync.WaitGroup
		stop     = make(chan struct{})
		searches atomic.Int64
		writeErr error
		readErr  error
		mu       sync.Mutex
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(stop)
		for i := 0; i < writes; i++ {
			doc, chunks := testDocument(fmt.Sprintf("doc-%02d.md", i), chunksPerWrite)
			if err := writer.ReplaceDocument(ctx, doc, chunks); err != nil {
				mu.Lock()
				writeErr = err
				mu.Unlock()
				return
			}
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if _, err := reader.Search(ctx, []float32{1, 0}, "test", 5); err != nil {
				mu.Lock()
				readErr = err
				mu.Unlock()
				return
			}
			searches.Add(1)
		}
	}()

	wg.Wait()
	if writeErr != nil {
		t.Fatalf("concurrent write failed: %v", writeErr)
	}
	if readErr != nil {
		t.Fatalf("concurrent read failed: %v", readErr)
	}
	if searches.Load() == 0 {
		t.Fatal("reader did not run")
	}

	results, err := reader.Search(ctx, []float32{1, 0}, "test", writes*chunksPerWrite)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != writes*chunksPerWrite {
		t.Fatalf("found %d chunks, want %d", len(results), writes*chunksPerWrite)
	}
}

func TestConcurrentWritersSameFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pinax.db")
	ctx := context.Background()
	first, err := OpenWithOptions(ctx, Options{Path: path, MaxOpenConns: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	second, err := OpenWithOptions(ctx, Options{Path: path, MaxOpenConns: 8})
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()

	const perWriter = 20
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for i, store := range []*Store{first, second} {
		wg.Add(1)
		go func(index int, store *Store) {
			defer wg.Done()
			for j := 0; j < perWriter; j++ {
				doc, chunks := testDocument(fmt.Sprintf("writer-%d-doc-%02d.md", index, j), 20)
				if err := store.ReplaceDocument(ctx, doc, chunks); err != nil {
					errs <- err
					return
				}
			}
		}(i, store)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent writer failed: %v", err)
	}
	results, err := first.Search(ctx, []float32{1, 0}, "test", perWriter*20*2)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != perWriter*20*2 {
		t.Fatalf("found %d chunks, want %d", len(results), perWriter*20*2)
	}
}
