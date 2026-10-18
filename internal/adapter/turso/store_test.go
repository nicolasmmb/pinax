package turso

import (
	"context"
	"testing"
	"time"

	"github.com/nicolasmmb/pinax/internal/domain"
)

func TestReplaceAndVectorSearch(t *testing.T) {
	ctx := context.Background()
	store, err := Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	for i, item := range []struct {
		text string
		vec  []float32
	}{{"aligned", []float32{1, 0}}, {"different", []float32{0, 1}}} {
		path := string(rune('a'+i)) + ".md"
		doc := domain.Document{
			Path:           path,
			Format:         "md",
			SHA256:         path,
			EmbeddingModel: "test",
			ChunkerVersion: "rune-window-v2",
			ChunkSize:      1200,
			ChunkOverlap:   150,
			IndexedAt:      time.Now(),
		}
		chunk := domain.Chunk{
			ID:          path,
			DocumentID:  path,
			Ordinal:     0,
			Page:        1,
			Text:        item.text,
			Hash:        path + "-hash",
			StartOffset: 0,
			EndOffset:   len(item.text),
			CharCount:   len(item.text),
			Dimensions:  len(item.vec),
			Embedding:   item.vec,
		}
		if err := store.ReplaceDocument(ctx, doc, []domain.Chunk{chunk}); err != nil {
			t.Fatal(err)
		}
	}
	results, err := store.Search(ctx, []float32{1, 0}, "test", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 || results[0].Text != "aligned" {
		t.Fatalf("unexpected vector results: %#v", results)
	}
	first := results[0]
	if first.Hash == "" || first.Dimensions != 2 || first.Page != 1 || first.CharCount == 0 {
		t.Fatalf("chunk metadata missing: %#v", first)
	}
	state, err := store.DocumentState(ctx, "a.md")
	if err != nil || state.SHA256 != "a.md" || state.EmbeddingModel != "test" {
		t.Fatalf("document state = %#v, %v", state, err)
	}
	if state.ChunkerVersion != "rune-window-v2" || state.ChunkSize != 1200 || state.ChunkOverlap != 150 {
		t.Fatalf("chunker metadata missing: %#v", state)
	}
}
