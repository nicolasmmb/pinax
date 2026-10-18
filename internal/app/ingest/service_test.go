package ingest

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/nicolasmmb/pinax/internal/domain"
)

type testExtractor struct{}

func (testExtractor) Supports(string) bool { return true }
func (testExtractor) Extract(_ context.Context, path string) ([]domain.Page, error) {
	b, err := os.ReadFile(path)
	return []domain.Page{{Number: 1, Text: string(b)}}, err
}

type testEmbedder struct{ model string }

func (e testEmbedder) Model() string { return e.model }
func (testEmbedder) EmbedDocuments(_ context.Context, texts []string) ([][]float32, error) {
	vectors := make([][]float32, len(texts))
	for i := range vectors {
		vectors[i] = []float32{1, 0}
	}
	return vectors, nil
}

type testRepository struct {
	state        domain.DocumentState
	chunks       []domain.Chunk
	replacements int
}

func (r *testRepository) DocumentState(context.Context, string) (domain.DocumentState, error) {
	if r.state.SHA256 == "" {
		return domain.DocumentState{}, ErrNotFound
	}
	return r.state, nil
}
func (r *testRepository) ReplaceDocument(_ context.Context, doc domain.Document, chunks []domain.Chunk) error {
	r.state = domain.DocumentState{
		SHA256:         doc.SHA256,
		EmbeddingModel: doc.EmbeddingModel,
		ChunkerVersion: doc.ChunkerVersion,
		ChunkSize:      doc.ChunkSize,
		ChunkOverlap:   doc.ChunkOverlap,
	}
	r.chunks = chunks
	r.replacements++
	return nil
}

func TestIndexSkipsUnchangedAndReembedsOnModelChange(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "notes.md")
	if err := os.WriteFile(path, []byte("some useful content"), 0o600); err != nil {
		t.Fatal(err)
	}
	repo := &testRepository{}
	service, err := New(root, []Extractor{testExtractor{}}, testEmbedder{"model-a"}, repo)
	if err != nil {
		t.Fatal(err)
	}
	first, err := service.Index(context.Background(), path)
	if err != nil || first.Indexed != 1 || repo.replacements != 1 {
		t.Fatalf("first index = %#v, replacements %d, error %v", first, repo.replacements, err)
	}
	second, err := service.Index(context.Background(), path)
	if err != nil || second.Skipped != 1 || repo.replacements != 1 {
		t.Fatalf("repeat index = %#v, replacements %d, error %v", second, repo.replacements, err)
	}
	service, err = New(root, []Extractor{testExtractor{}}, testEmbedder{"model-b"}, repo)
	if err != nil {
		t.Fatal(err)
	}
	third, err := service.Index(context.Background(), path)
	if err != nil || third.Indexed != 1 || repo.replacements != 2 {
		t.Fatalf("model change index = %#v, replacements %d, error %v", third, repo.replacements, err)
	}
}

func TestIndexReembedsOnChunkerChange(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "notes.md")
	if err := os.WriteFile(path, []byte("some useful content"), 0o600); err != nil {
		t.Fatal(err)
	}
	repo := &testRepository{}
	service, err := New(root, []Extractor{testExtractor{}}, testEmbedder{"model"}, repo)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Index(context.Background(), path); err != nil {
		t.Fatal(err)
	}
	service.chunkerVersion = "rune-window-v3"
	report, err := service.Index(context.Background(), path)
	if err != nil || report.Indexed != 1 || repo.replacements != 2 {
		t.Fatalf("chunker change index = %#v, replacements %d, error %v", report, repo.replacements, err)
	}
}

func TestIndexRejectsPathOutsideRoot(t *testing.T) {
	root := t.TempDir()
	outside := filepath.Join(t.TempDir(), "secret.md")
	if err := os.WriteFile(outside, []byte("private"), 0o600); err != nil {
		t.Fatal(err)
	}
	service, err := New(root, []Extractor{testExtractor{}}, testEmbedder{"model"}, &testRepository{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.Index(context.Background(), outside); err == nil {
		t.Fatal("expected path outside root to be rejected")
	}
}

func TestIndexSavesChunkMetadata(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "notes.md")
	content := "  um dois três quatro cinco seis  "
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	repo := &testRepository{}
	service, err := New(root, []Extractor{testExtractor{}}, testEmbedder{"model"}, repo)
	if err != nil {
		t.Fatal(err)
	}
	service.chunkSize = 12
	service.chunkOverlap = 3
	if _, err := service.Index(context.Background(), path); err != nil {
		t.Fatal(err)
	}
	if len(repo.chunks) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(repo.chunks))
	}
	runes := []rune(content)
	for i, chunk := range repo.chunks {
		if chunk.Ordinal != i {
			t.Fatalf("ordinal = %d, want %d", chunk.Ordinal, i)
		}
		if chunk.Page != 1 {
			t.Fatalf("page = %d, want 1", chunk.Page)
		}
		if chunk.Hash != hashText(chunk.Text) {
			t.Fatalf("hash does not match chunk text")
		}
		if chunk.Dimensions != 2 {
			t.Fatalf("dimensions = %d, want 2", chunk.Dimensions)
		}
		if chunk.CharCount != len([]rune(chunk.Text)) {
			t.Fatalf("char count = %d, text runes = %d", chunk.CharCount, len([]rune(chunk.Text)))
		}
		if got := string(runes[chunk.StartOffset:chunk.EndOffset]); got != chunk.Text {
			t.Fatalf("offsets select %q, want %q", got, chunk.Text)
		}
		if chunk.ID != chunkID(chunk.DocumentID, chunk.Hash, chunk.Page, chunk.StartOffset) {
			t.Fatalf("unexpected chunk id %q", chunk.ID)
		}
	}
}

type varyingEmbedder struct{}

func (varyingEmbedder) Model() string { return "model" }
func (varyingEmbedder) EmbedDocuments(_ context.Context, texts []string) ([][]float32, error) {
	vectors := make([][]float32, len(texts))
	for i := range vectors {
		if i == 0 {
			vectors[i] = []float32{1, 0}
		} else {
			vectors[i] = []float32{1, 0, 0}
		}
	}
	return vectors, nil
}

func TestIndexRejectsInconsistentDimensions(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "notes.md")
	if err := os.WriteFile(path, []byte("um dois três quatro cinco seis"), 0o600); err != nil {
		t.Fatal(err)
	}
	service, err := New(root, []Extractor{testExtractor{}}, varyingEmbedder{}, &testRepository{})
	if err != nil {
		t.Fatal(err)
	}
	service.chunkSize = 12
	service.chunkOverlap = 3
	report, err := service.Index(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Failures) == 0 {
		t.Fatal("expected inconsistent dimensions to be rejected")
	}
}

type recordingEmbedder struct {
	batches []int
}

func (e *recordingEmbedder) Model() string { return "model" }
func (e *recordingEmbedder) EmbedDocuments(_ context.Context, texts []string) ([][]float32, error) {
	e.batches = append(e.batches, len(texts))
	vectors := make([][]float32, len(texts))
	for i := range vectors {
		vectors[i] = []float32{1, 0}
	}
	return vectors, nil
}

func TestIndexFileReportsStagesBatchesAndChunks(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "notes.md")
	content := strings.Repeat("documento de teste com varias palavras ", 40)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	repo := &testRepository{}
	embedder := &recordingEmbedder{}
	service, err := New(root, []Extractor{testExtractor{}}, embedder, repo)
	if err != nil {
		t.Fatal(err)
	}
	service.chunkSize = 20
	service.chunkOverlap = 0
	service.embedBatchSize = 3
	var (
		stages []domain.Stage
		chunks []int
	)
	result, err := service.IndexFile(context.Background(), path, ProgressHooks{
		OnStage: func(stage domain.Stage) { stages = append(stages, stage) },
		OnChunks: func(done int) {
			chunks = append(chunks, done)
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Status != domain.FileIndexed || result.Chunks == 0 {
		t.Fatalf("unexpected result: %#v", result)
	}
	wantStages := []domain.Stage{
		domain.StageHashing,
		domain.StageExtracting,
		domain.StageChunking,
		domain.StageEmbedding,
		domain.StageSaving,
	}
	if len(stages) != len(wantStages) {
		t.Fatalf("stages = %#v, want %#v", stages, wantStages)
	}
	for i := range wantStages {
		if stages[i] != wantStages[i] {
			t.Fatalf("stages = %#v, want %#v", stages, wantStages)
		}
	}
	if len(embedder.batches) < 2 {
		t.Fatalf("expected multiple embedding batches, got %#v", embedder.batches)
	}
	for _, size := range embedder.batches {
		if size > 3 {
			t.Fatalf("batch larger than configured: %#v", embedder.batches)
		}
	}
	if len(chunks) != len(embedder.batches) || chunks[len(chunks)-1] != result.Chunks {
		t.Fatalf("chunk progress = %#v, result chunks = %d", chunks, result.Chunks)
	}
	for i := 1; i < len(chunks); i++ {
		if chunks[i] <= chunks[i-1] {
			t.Fatalf("chunk progress not monotonic: %#v", chunks)
		}
	}
}

type cancelingEmbedder struct {
	cancel context.CancelFunc
}

func (cancelingEmbedder) Model() string { return "model" }
func (e cancelingEmbedder) EmbedDocuments(ctx context.Context, texts []string) ([][]float32, error) {
	e.cancel()
	return nil, ctx.Err()
}

func TestIndexFileCancellationDoesNotPersist(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "notes.md")
	if err := os.WriteFile(path, []byte("conteudo suficiente para gerar chunks"), 0o600); err != nil {
		t.Fatal(err)
	}
	repo := &testRepository{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service, err := New(root, []Extractor{testExtractor{}}, cancelingEmbedder{cancel: cancel}, repo)
	if err != nil {
		t.Fatal(err)
	}
	service.chunkSize = 10
	service.chunkOverlap = 0
	result, err := service.IndexFile(ctx, path, ProgressHooks{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
	if result.Status != domain.FileCanceled {
		t.Fatalf("status = %s, want canceled", result.Status)
	}
	if repo.replacements != 0 {
		t.Fatalf("canceled file was persisted %d times", repo.replacements)
	}
}

func TestSplitRespectsRuneLimitAndOverlap(t *testing.T) {
	chunks := split("um dois três quatro cinco seis", 12, 3)
	if len(chunks) < 2 {
		t.Fatalf("expected multiple chunks, got %#v", chunks)
	}
	for _, chunk := range chunks {
		if len([]rune(chunk.Text)) > 12 {
			t.Fatalf("chunk exceeds limit: %q", chunk.Text)
		}
	}
}

func TestSplitReportsOffsets(t *testing.T) {
	text := "  um dois três quatro cinco seis  "
	runes := []rune(text)
	pieces := split(text, 12, 3)
	if len(pieces) < 2 {
		t.Fatalf("expected multiple pieces, got %#v", pieces)
	}
	for _, piece := range pieces {
		if piece.Start < 0 || piece.End > len(runes) || piece.Start >= piece.End {
			t.Fatalf("invalid offsets %d:%d", piece.Start, piece.End)
		}
		if got := string(runes[piece.Start:piece.End]); got != piece.Text {
			t.Fatalf("offsets select %q, want %q", got, piece.Text)
		}
		if piece.End-piece.Start != len([]rune(piece.Text)) {
			t.Fatalf("offset size %d does not match %d runes", piece.End-piece.Start, len([]rune(piece.Text)))
		}
	}
}
