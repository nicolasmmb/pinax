package ingest

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	"github.com/nicolasmmb/pinax/internal/domain"
)

const (
	defaultChunkerVersion = "rune-window-v2"
	defaultChunkSize      = 1200
	defaultChunkOverlap   = 150
	defaultEmbedBatchSize = 64
)

type Extractor interface {
	Supports(extension string) bool
	Extract(context.Context, string) ([]domain.Page, error)
}

type Embedder interface {
	Model() string
	EmbedDocuments(context.Context, []string) ([][]float32, error)
}

type Repository interface {
	DocumentState(context.Context, string) (domain.DocumentState, error)
	ReplaceDocument(context.Context, domain.Document, []domain.Chunk) error
}

type ProgressHooks struct {
	OnStage  func(domain.Stage)
	OnChunks func(int)
}

func (h ProgressHooks) stage(stage domain.Stage) {
	if h.OnStage != nil {
		h.OnStage(stage)
	}
}

func (h ProgressHooks) chunks(done int) {
	if h.OnChunks != nil {
		h.OnChunks(done)
	}
}

type FileResult struct {
	Status domain.FileStatus
	Chunks int
	Err    error
}

type Service struct {
	root           string
	extractors     []Extractor
	embedder       Embedder
	repository     Repository
	chunkerVersion string
	chunkSize      int
	chunkOverlap   int
	embedBatchSize int
}

func New(root string, extractors []Extractor, embedder Embedder, repository Repository) (*Service, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve document root: %w", err)
	}
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, fmt.Errorf("create document root: %w", err)
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return nil, fmt.Errorf("resolve document root: %w", err)
	}
	return &Service{
		root:           filepath.Clean(root),
		extractors:     extractors,
		embedder:       embedder,
		repository:     repository,
		chunkerVersion: defaultChunkerVersion,
		chunkSize:      defaultChunkSize,
		chunkOverlap:   defaultChunkOverlap,
		embedBatchSize: defaultEmbedBatchSize,
	}, nil
}

func (s *Service) Model() string { return s.embedder.Model() }

func (s *Service) Chunker() (string, int, int) {
	return s.chunkerVersion, s.chunkSize, s.chunkOverlap
}

// ChunkerDefaults returns the chunker configuration used by new services.
func ChunkerDefaults() (string, int, int) {
	return defaultChunkerVersion, defaultChunkSize, defaultChunkOverlap
}

type Validator struct {
	root       string
	extractors []Extractor
}

func NewValidator(root string, extractors []Extractor) (*Validator, error) {
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve document root: %w", err)
	}
	if err := os.MkdirAll(root, 0o750); err != nil {
		return nil, fmt.Errorf("create document root: %w", err)
	}
	root, err = filepath.EvalSymlinks(root)
	if err != nil {
		return nil, fmt.Errorf("resolve document root: %w", err)
	}
	return &Validator{root: filepath.Clean(root), extractors: extractors}, nil
}

func (v *Validator) ResolveInput(inputPath string) (string, domain.InputKind, error) {
	return resolveInput(v.root, v.extractors, inputPath)
}

func (s *Service) SetEmbedBatchSize(size int) {
	if size > 0 {
		s.embedBatchSize = size
	}
}

func (s *Service) ResolveInput(inputPath string) (string, domain.InputKind, error) {
	return resolveInput(s.root, s.extractors, inputPath)
}

func resolveInput(root string, extractors []Extractor, inputPath string) (string, domain.InputKind, error) {
	path, err := filepath.Abs(inputPath)
	if err != nil {
		return "", "", fmt.Errorf("resolve path: %w", err)
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		return "", "", fmt.Errorf("resolve path: %w", err)
	}
	if !within(root, path) {
		return "", "", fmt.Errorf("path %q is outside configured document root", path)
	}
	info, err := os.Stat(path)
	if err != nil {
		return "", "", err
	}
	if info.IsDir() {
		return path, domain.InputDir, nil
	}
	if supported(path, extractors) {
		return path, domain.InputFile, nil
	}
	return "", "", fmt.Errorf("unsupported file type: %s", filepath.Ext(path))
}

func (s *Service) Plan(ctx context.Context, inputPath string) ([]string, error) {
	path, kind, err := s.ResolveInput(inputPath)
	if err != nil {
		return nil, err
	}
	if kind == domain.InputFile {
		return []string{path}, nil
	}
	var files []string
	err = filepath.WalkDir(path, func(file string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if entry.Type()&os.ModeSymlink != 0 {
			return nil
		}
		if !entry.IsDir() && supported(file, s.extractors) {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			if info.Mode().IsRegular() {
				files = append(files, file)
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk directory: %w", err)
	}
	return files, nil
}

func (s *Service) Index(ctx context.Context, inputPath string) (domain.IngestReport, error) {
	files, err := s.Plan(ctx, inputPath)
	if err != nil {
		return domain.IngestReport{}, err
	}
	report := domain.IngestReport{}
	for _, file := range files {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		result, err := s.IndexFile(ctx, file, ProgressHooks{})
		if err != nil && ctx.Err() != nil {
			return report, err
		}
		switch result.Status {
		case domain.FilePendingOCR:
			report.Pending = append(report.Pending, file)
		case domain.FileSkipped:
			report.Skipped++
		case domain.FileIndexed:
			report.Indexed++
		case domain.FileFailed:
			message := "index failed"
			if err != nil {
				message = err.Error()
			}
			report.Failures = append(report.Failures, fmt.Sprintf("%s: %s", file, message))
		}
	}
	return report, nil
}

func (s *Service) IndexFile(ctx context.Context, path string, hooks ProgressHooks) (FileResult, error) {
	info, err := os.Stat(path)
	if err != nil {
		return FileResult{Status: domain.FileFailed, Err: err}, err
	}
	if !info.Mode().IsRegular() {
		err := fmt.Errorf("not a regular file")
		return FileResult{Status: domain.FileFailed, Err: err}, err
	}
	if err := ctx.Err(); err != nil {
		return FileResult{Status: domain.FileCanceled, Err: err}, err
	}
	hooks.stage(domain.StageHashing)
	contents, err := os.ReadFile(path)
	if err != nil {
		return FileResult{Status: domain.FileFailed, Err: err}, err
	}
	sum := sha256.Sum256(contents)
	hash := hex.EncodeToString(sum[:])
	state, err := s.repository.DocumentState(ctx, path)
	if err == nil &&
		state.SHA256 == hash &&
		state.EmbeddingModel == s.embedder.Model() &&
		state.ChunkerVersion == s.chunkerVersion &&
		state.ChunkSize == s.chunkSize &&
		state.ChunkOverlap == s.chunkOverlap {
		return FileResult{Status: domain.FileSkipped}, nil
	} else if err != nil && err != ErrNotFound {
		return FileResult{Status: domain.FileFailed, Err: err}, err
	}
	var extractor Extractor
	for _, candidate := range s.extractors {
		if candidate.Supports(strings.ToLower(filepath.Ext(path))) {
			extractor = candidate
			break
		}
	}
	if extractor == nil {
		err := fmt.Errorf("no extractor for %s", filepath.Ext(path))
		return FileResult{Status: domain.FileFailed, Err: err}, err
	}
	if err := ctx.Err(); err != nil {
		return FileResult{Status: domain.FileCanceled, Err: err}, err
	}
	hooks.stage(domain.StageExtracting)
	pages, err := extractor.Extract(ctx, path)
	if err != nil {
		if ctx.Err() != nil {
			return FileResult{Status: domain.FileCanceled, Err: ctx.Err()}, ctx.Err()
		}
		return FileResult{Status: domain.FileFailed, Err: err}, err
	}
	hooks.stage(domain.StageChunking)
	var chunks []domain.Chunk
	var texts []string
	for _, page := range pages {
		for _, piece := range split(page.Text, s.chunkSize, s.chunkOverlap) {
			chunkHash := hashText(piece.Text)
			chunks = append(chunks, domain.Chunk{
				ID:          chunkID(path, chunkHash, page.Number, piece.Start),
				DocumentID:  path,
				Ordinal:     len(chunks),
				Page:        page.Number,
				Text:        piece.Text,
				Hash:        chunkHash,
				StartOffset: piece.Start,
				EndOffset:   piece.End,
				CharCount:   piece.End - piece.Start,
			})
			texts = append(texts, piece.Text)
		}
	}
	if len(chunks) == 0 {
		return FileResult{Status: domain.FilePendingOCR}, nil
	}
	hooks.stage(domain.StageEmbedding)
	dimensions := 0
	done := 0
	for start := 0; start < len(texts); start += s.embedBatchSize {
		if err := ctx.Err(); err != nil {
			return FileResult{Status: domain.FileCanceled, Err: err}, err
		}
		end := min(start+s.embedBatchSize, len(texts))
		vectors, err := s.embedder.EmbedDocuments(ctx, texts[start:end])
		if err != nil {
			if ctx.Err() != nil {
				return FileResult{Status: domain.FileCanceled, Err: ctx.Err()}, ctx.Err()
			}
			return FileResult{Status: domain.FileFailed, Err: err}, err
		}
		if len(vectors) != end-start {
			err := fmt.Errorf("embedder returned %d vectors for %d chunks", len(vectors), end-start)
			return FileResult{Status: domain.FileFailed, Err: err}, err
		}
		for i := range vectors {
			if dimensions == 0 {
				dimensions = len(vectors[i])
			}
			if dimensions == 0 {
				err := fmt.Errorf("embedder returned empty vector")
				return FileResult{Status: domain.FileFailed, Err: err}, err
			}
			if len(vectors[i]) != dimensions {
				err := fmt.Errorf("embedder returned inconsistent dimensions: chunk %d has %d, expected %d", start+i, len(vectors[i]), dimensions)
				return FileResult{Status: domain.FileFailed, Err: err}, err
			}
			chunks[start+i].Dimensions = dimensions
			chunks[start+i].Embedding = vectors[i]
		}
		done += len(vectors)
		hooks.chunks(done)
	}
	hooks.stage(domain.StageSaving)
	doc := domain.Document{
		Path:           path,
		Format:         strings.TrimPrefix(strings.ToLower(filepath.Ext(path)), "."),
		SHA256:         hash,
		EmbeddingModel: s.embedder.Model(),
		ChunkerVersion: s.chunkerVersion,
		ChunkSize:      s.chunkSize,
		ChunkOverlap:   s.chunkOverlap,
		IndexedAt:      time.Now().UTC(),
	}
	if err := s.repository.ReplaceDocument(ctx, doc, chunks); err != nil {
		if ctx.Err() != nil {
			return FileResult{Status: domain.FileCanceled, Err: ctx.Err()}, ctx.Err()
		}
		return FileResult{Status: domain.FileFailed, Err: err}, err
	}
	return FileResult{Status: domain.FileIndexed, Chunks: len(chunks)}, nil
}

var ErrNotFound = fmt.Errorf("document not found")

func within(root, path string) bool {
	rel, err := filepath.Rel(root, path)
	return err == nil && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

func supported(path string, extractors []Extractor) bool {
	ext := strings.ToLower(filepath.Ext(path))
	for _, e := range extractors {
		if e.Supports(ext) {
			return true
		}
	}
	return false
}

type piece struct {
	Text  string
	Start int
	End   int
}

func split(text string, size, overlap int) []piece {
	if size <= 0 {
		return nil
	}
	if overlap < 0 {
		overlap = 0
	}
	runes := []rune(text)
	var result []piece
	for start := 0; start < len(runes); {
		end := min(start+size, len(runes))
		if end < len(runes) {
			for i := end; i > start+size/2; i-- {
				if runes[i-1] == ' ' || runes[i-1] == '\n' {
					end = i
					break
				}
			}
		}
		trimmedStart := start
		for trimmedStart < end && unicode.IsSpace(runes[trimmedStart]) {
			trimmedStart++
		}
		trimmedEnd := end
		for trimmedEnd > trimmedStart && unicode.IsSpace(runes[trimmedEnd-1]) {
			trimmedEnd--
		}
		if trimmedEnd > trimmedStart {
			result = append(result, piece{
				Text:  string(runes[trimmedStart:trimmedEnd]),
				Start: trimmedStart,
				End:   trimmedEnd,
			})
		}
		if end == len(runes) {
			break
		}
		next := end - overlap
		if next <= start {
			next = start + 1
		}
		start = next
	}
	return result
}

func hashText(text string) string {
	sum := sha256.Sum256([]byte(text))
	return hex.EncodeToString(sum[:])
}

func chunkID(path, hash string, page, start int) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%s\x00%d\x00%d", path, hash, page, start)))
	return hex.EncodeToString(sum[:])
}
