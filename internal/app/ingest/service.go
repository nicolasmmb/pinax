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

type Service struct {
	root           string
	extractors     []Extractor
	embedder       Embedder
	repository     Repository
	chunkerVersion string
	chunkSize      int
	chunkOverlap   int
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
	}, nil
}

func (s *Service) Index(ctx context.Context, inputPath string) (domain.IngestReport, error) {
	path, err := filepath.Abs(inputPath)
	if err != nil {
		return domain.IngestReport{}, fmt.Errorf("resolve path: %w", err)
	}
	path, err = filepath.EvalSymlinks(path)
	if err != nil {
		return domain.IngestReport{}, fmt.Errorf("resolve path: %w", err)
	}
	if !within(s.root, path) {
		return domain.IngestReport{}, fmt.Errorf("path %q is outside configured document root", path)
	}
	info, err := os.Stat(path)
	if err != nil {
		return domain.IngestReport{}, err
	}
	var files []string
	if info.IsDir() {
		err = filepath.WalkDir(path, func(file string, entry os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
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
			return domain.IngestReport{}, fmt.Errorf("walk directory: %w", err)
		}
	} else if supported(path, s.extractors) {
		files = []string{path}
	} else {
		return domain.IngestReport{}, fmt.Errorf("unsupported file type: %s", filepath.Ext(path))
	}

	report := domain.IngestReport{}
	for _, file := range files {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		indexed, pending, err := s.indexFile(ctx, file)
		if pending {
			report.Pending = append(report.Pending, file)
		} else if err == errUnchanged {
			report.Skipped++
		} else if err != nil {
			report.Failures = append(report.Failures, fmt.Sprintf("%s: %v", file, err))
		} else if indexed {
			report.Indexed++
		}
	}
	return report, nil
}

var errUnchanged = fmt.Errorf("document unchanged")

func (s *Service) indexFile(ctx context.Context, path string) (indexed, pending bool, err error) {
	info, err := os.Stat(path)
	if err != nil {
		return false, false, err
	}
	if !info.Mode().IsRegular() {
		return false, false, fmt.Errorf("not a regular file")
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		return false, false, err
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
		return false, false, errUnchanged
	} else if err != nil && err != ErrNotFound {
		return false, false, err
	}
	var extractor Extractor
	for _, candidate := range s.extractors {
		if candidate.Supports(strings.ToLower(filepath.Ext(path))) {
			extractor = candidate
			break
		}
	}
	if extractor == nil {
		return false, false, fmt.Errorf("no extractor for %s", filepath.Ext(path))
	}
	pages, err := extractor.Extract(ctx, path)
	if err != nil {
		return false, false, err
	}
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
		return false, true, nil
	}
	vectors, err := s.embedder.EmbedDocuments(ctx, texts)
	if err != nil {
		return false, false, err
	}
	if len(vectors) != len(chunks) {
		return false, false, fmt.Errorf("embedder returned %d vectors for %d chunks", len(vectors), len(chunks))
	}
	dimensions := len(vectors[0])
	if dimensions == 0 {
		return false, false, fmt.Errorf("embedder returned empty vector")
	}
	for i := range chunks {
		if len(vectors[i]) != dimensions {
			return false, false, fmt.Errorf("embedder returned inconsistent dimensions: chunk %d has %d, expected %d", i, len(vectors[i]), dimensions)
		}
		chunks[i].Dimensions = dimensions
		chunks[i].Embedding = vectors[i]
	}
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
		return false, false, err
	}
	return true, false, nil
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
