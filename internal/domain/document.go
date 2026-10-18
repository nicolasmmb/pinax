package domain

import "time"

type Page struct {
	Number int
	Text   string
}

type Document struct {
	Path           string
	Format         string
	SHA256         string
	EmbeddingModel string
	ChunkerVersion string
	ChunkSize      int
	ChunkOverlap   int
	IndexedAt      time.Time
}

type DocumentState struct {
	SHA256         string
	EmbeddingModel string
	ChunkerVersion string
	ChunkSize      int
	ChunkOverlap   int
}

type Chunk struct {
	ID          string
	DocumentID  string
	Ordinal     int
	Page        int
	Text        string
	Hash        string
	StartOffset int
	EndOffset   int
	CharCount   int
	Dimensions  int
	Embedding   []float32
}

type SearchResult struct {
	Chunk
	Path     string
	Distance float64
}

type IngestReport struct {
	Indexed  int      `json:"indexed"`
	Skipped  int      `json:"skipped"`
	Pending  []string `json:"pending_ocr,omitempty"`
	Failures []string `json:"failures,omitempty"`
}
