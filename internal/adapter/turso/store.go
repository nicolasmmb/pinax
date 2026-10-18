package turso

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/nicolasmmb/pinax/internal/app/ingest"
	"github.com/nicolasmmb/pinax/internal/domain"

	_ "turso.tech/database/tursogo"
)

const (
	defaultMaxOpenConns = 8
	defaultBusyTimeout  = 10 * time.Second
)

type Options struct {
	Path         string
	MaxOpenConns int
	BusyTimeout  time.Duration
}

type Store struct {
	db *sql.DB
}

func Open(ctx context.Context, path string) (*Store, error) {
	return OpenWithOptions(ctx, Options{Path: path})
}

func OpenWithOptions(ctx context.Context, opts Options) (*Store, error) {
	path := opts.Path
	inMemory := strings.HasPrefix(path, ":memory:")
	if !inMemory {
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
			return nil, fmt.Errorf("create database directory: %w", err)
		}
	}
	maxConns := opts.MaxOpenConns
	if maxConns <= 0 {
		maxConns = defaultMaxOpenConns
	}
	if inMemory {
		maxConns = 1
	}
	busyTimeout := opts.BusyTimeout
	if busyTimeout <= 0 {
		busyTimeout = defaultBusyTimeout
	}
	dsn := path
	if !inMemory {
		separator := "?"
		if strings.Contains(dsn, "?") {
			separator = "&"
		}
		dsn += separator + "_busy_timeout=" + strconv.FormatInt(busyTimeout.Milliseconds(), 10)
	}
	db, err := sql.Open("turso", dsn)
	if err != nil {
		return nil, fmt.Errorf("open Turso database: %w", err)
	}
	db.SetMaxOpenConns(maxConns)
	db.SetMaxIdleConns(maxConns)
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("connect to Turso database: %w", err)
	}
	if !inMemory {
		var mode string
		if err := db.QueryRowContext(ctx, "PRAGMA journal_mode = WAL").Scan(&mode); err != nil {
			db.Close()
			return nil, fmt.Errorf("enable Turso WAL: %w", err)
		}
	}
	if _, err := db.ExecContext(ctx, "PRAGMA foreign_keys = ON"); err != nil {
		db.Close()
		return nil, fmt.Errorf("enable Turso foreign keys: %w", err)
	}
	store := &Store{db: db}
	if err := store.migrate(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate(ctx context.Context) error {
	const schema = `
CREATE TABLE IF NOT EXISTS documents (
  path TEXT PRIMARY KEY,
  format TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  embedding_model TEXT NOT NULL,
  chunker_version TEXT NOT NULL,
  chunk_size INTEGER NOT NULL,
  chunk_overlap INTEGER NOT NULL,
  indexed_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS chunks (
  id TEXT PRIMARY KEY,
  document_path TEXT NOT NULL REFERENCES documents(path) ON DELETE CASCADE,
  ordinal INTEGER NOT NULL,
  page INTEGER NOT NULL,
  text TEXT NOT NULL,
  content_hash TEXT NOT NULL,
  start_offset INTEGER NOT NULL,
  end_offset INTEGER NOT NULL,
  char_count INTEGER NOT NULL,
  dimensions INTEGER NOT NULL,
  embedding BLOB NOT NULL,
  UNIQUE(document_path, ordinal)
);
CREATE INDEX IF NOT EXISTS chunks_document_path ON chunks(document_path);
CREATE INDEX IF NOT EXISTS chunks_content_hash ON chunks(content_hash);

CREATE TABLE IF NOT EXISTS ingest_jobs (
  id TEXT PRIMARY KEY,
  input_path TEXT NOT NULL,
  input_kind TEXT NOT NULL CHECK (input_kind IN ('file','dir')),
  status TEXT NOT NULL CHECK (status IN ('queued','running','completed','completed_with_errors','canceled','failed')),
  cancel_requested INTEGER NOT NULL DEFAULT 0 CHECK (cancel_requested IN (0,1)),
  embedding_model TEXT NOT NULL,
  chunker_version TEXT NOT NULL,
  chunk_size INTEGER NOT NULL,
  chunk_overlap INTEGER NOT NULL,
  planning_done INTEGER NOT NULL DEFAULT 0 CHECK (planning_done IN (0,1)),
  planned_at TEXT,
  worker_id TEXT,
  error TEXT,
  created_at TEXT NOT NULL,
  started_at TEXT,
  finished_at TEXT,
  heartbeat_at TEXT
);
CREATE TABLE IF NOT EXISTS ingest_job_files (
  job_id TEXT NOT NULL REFERENCES ingest_jobs(id) ON DELETE CASCADE,
  path TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('queued','running','indexed','skipped','pending_ocr','failed','canceled')),
  stage TEXT CHECK (stage IS NULL OR stage IN ('hashing','extracting','chunking','embedding','saving')),
  chunks INTEGER NOT NULL DEFAULT 0,
  attempts INTEGER NOT NULL DEFAULT 0,
  error TEXT,
  started_at TEXT,
  finished_at TEXT,
  PRIMARY KEY (job_id, path)
);
CREATE TABLE IF NOT EXISTS worker_leases (
  name TEXT PRIMARY KEY,
  worker_id TEXT NOT NULL,
  pid INTEGER NOT NULL,
  hostname TEXT NOT NULL,
  started_at TEXT NOT NULL,
  heartbeat_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS ingest_jobs_status_created ON ingest_jobs(status, created_at);
CREATE UNIQUE INDEX IF NOT EXISTS ingest_jobs_active_input ON ingest_jobs(input_path) WHERE status IN ('queued','running');
CREATE INDEX IF NOT EXISTS ingest_job_files_job_status ON ingest_job_files(job_id, status);
CREATE INDEX IF NOT EXISTS ingest_job_files_job_stage ON ingest_job_files(job_id, stage) WHERE status = 'running';
CREATE INDEX IF NOT EXISTS ingest_jobs_finished ON ingest_jobs(finished_at) WHERE finished_at IS NOT NULL;
`
	if _, err := s.db.ExecContext(ctx, schema); err != nil {
		return fmt.Errorf("create Turso schema: %w", err)
	}
	return nil
}

func (s *Store) DocumentState(ctx context.Context, path string) (domain.DocumentState, error) {
	var state domain.DocumentState
	err := s.db.QueryRowContext(ctx, `SELECT sha256, embedding_model, chunker_version, chunk_size, chunk_overlap
FROM documents WHERE path = ?`, path).Scan(
		&state.SHA256,
		&state.EmbeddingModel,
		&state.ChunkerVersion,
		&state.ChunkSize,
		&state.ChunkOverlap,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.DocumentState{}, ingest.ErrNotFound
	}
	return state, err
}

func (s *Store) ReplaceDocument(ctx context.Context, document domain.Document, chunks []domain.Chunk) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO documents(path, format, sha256, embedding_model, chunker_version, chunk_size, chunk_overlap, indexed_at)
VALUES(?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(path) DO UPDATE SET format=excluded.format, sha256=excluded.sha256,
embedding_model=excluded.embedding_model, chunker_version=excluded.chunker_version,
chunk_size=excluded.chunk_size, chunk_overlap=excluded.chunk_overlap, indexed_at=excluded.indexed_at`,
		document.Path, document.Format, document.SHA256, document.EmbeddingModel,
		document.ChunkerVersion, document.ChunkSize, document.ChunkOverlap,
		document.IndexedAt.UTC().Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("save document: %w", err)
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM chunks WHERE document_path = ?", document.Path); err != nil {
		return fmt.Errorf("replace document chunks: %w", err)
	}
	for _, chunk := range chunks {
		vector, err := json.Marshal(chunk.Embedding)
		if err != nil {
			return fmt.Errorf("encode embedding: %w", err)
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO chunks(id, document_path, ordinal, page, text, content_hash, start_offset, end_offset, char_count, dimensions, embedding)
VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, vector32(?))`,
			chunk.ID, document.Path, chunk.Ordinal, chunk.Page, chunk.Text, chunk.Hash,
			chunk.StartOffset, chunk.EndOffset, chunk.CharCount, chunk.Dimensions, string(vector))
		if err != nil {
			return fmt.Errorf("save chunk %d: %w", chunk.Ordinal, err)
		}
	}
	return tx.Commit()
}

func (s *Store) Search(ctx context.Context, vector []float32, model string, limit int) ([]domain.SearchResult, error) {
	encoded, err := json.Marshal(vector)
	if err != nil {
		return nil, fmt.Errorf("encode query vector: %w", err)
	}
	// shortcut: exact vector scan is suitable for the initial corpus, add an ANN index when scan latency becomes material.
	rows, err := s.db.QueryContext(ctx, `SELECT c.id, c.document_path, c.ordinal, c.page, c.text,
c.content_hash, c.start_offset, c.end_offset, c.char_count, c.dimensions,
vector_distance_cos(c.embedding, vector32(?)) AS distance
FROM chunks c JOIN documents d ON d.path = c.document_path
WHERE d.embedding_model = ?
ORDER BY distance LIMIT ?`, string(encoded), model, limit)
	if err != nil {
		return nil, fmt.Errorf("search Turso vectors: %w", err)
	}
	defer rows.Close()
	var results []domain.SearchResult
	for rows.Next() {
		var result domain.SearchResult
		if err := rows.Scan(
			&result.ID,
			&result.Path,
			&result.Ordinal,
			&result.Page,
			&result.Text,
			&result.Hash,
			&result.StartOffset,
			&result.EndOffset,
			&result.CharCount,
			&result.Dimensions,
			&result.Distance,
		); err != nil {
			return nil, err
		}
		result.DocumentID = result.Path
		results = append(results, result)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return results, nil
}
