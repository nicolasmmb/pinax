package turso

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/nicolasmmb/pinax/internal/domain"
)

const timeLayout = "2006-01-02T15:04:05.000000000Z"

const jobColumns = `id, input_path, input_kind, status, cancel_requested,
embedding_model, chunker_version, chunk_size, chunk_overlap,
worker_id, error,
created_at, started_at, finished_at, heartbeat_at`

func formatTime(t time.Time) string {
	return t.UTC().Format(timeLayout)
}

func parseTime(value string) (time.Time, error) {
	t, err := time.Parse(timeLayout, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse timestamp %q: %w", value, err)
	}
	return t, nil
}

func parseNullableTime(value sql.NullString) (*time.Time, error) {
	if !value.Valid {
		return nil, nil
	}
	t, err := parseTime(value.String)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func nullableTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return formatTime(*value)
}

func retrySnapshot(ctx context.Context, fn func(context.Context) error) error {
	var err error
	for attempt := 0; attempt < 5; attempt++ {
		err = fn(ctx)
		if err == nil || !strings.Contains(strings.ToLower(err.Error()), "snapshot is stale") {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt+1) * 20 * time.Millisecond):
		}
	}
	return err
}

func newJobID() (string, error) {
	var raw [16]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("generate job id: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}

type jobScanner interface {
	Scan(dest ...any) error
}

func scanJob(scanner jobScanner) (domain.Job, error) {
	var (
		job             domain.Job
		cancelRequested int
		workerID        sql.NullString
		jobError        sql.NullString
		createdAt       string
		startedAt       sql.NullString
		finishedAt      sql.NullString
		heartbeatAt     sql.NullString
		status          string
		kind            string
	)
	err := scanner.Scan(
		&job.ID,
		&job.InputPath,
		&kind,
		&status,
		&cancelRequested,
		&job.EmbeddingModel,
		&job.ChunkerVersion,
		&job.ChunkSize,
		&job.ChunkOverlap,
		&workerID,
		&jobError,
		&createdAt,
		&startedAt,
		&finishedAt,
		&heartbeatAt,
	)
	if err != nil {
		return domain.Job{}, err
	}
	job.InputKind = domain.InputKind(kind)
	job.Status = domain.JobStatus(status)
	job.CancelRequested = cancelRequested == 1
	job.WorkerID = workerID.String
	job.Error = jobError.String
	if job.CreatedAt, err = parseTime(createdAt); err != nil {
		return domain.Job{}, err
	}
	if job.StartedAt, err = parseNullableTime(startedAt); err != nil {
		return domain.Job{}, err
	}
	if job.FinishedAt, err = parseNullableTime(finishedAt); err != nil {
		return domain.Job{}, err
	}
	if job.HeartbeatAt, err = parseNullableTime(heartbeatAt); err != nil {
		return domain.Job{}, err
	}
	return job, nil
}

func (s *Store) EnqueueJob(ctx context.Context, request domain.EnqueueRequest) (domain.Job, bool, error) {
	if err := request.Validate(); err != nil {
		return domain.Job{}, false, err
	}
	id, err := newJobID()
	if err != nil {
		return domain.Job{}, false, err
	}
	now := time.Now().UTC()
	_, err = s.db.ExecContext(ctx, `INSERT INTO ingest_jobs(
id, input_path, input_kind, status, cancel_requested, embedding_model,
chunker_version, chunk_size, chunk_overlap, created_at)
VALUES(?, ?, ?, 'queued', 0, ?, ?, ?, ?, ?)`,
		id, request.InputPath, string(request.InputKind), request.EmbeddingModel,
		request.ChunkerVersion, request.ChunkSize, request.ChunkOverlap, formatTime(now))
	if err != nil {
		existing, lookupErr := s.activeJobByInput(ctx, request.InputPath)
		if lookupErr == nil {
			return existing, true, nil
		}
		return domain.Job{}, false, fmt.Errorf("enqueue job: %w", err)
	}
	job, err := s.GetJob(ctx, id)
	if err != nil {
		return domain.Job{}, false, err
	}
	return job, false, nil
}

func (s *Store) activeJobByInput(ctx context.Context, path string) (domain.Job, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM ingest_jobs
WHERE input_path = ? AND status IN ('queued','running') LIMIT 1`, path)
	return scanJob(row)
}

func (s *Store) GetJob(ctx context.Context, id string) (domain.Job, error) {
	row := s.db.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM ingest_jobs WHERE id = ?`, id)
	job, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Job{}, domain.ErrJobNotFound
	}
	if err != nil {
		return domain.Job{}, fmt.Errorf("get job: %w", err)
	}
	return job, nil
}

func (s *Store) ListJobs(ctx context.Context, limit int) ([]domain.Job, error) {
	if limit <= 0 {
		limit = 10
	}
	rows, err := s.db.QueryContext(ctx, `SELECT `+jobColumns+` FROM ingest_jobs
ORDER BY created_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, fmt.Errorf("list jobs: %w", err)
	}
	defer rows.Close()
	var result []domain.Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, fmt.Errorf("scan job: %w", err)
		}
		result = append(result, job)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

func (s *Store) JobProgress(ctx context.Context, id string) (domain.JobProgress, error) {
	return jobProgress(ctx, s.db, id)
}

type queryer interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func jobProgress(ctx context.Context, db queryer, id string) (domain.JobProgress, error) {
	rows, err := db.QueryContext(ctx, `SELECT status, COUNT(*), COALESCE(SUM(chunks),0)
FROM ingest_job_files WHERE job_id = ? GROUP BY status`, id)
	if err != nil {
		return domain.JobProgress{}, fmt.Errorf("job progress: %w", err)
	}
	defer rows.Close()
	var progress domain.JobProgress
	for rows.Next() {
		var (
			status string
			files  int
			chunks int
		)
		if err := rows.Scan(&status, &files, &chunks); err != nil {
			return domain.JobProgress{}, err
		}
		progress.FilesTotal += files
		progress.Chunks += chunks
		switch domain.FileStatus(status) {
		case domain.FileQueued, domain.FileRunning:
		case domain.FileIndexed:
			progress.Indexed += files
			progress.FilesDone += files
		case domain.FileSkipped:
			progress.Skipped += files
			progress.FilesDone += files
		case domain.FilePendingOCR:
			progress.PendingOCR += files
			progress.FilesDone += files
		case domain.FileFailed:
			progress.Failed += files
			progress.FilesDone += files
		case domain.FileCanceled:
			progress.Canceled += files
			progress.FilesDone += files
		}
	}
	if err := rows.Err(); err != nil {
		return domain.JobProgress{}, err
	}
	if progress.FilesTotal > 0 {
		progress.Percent = float64(progress.FilesDone) / float64(progress.FilesTotal) * 100
	}
	return progress, nil
}

func (s *Store) ActiveFiles(ctx context.Context, id string) ([]domain.JobFile, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT job_id, path, status, COALESCE(stage,''), chunks, attempts,
COALESCE(error,''), started_at, finished_at FROM ingest_job_files
WHERE job_id = ? AND status = 'running' ORDER BY started_at, path`, id)
	if err != nil {
		return nil, fmt.Errorf("active files: %w", err)
	}
	defer rows.Close()
	var result []domain.JobFile
	for rows.Next() {
		file, err := scanJobFile(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, file)
	}
	return result, rows.Err()
}

func (s *Store) Failures(ctx context.Context, id string, limit int) ([]domain.JobFile, int, error) {
	if limit <= 0 {
		limit = 20
	}
	var total int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM ingest_job_files
WHERE job_id = ? AND status = 'failed'`, id).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count failures: %w", err)
	}
	rows, err := s.db.QueryContext(ctx, `SELECT job_id, path, status, COALESCE(stage,''), chunks, attempts,
COALESCE(error,''), started_at, finished_at FROM ingest_job_files
WHERE job_id = ? AND status = 'failed' ORDER BY path LIMIT ?`, id, limit)
	if err != nil {
		return nil, 0, fmt.Errorf("list failures: %w", err)
	}
	defer rows.Close()
	var result []domain.JobFile
	for rows.Next() {
		file, err := scanJobFile(rows)
		if err != nil {
			return nil, 0, err
		}
		result = append(result, file)
	}
	if err := rows.Err(); err != nil {
		return nil, 0, err
	}
	return result, total, nil
}

type jobFileScanner interface {
	Scan(dest ...any) error
}

func scanJobFile(scanner jobFileScanner) (domain.JobFile, error) {
	var (
		file       domain.JobFile
		status     string
		stage      string
		startedAt  sql.NullString
		finishedAt sql.NullString
	)
	if err := scanner.Scan(&file.JobID, &file.Path, &status, &stage, &file.Chunks,
		&file.Attempts, &file.Error, &startedAt, &finishedAt); err != nil {
		return domain.JobFile{}, err
	}
	file.Status = domain.FileStatus(status)
	file.Stage = domain.Stage(stage)
	var err error
	if file.StartedAt, err = parseNullableTime(startedAt); err != nil {
		return domain.JobFile{}, err
	}
	if file.FinishedAt, err = parseNullableTime(finishedAt); err != nil {
		return domain.JobFile{}, err
	}
	return file, nil
}

func (s *Store) JobFiles(ctx context.Context, id string) ([]domain.JobFile, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT job_id, path, status, COALESCE(stage,''), chunks, attempts,
COALESCE(error,''), started_at, finished_at FROM ingest_job_files
WHERE job_id = ? ORDER BY path`, id)
	if err != nil {
		return nil, fmt.Errorf("list job files: %w", err)
	}
	defer rows.Close()
	var result []domain.JobFile
	for rows.Next() {
		file, err := scanJobFile(rows)
		if err != nil {
			return nil, err
		}
		result = append(result, file)
	}
	return result, rows.Err()
}

func (s *Store) QueuedFiles(ctx context.Context, id string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT path FROM ingest_job_files
WHERE job_id = ? AND status = 'queued' ORDER BY path`, id)
	if err != nil {
		return nil, fmt.Errorf("queued files: %w", err)
	}
	defer rows.Close()
	var paths []string
	for rows.Next() {
		var path string
		if err := rows.Scan(&path); err != nil {
			return nil, err
		}
		paths = append(paths, path)
	}
	return paths, rows.Err()
}

func (s *Store) PlanJobFiles(ctx context.Context, id string, paths []string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	const batchSize = 200
	for start := 0; start < len(paths); start += batchSize {
		end := min(start+batchSize, len(paths))
		for _, path := range paths[start:end] {
			if _, err := tx.ExecContext(ctx, `INSERT OR IGNORE INTO ingest_job_files(job_id, path, status)
VALUES(?, ?, 'queued')`, id, path); err != nil {
				return fmt.Errorf("plan file %s: %w", path, err)
			}
		}
	}
	return tx.Commit()
}

func (s *Store) StartJobFile(ctx context.Context, id, path string) (bool, error) {
	result, err := s.db.ExecContext(ctx, `UPDATE ingest_job_files
SET status = 'running', stage = 'hashing', attempts = attempts + 1,
started_at = ?, finished_at = NULL, error = NULL
WHERE job_id = ? AND path = ? AND status = 'queued'`,
		formatTime(time.Now().UTC()), id, path)
	if err != nil {
		return false, fmt.Errorf("start job file: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected == 1, nil
}

func (s *Store) SetJobFileStage(ctx context.Context, id, path string, stage domain.Stage) error {
	if !stage.Valid() {
		return fmt.Errorf("invalid stage %q", stage)
	}
	_, err := s.db.ExecContext(ctx, `UPDATE ingest_job_files SET stage = ?
WHERE job_id = ? AND path = ? AND status = 'running'`, string(stage), id, path)
	if err != nil {
		return fmt.Errorf("set job file stage: %w", err)
	}
	return nil
}

func (s *Store) SetJobFileChunks(ctx context.Context, id, path string, chunks int) error {
	_, err := s.db.ExecContext(ctx, `UPDATE ingest_job_files SET chunks = ?
WHERE job_id = ? AND path = ? AND status = 'running'`, chunks, id, path)
	if err != nil {
		return fmt.Errorf("set job file chunks: %w", err)
	}
	return nil
}

func (s *Store) FinishJobFile(ctx context.Context, id, path string, status domain.FileStatus, chunks int, message string) error {
	if !status.Terminal() {
		return fmt.Errorf("cannot finish file with non-terminal status %q", status)
	}
	_, err := s.db.ExecContext(ctx, `UPDATE ingest_job_files
SET status = ?, stage = NULL, chunks = ?, error = ?, finished_at = ?
WHERE job_id = ? AND path = ?`, string(status), chunks, nullableString(message),
		formatTime(time.Now().UTC()), id, path)
	if err != nil {
		return fmt.Errorf("finish job file: %w", err)
	}
	return nil
}

func (s *Store) RequeueJobFile(ctx context.Context, id, path string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE ingest_job_files
SET status = 'queued', stage = NULL, started_at = NULL, finished_at = NULL
WHERE job_id = ? AND path = ? AND status = 'running'`, id, path)
	if err != nil {
		return fmt.Errorf("requeue job file: %w", err)
	}
	return nil
}

func (s *Store) CancelQueuedFiles(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE ingest_job_files
SET status = 'canceled', stage = NULL, finished_at = ?
WHERE job_id = ? AND status = 'queued'`,
		formatTime(time.Now().UTC()), id)
	if err != nil {
		return fmt.Errorf("cancel queued files: %w", err)
	}
	return nil
}

func (s *Store) ClaimNextJob(ctx context.Context, workerID string, now time.Time) (*domain.Job, error) {
	row := s.db.QueryRowContext(ctx, `UPDATE ingest_jobs
SET status = 'running', worker_id = ?, started_at = COALESCE(started_at, ?), heartbeat_at = ?
WHERE id = (SELECT id FROM ingest_jobs WHERE status = 'queued' ORDER BY created_at LIMIT 1)
RETURNING `+jobColumns, workerID, formatTime(now), formatTime(now))
	job, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("claim job: %w", err)
	}
	return &job, nil
}

func (s *Store) HeartbeatJob(ctx context.Context, id, workerID string, now time.Time) error {
	result, err := s.db.ExecContext(ctx, `UPDATE ingest_jobs SET heartbeat_at = ?
WHERE id = ? AND worker_id = ? AND status = 'running'`, formatTime(now), id, workerID)
	if err != nil {
		return fmt.Errorf("heartbeat job: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return domain.ErrJobNotOwned
	}
	return nil
}

func (s *Store) RecoverStaleJobs(ctx context.Context, now time.Time, timeout time.Duration) (int, error) {
	recovered := 0
	err := retrySnapshot(ctx, func(ctx context.Context) error {
		var err error
		recovered, err = s.recoverStaleJobsOnce(ctx, now, timeout)
		return err
	})
	return recovered, err
}

func (s *Store) recoverStaleJobsOnce(ctx context.Context, now time.Time, timeout time.Duration) (int, error) {
	before := formatTime(now.Add(-timeout))
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, `SELECT id FROM ingest_jobs
WHERE status = 'running' AND (heartbeat_at IS NULL OR heartbeat_at < ?)`, before)
	if err != nil {
		return 0, fmt.Errorf("find stale jobs: %w", err)
	}
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	if err := rows.Close(); err != nil {
		return 0, err
	}
	for _, id := range ids {
		if _, err := tx.ExecContext(ctx, `UPDATE ingest_jobs
SET status = 'queued', worker_id = NULL, heartbeat_at = NULL WHERE id = ?`, id); err != nil {
			return 0, fmt.Errorf("requeue stale job: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE ingest_job_files
SET status = 'queued', stage = NULL, started_at = NULL
WHERE job_id = ? AND status = 'running'`, id); err != nil {
			return 0, fmt.Errorf("requeue stale files: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return len(ids), nil
}

func (s *Store) RequeueJob(ctx context.Context, id, workerID string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, `UPDATE ingest_jobs
SET status = 'queued', worker_id = NULL, heartbeat_at = NULL
WHERE id = ? AND status = 'running' AND worker_id = ?`, id, workerID)
	if err != nil {
		return fmt.Errorf("requeue job: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE ingest_job_files
SET status = 'queued', stage = NULL, started_at = NULL
WHERE job_id = ? AND status = 'running'`, id); err != nil {
		return fmt.Errorf("requeue job files: %w", err)
	}
	return tx.Commit()
}

func (s *Store) FinalizeJob(ctx context.Context, id string) (domain.Job, bool, error) {
	var (
		job  domain.Job
		done bool
	)
	err := retrySnapshot(ctx, func(ctx context.Context) error {
		var err error
		job, done, err = s.finalizeJobOnce(ctx, id)
		return err
	})
	return job, done, err
}

func (s *Store) finalizeJobOnce(ctx context.Context, id string) (domain.Job, bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.Job{}, false, err
	}
	defer tx.Rollback()
	row := tx.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM ingest_jobs WHERE id = ?`, id)
	job, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Job{}, false, domain.ErrJobNotFound
	}
	if err != nil {
		return domain.Job{}, false, err
	}
	if job.Status.Terminal() {
		return job, false, nil
	}
	progress, err := jobProgress(ctx, tx, id)
	if err != nil {
		return domain.Job{}, false, err
	}
	if !progress.Complete() && progress.FilesTotal > 0 {
		return job, false, nil
	}
	status := domain.JobCompleted
	switch {
	case job.CancelRequested:
		status = domain.JobCanceled
	case progress.Failed > 0:
		status = domain.JobCompletedWithErrors
	}
	now := time.Now().UTC()
	if _, err := tx.ExecContext(ctx, `UPDATE ingest_jobs
SET status = ?, finished_at = ?, heartbeat_at = ? WHERE id = ?`,
		string(status), formatTime(now), formatTime(now), id); err != nil {
		return domain.Job{}, false, fmt.Errorf("finalize job: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return domain.Job{}, false, err
	}
	job.Status = status
	job.FinishedAt = &now
	job.HeartbeatAt = &now
	return job, true, nil
}

func (s *Store) FailJob(ctx context.Context, id, message string) error {
	now := formatTime(time.Now().UTC())
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `UPDATE ingest_jobs
SET status = 'failed', error = ?, finished_at = ? WHERE id = ?`,
		message, now, id); err != nil {
		return fmt.Errorf("fail job: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `UPDATE ingest_job_files
SET status = 'failed', stage = NULL, error = ?, finished_at = ?
WHERE job_id = ? AND status IN ('queued','running')`, message, now, id); err != nil {
		return fmt.Errorf("fail job files: %w", err)
	}
	return tx.Commit()
}

func (s *Store) RequestCancel(ctx context.Context, id string) (domain.Job, error) {
	var job domain.Job
	err := retrySnapshot(ctx, func(ctx context.Context) error {
		var err error
		job, err = s.requestCancelOnce(ctx, id)
		return err
	})
	return job, err
}

func (s *Store) requestCancelOnce(ctx context.Context, id string) (domain.Job, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.Job{}, err
	}
	defer tx.Rollback()
	row := tx.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM ingest_jobs WHERE id = ?`, id)
	job, err := scanJob(row)
	if errors.Is(err, sql.ErrNoRows) {
		return domain.Job{}, domain.ErrJobNotFound
	}
	if err != nil {
		return domain.Job{}, err
	}
	if job.Status.Terminal() {
		return job, nil
	}
	now := formatTime(time.Now().UTC())
	if job.Status == domain.JobQueued {
		if _, err := tx.ExecContext(ctx, `UPDATE ingest_jobs
SET cancel_requested = 1, status = 'canceled', finished_at = ? WHERE id = ?`, now, id); err != nil {
			return domain.Job{}, fmt.Errorf("cancel queued job: %w", err)
		}
		if _, err := tx.ExecContext(ctx, `UPDATE ingest_job_files
SET status = 'canceled', stage = NULL, finished_at = ?
WHERE job_id = ? AND status IN ('queued','running')`, now, id); err != nil {
			return domain.Job{}, fmt.Errorf("cancel queued files: %w", err)
		}
		job.Status = domain.JobCanceled
		job.CancelRequested = true
		finished, _ := parseTime(now)
		job.FinishedAt = &finished
	} else {
		if _, err := tx.ExecContext(ctx, `UPDATE ingest_jobs
SET cancel_requested = 1 WHERE id = ?`, id); err != nil {
			return domain.Job{}, fmt.Errorf("request job cancel: %w", err)
		}
		job.CancelRequested = true
	}
	if err := tx.Commit(); err != nil {
		return domain.Job{}, err
	}
	return job, nil
}

func (s *Store) CancelRequested(ctx context.Context, id string) (bool, error) {
	var requested int
	err := s.db.QueryRowContext(ctx, `SELECT cancel_requested FROM ingest_jobs WHERE id = ?`, id).Scan(&requested)
	if errors.Is(err, sql.ErrNoRows) {
		return false, domain.ErrJobNotFound
	}
	if err != nil {
		return false, fmt.Errorf("cancel requested: %w", err)
	}
	return requested == 1, nil
}

func (s *Store) PurgeJobs(ctx context.Context, before time.Time) (int, error) {
	result, err := s.db.ExecContext(ctx, `DELETE FROM ingest_jobs
WHERE status NOT IN ('queued','running') AND finished_at IS NOT NULL AND finished_at < ?`,
		formatTime(before))
	if err != nil {
		return 0, fmt.Errorf("purge jobs: %w", err)
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return 0, err
	}
	return int(affected), nil
}

func (s *Store) TouchWorker(ctx context.Context, info domain.WorkerInfo) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO worker_heartbeat(
id, worker_id, pid, hostname, started_at, heartbeat_at)
VALUES(1, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
worker_id = excluded.worker_id,
pid = excluded.pid,
hostname = excluded.hostname,
started_at = excluded.started_at,
heartbeat_at = excluded.heartbeat_at`,
		info.WorkerID, info.PID, info.Hostname,
		formatTime(info.StartedAt), formatTime(info.HeartbeatAt))
	if err != nil {
		return fmt.Errorf("touch worker heartbeat: %w", err)
	}
	return nil
}

func (s *Store) ClearWorker(ctx context.Context) error {
	if _, err := s.db.ExecContext(ctx, `DELETE FROM worker_heartbeat WHERE id = 1`); err != nil {
		return fmt.Errorf("clear worker heartbeat: %w", err)
	}
	return nil
}

func (s *Store) WorkerInfo(ctx context.Context) (*domain.WorkerInfo, error) {
	var (
		info        domain.WorkerInfo
		startedAt   string
		heartbeatAt string
	)
	err := s.db.QueryRowContext(ctx, `SELECT worker_id, pid, hostname, started_at, heartbeat_at
FROM worker_heartbeat WHERE id = 1`).Scan(
		&info.WorkerID, &info.PID, &info.Hostname, &startedAt, &heartbeatAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get worker heartbeat: %w", err)
	}
	if info.StartedAt, err = parseTime(startedAt); err != nil {
		return nil, err
	}
	if info.HeartbeatAt, err = parseTime(heartbeatAt); err != nil {
		return nil, err
	}
	return &info, nil
}

func (s *Store) ActiveJobIDs(ctx context.Context, workerID string) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id FROM ingest_jobs
WHERE worker_id = ? AND status = 'running'`, workerID)
	if err != nil {
		return nil, fmt.Errorf("list active jobs: %w", err)
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *Store) HeartbeatJobs(ctx context.Context, ids []string, workerID string, now time.Time) error {
	if len(ids) == 0 {
		return nil
	}
	placeholders := make([]string, len(ids))
	args := make([]any, 0, len(ids)+2)
	args = append(args, formatTime(now))
	for i, id := range ids {
		placeholders[i] = "?"
		args = append(args, id)
	}
	args = append(args, workerID)
	query := `UPDATE ingest_jobs SET heartbeat_at = ? WHERE id IN (` + strings.Join(placeholders, ",") + `) AND worker_id = ?`
	if _, err := s.db.ExecContext(ctx, query, args...); err != nil {
		return fmt.Errorf("heartbeat jobs: %w", err)
	}
	return nil
}
