package domain

import (
	"fmt"
	"time"
)

type JobStatus string

const (
	JobQueued              JobStatus = "queued"
	JobRunning             JobStatus = "running"
	JobCompleted           JobStatus = "completed"
	JobCompletedWithErrors JobStatus = "completed_with_errors"
	JobCanceled            JobStatus = "canceled"
	JobFailed              JobStatus = "failed"
)

func (s JobStatus) Valid() bool {
	switch s {
	case JobQueued, JobRunning, JobCompleted, JobCompletedWithErrors, JobCanceled, JobFailed:
		return true
	}
	return false
}

func (s JobStatus) Terminal() bool {
	switch s {
	case JobCompleted, JobCompletedWithErrors, JobCanceled, JobFailed:
		return true
	}
	return false
}

func (s JobStatus) Active() bool {
	return s == JobQueued || s == JobRunning
}

type FileStatus string

const (
	FileQueued     FileStatus = "queued"
	FileRunning    FileStatus = "running"
	FileIndexed    FileStatus = "indexed"
	FileSkipped    FileStatus = "skipped"
	FilePendingOCR FileStatus = "pending_ocr"
	FileFailed     FileStatus = "failed"
	FileCanceled   FileStatus = "canceled"
)

func (s FileStatus) Valid() bool {
	switch s {
	case FileQueued, FileRunning, FileIndexed, FileSkipped, FilePendingOCR, FileFailed, FileCanceled:
		return true
	}
	return false
}

func (s FileStatus) Terminal() bool {
	switch s {
	case FileIndexed, FileSkipped, FilePendingOCR, FileFailed, FileCanceled:
		return true
	}
	return false
}

type Stage string

const (
	StageHashing    Stage = "hashing"
	StageExtracting Stage = "extracting"
	StageChunking   Stage = "chunking"
	StageEmbedding  Stage = "embedding"
	StageSaving     Stage = "saving"
)

func (s Stage) Valid() bool {
	switch s {
	case StageHashing, StageExtracting, StageChunking, StageEmbedding, StageSaving:
		return true
	}
	return false
}

type InputKind string

const (
	InputFile InputKind = "file"
	InputDir  InputKind = "dir"
)

func (k InputKind) Valid() bool {
	return k == InputFile || k == InputDir
}

type Job struct {
	ID              string
	InputPath       string
	InputKind       InputKind
	Status          JobStatus
	CancelRequested bool

	EmbeddingModel string
	ChunkerVersion string
	ChunkSize      int
	ChunkOverlap   int

	PlanningDone bool
	PlannedAt    *time.Time
	WorkerID     string
	Error        string
	CreatedAt    time.Time
	StartedAt    *time.Time
	FinishedAt   *time.Time
	HeartbeatAt  *time.Time
}

func (j Job) Terminal() bool { return j.Status.Terminal() }

type JobFile struct {
	JobID      string
	Path       string
	Status     FileStatus
	Stage      Stage
	Chunks     int
	Attempts   int
	Error      string
	StartedAt  *time.Time
	FinishedAt *time.Time
}

type JobProgress struct {
	FilesTotal int     `json:"files_total"`
	FilesDone  int     `json:"done"`
	Indexed    int     `json:"indexed"`
	Skipped    int     `json:"skipped"`
	PendingOCR int     `json:"pending_ocr"`
	Failed     int     `json:"failed"`
	Canceled   int     `json:"canceled"`
	Chunks     int     `json:"chunks"`
	Percent    float64 `json:"percent"`
}

func (p JobProgress) Complete() bool {
	return p.FilesTotal > 0 && p.FilesDone == p.FilesTotal || p.FilesTotal == 0
}

type JobSnapshot struct {
	Job
	Progress          JobProgress `json:"progress"`
	ActiveFiles       []JobFile   `json:"active_files"`
	Failures          []JobFile   `json:"failures"`
	FailuresTruncated bool        `json:"failures_truncated"`
}

type WorkerLease struct {
	Name        string
	WorkerID    string
	PID         int
	Hostname    string
	StartedAt   time.Time
	HeartbeatAt time.Time
}

type WorkerStatus struct {
	WorkerLease
	Online bool `json:"online"`
	Stale  bool `json:"stale"`
}

type StatusResult struct {
	Worker WorkerStatus  `json:"worker"`
	Jobs   []JobSnapshot `json:"jobs"`
}

type EnqueueRequest struct {
	InputPath      string
	InputKind      InputKind
	EmbeddingModel string
	ChunkerVersion string
	ChunkSize      int
	ChunkOverlap   int
}

func (r EnqueueRequest) Validate() error {
	if r.InputPath == "" {
		return fmt.Errorf("input path cannot be empty")
	}
	if !r.InputKind.Valid() {
		return fmt.Errorf("invalid input kind %q", r.InputKind)
	}
	if r.EmbeddingModel == "" {
		return fmt.Errorf("embedding model cannot be empty")
	}
	if r.ChunkerVersion == "" {
		return fmt.Errorf("chunker version cannot be empty")
	}
	if r.ChunkSize <= 0 {
		return fmt.Errorf("chunk size must be positive")
	}
	if r.ChunkOverlap < 0 || r.ChunkOverlap >= r.ChunkSize {
		return fmt.Errorf("chunk overlap must be between 0 and chunk size")
	}
	return nil
}
