package jobs

import (
	"context"
	"errors"
	"time"

	"github.com/nicolasmmb/pinax/internal/app/ingest"
	"github.com/nicolasmmb/pinax/internal/domain"
)

var (
	ErrJobNotFound = errors.New("job not found")
	ErrJobNotOwned = errors.New("job not owned by this worker")
)

type Repository interface {
	EnqueueJob(context.Context, domain.EnqueueRequest) (domain.Job, bool, error)
	GetJob(context.Context, string) (domain.Job, error)
	ListJobs(context.Context, int) ([]domain.Job, error)
	JobProgress(context.Context, string) (domain.JobProgress, error)
	ActiveFiles(context.Context, string) ([]domain.JobFile, error)
	Failures(context.Context, string, int) ([]domain.JobFile, int, error)
	QueuedFiles(context.Context, string) ([]string, error)
	PlanJobFiles(context.Context, string, []string) error
	ResetPlanning(context.Context, string) error
	StartJobFile(context.Context, string, string) (bool, error)
	SetJobFileStage(context.Context, string, string, domain.Stage) error
	SetJobFileChunks(context.Context, string, string, int) error
	FinishJobFile(context.Context, string, string, domain.FileStatus, int, string) error
	RequeueJobFile(context.Context, string, string) error
	CancelQueuedFiles(context.Context, string) error
	ClaimNextJob(context.Context, string, time.Time) (*domain.Job, error)
	HeartbeatJob(context.Context, string, string, time.Time) error
	RecoverStaleJobs(context.Context, time.Time, time.Duration) (int, error)
	RequeueJob(context.Context, string, string) error
	FinalizeJob(context.Context, string) (domain.Job, bool, error)
	FailJob(context.Context, string, string) error
	RequestCancel(context.Context, string) (domain.Job, error)
	CancelRequested(context.Context, string) (bool, error)
	PurgeJobs(context.Context, time.Time) (int, error)
	AcquireLease(context.Context, domain.WorkerLease, time.Duration) (bool, error)
	RenewLease(context.Context, string, string, time.Time) (bool, error)
	ReleaseLease(context.Context, string, string) error
	Lease(context.Context) (*domain.WorkerLease, error)
}

type Ingester interface {
	Model() string
	Chunker() (version string, size, overlap int)
	ResolveInput(path string) (string, domain.InputKind, error)
	Plan(context.Context, string) ([]string, error)
	IndexFile(context.Context, string, ingest.ProgressHooks) (ingest.FileResult, error)
}
