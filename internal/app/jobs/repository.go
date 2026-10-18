package jobs

import (
	"context"
	"time"

	"github.com/nicolasmmb/pinax/internal/app/ingest"
	"github.com/nicolasmmb/pinax/internal/domain"
)

// WorkerStore is the persistence port used by the ingestion worker.
type WorkerStore interface {
	QueuedFiles(context.Context, string) ([]string, error)
	PlanJobFiles(context.Context, string, []string) error
	StartJobFile(context.Context, string, string) (bool, error)
	SetJobFileStage(context.Context, string, string, domain.Stage) error
	SetJobFileChunks(context.Context, string, string, int) error
	FinishJobFile(context.Context, string, string, domain.FileStatus, int, string) error
	RequeueJobFile(context.Context, string, string) error
	CancelQueuedFiles(context.Context, string) error
	ClaimNextJob(context.Context, string, time.Time) (*domain.Job, error)
	ActiveJobIDs(context.Context, string) ([]string, error)
	HeartbeatJobs(context.Context, []string, string, time.Time) error
	RecoverStaleJobs(context.Context, time.Time, time.Duration) (int, error)
	RequeueJob(context.Context, string, string) error
	FinalizeJob(context.Context, string) (domain.Job, bool, error)
	FailJob(context.Context, string, string) error
	CancelRequested(context.Context, string) (bool, error)
	PurgeJobs(context.Context, time.Time) (int, error)
	TouchWorker(context.Context, domain.WorkerInfo) error
	ClearWorker(context.Context) error
}

type Ingester interface {
	Model() string
	Chunker() (version string, size, overlap int)
	ResolveInput(path string) (string, domain.InputKind, error)
	Plan(context.Context, string) ([]string, error)
	IndexFile(context.Context, string, ingest.ProgressHooks) (ingest.FileResult, error)
}
