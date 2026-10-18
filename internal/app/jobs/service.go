package jobs

import (
	"context"
	"fmt"
	"time"

	"github.com/nicolasmmb/pinax/internal/domain"
)

const (
	defaultLeaseTTL   = 30 * time.Second
	defaultStaleAfter = 30 * time.Second
	onlineWindow      = 15 * time.Second
)

type SnapshotConfig struct {
	EmbeddingModel string
	ChunkerVersion string
	ChunkSize      int
	ChunkOverlap   int
}

type InputResolver interface {
	ResolveInput(path string) (string, domain.InputKind, error)
}

type ServiceOptions struct {
	LeaseTTL   time.Duration
	StaleAfter time.Duration
}

type Service struct {
	repository Repository
	resolver   InputResolver
	snapshot   SnapshotConfig
	leaseTTL   time.Duration
	staleAfter time.Duration
}

func NewService(repository Repository, resolver InputResolver, snapshot SnapshotConfig, opts ServiceOptions) *Service {
	if opts.LeaseTTL <= 0 {
		opts.LeaseTTL = defaultLeaseTTL
	}
	if opts.StaleAfter <= 0 {
		opts.StaleAfter = defaultStaleAfter
	}
	return &Service{
		repository: repository,
		resolver:   resolver,
		snapshot:   snapshot,
		leaseTTL:   opts.LeaseTTL,
		staleAfter: opts.StaleAfter,
	}
}

func (s *Service) Enqueue(ctx context.Context, inputPath string) (domain.Job, bool, error) {
	resolved, kind, err := s.resolver.ResolveInput(inputPath)
	if err != nil {
		return domain.Job{}, false, err
	}
	request := domain.EnqueueRequest{
		InputPath:      resolved,
		InputKind:      kind,
		EmbeddingModel: s.snapshot.EmbeddingModel,
		ChunkerVersion: s.snapshot.ChunkerVersion,
		ChunkSize:      s.snapshot.ChunkSize,
		ChunkOverlap:   s.snapshot.ChunkOverlap,
	}
	job, deduplicated, err := s.repository.EnqueueJob(ctx, request)
	if err != nil {
		return domain.Job{}, false, fmt.Errorf("enqueue job: %w", err)
	}
	return job, deduplicated, nil
}

func (s *Service) Status(ctx context.Context, jobID string, limit int) (domain.StatusResult, error) {
	if limit <= 0 {
		limit = 10
	}
	if limit > 50 {
		limit = 50
	}
	result := domain.StatusResult{Jobs: []domain.JobSnapshot{}}
	worker, err := s.workerStatus(ctx)
	if err != nil {
		return domain.StatusResult{}, err
	}
	result.Worker = worker
	if jobID != "" {
		snapshot, err := s.snapshotFor(ctx, jobID)
		if err != nil {
			return domain.StatusResult{}, err
		}
		result.Jobs = []domain.JobSnapshot{snapshot}
		return result, nil
	}
	list, err := s.repository.ListJobs(ctx, limit)
	if err != nil {
		return domain.StatusResult{}, err
	}
	for _, job := range list {
		snapshot, err := s.snapshotFor(ctx, job.ID)
		if err != nil {
			return domain.StatusResult{}, err
		}
		result.Jobs = append(result.Jobs, snapshot)
	}
	return result, nil
}

func (s *Service) Cancel(ctx context.Context, jobID string) (domain.JobSnapshot, error) {
	if _, err := s.repository.RequestCancel(ctx, jobID); err != nil {
		return domain.JobSnapshot{}, err
	}
	return s.snapshotFor(ctx, jobID)
}

func (s *Service) workerStatus(ctx context.Context) (domain.WorkerStatus, error) {
	lease, err := s.repository.Lease(ctx)
	if err != nil {
		return domain.WorkerStatus{}, err
	}
	if lease == nil {
		return domain.WorkerStatus{}, nil
	}
	status := domain.WorkerStatus{WorkerLease: *lease, Online: true}
	if time.Since(lease.HeartbeatAt) > onlineWindow {
		status.Online = false
		status.Stale = true
	}
	return status, nil
}

func (s *Service) snapshotFor(ctx context.Context, jobID string) (domain.JobSnapshot, error) {
	job, err := s.repository.GetJob(ctx, jobID)
	if err != nil {
		return domain.JobSnapshot{}, err
	}
	progress, err := s.repository.JobProgress(ctx, jobID)
	if err != nil {
		return domain.JobSnapshot{}, err
	}
	active, err := s.repository.ActiveFiles(ctx, jobID)
	if err != nil {
		return domain.JobSnapshot{}, err
	}
	failures, totalFailures, err := s.repository.Failures(ctx, jobID, 20)
	if err != nil {
		return domain.JobSnapshot{}, err
	}
	return domain.JobSnapshot{
		Job:               job,
		Progress:          progress,
		ActiveFiles:       nonNilFiles(active),
		Failures:          nonNilFiles(failures),
		FailuresTruncated: totalFailures > len(failures),
	}, nil
}

func nonNilFiles(files []domain.JobFile) []domain.JobFile {
	if files == nil {
		return []domain.JobFile{}
	}
	return files
}
