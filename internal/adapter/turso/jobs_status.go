package turso

import (
	"context"
	"time"

	"github.com/nicolasmmb/pinax/internal/domain"
)

const workerOnlineWindow = 15 * time.Second

func (s *Store) Status(ctx context.Context, jobID string, limit int) (domain.StatusResult, error) {
	if limit <= 0 {
		limit = 10
	}
	if limit > 50 {
		limit = 50
	}
	result := domain.StatusResult{Jobs: []domain.JobSnapshot{}}
	lease, err := s.Lease(ctx)
	if err != nil {
		return domain.StatusResult{}, err
	}
	if lease != nil {
		result.Worker = domain.WorkerStatus{WorkerLease: *lease, Online: true}
		if time.Since(lease.HeartbeatAt) > workerOnlineWindow {
			result.Worker.Online = false
			result.Worker.Stale = true
		}
	}
	if jobID != "" {
		snapshot, err := s.jobSnapshot(ctx, jobID)
		if err != nil {
			return domain.StatusResult{}, err
		}
		result.Jobs = []domain.JobSnapshot{snapshot}
		return result, nil
	}
	list, err := s.ListJobs(ctx, limit)
	if err != nil {
		return domain.StatusResult{}, err
	}
	for _, job := range list {
		snapshot, err := s.jobSnapshot(ctx, job.ID)
		if err != nil {
			return domain.StatusResult{}, err
		}
		result.Jobs = append(result.Jobs, snapshot)
	}
	return result, nil
}

func (s *Store) Cancel(ctx context.Context, jobID string) (domain.JobSnapshot, error) {
	if _, err := s.RequestCancel(ctx, jobID); err != nil {
		return domain.JobSnapshot{}, err
	}
	return s.jobSnapshot(ctx, jobID)
}

func (s *Store) jobSnapshot(ctx context.Context, jobID string) (domain.JobSnapshot, error) {
	job, err := s.GetJob(ctx, jobID)
	if err != nil {
		return domain.JobSnapshot{}, err
	}
	progress, err := s.JobProgress(ctx, jobID)
	if err != nil {
		return domain.JobSnapshot{}, err
	}
	active, err := s.ActiveFiles(ctx, jobID)
	if err != nil {
		return domain.JobSnapshot{}, err
	}
	failures, totalFailures, err := s.Failures(ctx, jobID, 20)
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
