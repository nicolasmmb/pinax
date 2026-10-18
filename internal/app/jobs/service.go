package jobs

import (
	"context"
	"fmt"

	"github.com/nicolasmmb/pinax/internal/domain"
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

type ControlRepository interface {
	EnqueueJob(context.Context, domain.EnqueueRequest) (domain.Job, bool, error)
	Status(context.Context, string, int) (domain.StatusResult, error)
	Cancel(context.Context, string) (domain.JobSnapshot, error)
}

type Service struct {
	repository ControlRepository
	resolver   InputResolver
	snapshot   SnapshotConfig
}

func NewService(repository ControlRepository, resolver InputResolver, snapshot SnapshotConfig) *Service {
	return &Service{
		repository: repository,
		resolver:   resolver,
		snapshot:   snapshot,
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
	return s.repository.Status(ctx, jobID, limit)
}

func (s *Service) Cancel(ctx context.Context, jobID string) (domain.JobSnapshot, error) {
	return s.repository.Cancel(ctx, jobID)
}
