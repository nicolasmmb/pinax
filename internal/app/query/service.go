package query

import (
	"context"
	"fmt"
	"strings"

	"github.com/nicolasmmb/pinax/internal/domain"
)

type Embedder interface {
	Model() string
	EmbedQuery(context.Context, string) ([]float32, error)
}

type Repository interface {
	Search(context.Context, []float32, string, int) ([]domain.SearchResult, error)
}

type Service struct {
	embedder   Embedder
	repository Repository
}

func New(embedder Embedder, repository Repository) *Service {
	return &Service{embedder: embedder, repository: repository}
}

func (s *Service) Search(ctx context.Context, question string, limit int) ([]domain.SearchResult, error) {
	question = strings.TrimSpace(question)
	if question == "" {
		return nil, fmt.Errorf("query cannot be empty")
	}
	if limit < 1 || limit > 20 {
		return nil, fmt.Errorf("limit must be between 1 and 20")
	}
	vector, err := s.embedder.EmbedQuery(ctx, question)
	if err != nil {
		return nil, err
	}
	return s.repository.Search(ctx, vector, s.embedder.Model(), limit)
}
