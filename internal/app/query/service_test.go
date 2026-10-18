package query

import (
	"context"
	"testing"

	"github.com/nicolasmmb/pinax/internal/domain"
)

type testEmbedder struct{}

func (testEmbedder) Model() string { return "test" }
func (testEmbedder) EmbedQuery(context.Context, string) ([]float32, error) {
	return []float32{1}, nil
}

type testRepository struct{ results []domain.SearchResult }

func (r testRepository) Search(context.Context, []float32, string, int) ([]domain.SearchResult, error) {
	return r.results, nil
}

func TestSearchValidatesQueryAndLimit(t *testing.T) {
	service := New(testEmbedder{}, testRepository{})
	for _, test := range []struct {
		query string
		limit int
	}{{"", 5}, {"question", 0}, {"question", 21}} {
		if _, err := service.Search(context.Background(), test.query, test.limit); err == nil {
			t.Fatalf("expected validation error for %#v", test)
		}
	}
}
