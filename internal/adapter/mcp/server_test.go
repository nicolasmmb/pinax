package mcpadapter

import (
	"context"
	"testing"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nicolasmmb/pinax/internal/app/query"
	"github.com/nicolasmmb/pinax/internal/domain"
)

type testEmbedder struct{}

func (testEmbedder) Model() string { return "test" }
func (testEmbedder) EmbedQuery(context.Context, string) ([]float32, error) {
	return []float32{1}, nil
}

type testRepository struct{}

func (testRepository) Search(context.Context, []float32, string, int) ([]domain.SearchResult, error) {
	return []domain.SearchResult{{Path: "source.md", Chunk: domain.Chunk{Text: "found"}}}, nil
}

func TestSearchDocumentsToolReturnsSources(t *testing.T) {
	ctx := context.Background()
	server := NewServer(nil, query.New(testEmbedder{}, testRepository{}))
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "1"}, nil)
	serverTransport, clientTransport := mcp.NewInMemoryTransports()
	if _, err := server.Connect(ctx, serverTransport, nil); err != nil {
		t.Fatal(err)
	}
	session, err := client.Connect(ctx, clientTransport, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close()
	result, err := session.CallTool(ctx, &mcp.CallToolParams{Name: "search_documents", Arguments: map[string]any{"query": "question"}})
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError || len(result.Content) == 0 {
		t.Fatalf("unexpected tool result: %#v", result)
	}
}
