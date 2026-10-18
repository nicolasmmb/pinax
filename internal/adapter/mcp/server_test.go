package mcpadapter

import (
	"context"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nicolasmmb/pinax/internal/adapter/turso"
	"github.com/nicolasmmb/pinax/internal/app/jobs"
	"github.com/nicolasmmb/pinax/internal/app/query"
	"github.com/nicolasmmb/pinax/internal/domain"
)

type testResolver struct{}

func (testResolver) ResolveInput(path string) (string, domain.InputKind, error) {
	return path, domain.InputFile, nil
}

func callTool(t *testing.T, session *mcp.ClientSession, name string, args map[string]any) map[string]any {
	t.Helper()
	result, err := session.CallTool(context.Background(), &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatal(err)
	}
	if result.IsError {
		t.Fatalf("tool %s returned error: %#v", name, result.Content)
	}
	structured, ok := result.StructuredContent.(map[string]any)
	if !ok {
		t.Fatalf("tool %s returned no structured content: %#v", name, result.StructuredContent)
	}
	return structured
}

type testEmbedder struct{}

func (testEmbedder) Model() string { return "test" }
func (testEmbedder) EmbedQuery(context.Context, string) ([]float32, error) {
	return []float32{1}, nil
}

type testRepository struct{}

func (testRepository) Search(context.Context, []float32, string, int) ([]domain.SearchResult, error) {
	return []domain.SearchResult{{Path: "source.md", Chunk: domain.Chunk{Text: "found"}}}, nil
}

func TestIndexPathIsAsyncAndStatusReportsJob(t *testing.T) {
	ctx := context.Background()
	store, err := turso.Open(ctx, ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	jobService := jobs.NewService(store, testResolver{}, jobs.SnapshotConfig{
		EmbeddingModel: "test",
		ChunkerVersion: "rune-window-v2",
		ChunkSize:      1200,
		ChunkOverlap:   150,
	})
	server := NewServer(jobService, query.New(testEmbedder{}, testRepository{}))
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

	started := time.Now()
	indexed := callTool(t, session, "index_path", map[string]any{"path": "/docs"})
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("index_path took %s, expected immediate enqueue", elapsed)
	}
	jobID, _ := indexed["job_id"].(string)
	if jobID == "" || indexed["status"] != "queued" {
		t.Fatalf("unexpected index output: %#v", indexed)
	}

	status := callTool(t, session, "ingest_status", map[string]any{"job_id": jobID})
	jobsOut, _ := status["jobs"].([]any)
	if len(jobsOut) != 1 {
		t.Fatalf("unexpected status output: %#v", status)
	}
	jobOut, _ := jobsOut[0].(map[string]any)
	if jobOut["id"] != jobID || jobOut["status"] != "queued" {
		t.Fatalf("unexpected job: %#v", jobOut)
	}

	canceled := callTool(t, session, "ingest_cancel", map[string]any{"job_id": jobID})
	if canceled["status"] != "canceled" {
		t.Fatalf("unexpected cancel output: %#v", canceled)
	}
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
