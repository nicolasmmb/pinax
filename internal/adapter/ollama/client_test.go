package ollama

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestEmbedderUsesOllamaEndpoint(t *testing.T) {
	client := &Client{baseURL: "http://ollama", http: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		var body string
		if r.URL.Path == "/api/embed" {
			var input struct {
				Model string   `json:"model"`
				Input []string `json:"input"`
			}
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				return nil, err
			}
			if input.Model != "embeddinggemma-2:740m" || len(input.Input) != 1 {
				return nil, fmt.Errorf("unexpected embedding request: %#v", input)
			}
			if input.Input[0] != "task: search result | query: question" {
				return nil, fmt.Errorf("unexpected retrieval prefix: %q", input.Input[0])
			}
			body = `{"embeddings":[[1,0]]}`
		} else {
			return nil, fmt.Errorf("unexpected endpoint %s", r.URL.Path)
		}
		return &http.Response{StatusCode: http.StatusOK, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}, nil
	})}}
	embedder := NewEmbedder(client, "embeddinggemma-2:740m")
	vector, err := embedder.EmbedQuery(context.Background(), "question")
	if err != nil || len(vector) != 2 || vector[0] != 1 {
		t.Fatalf("embedding = %#v, error %v", vector, err)
	}
}
