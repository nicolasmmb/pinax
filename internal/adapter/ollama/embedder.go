package ollama

import (
	"context"
	"fmt"
	"strings"
)

type Embedder struct {
	client *Client
	model  string
}

func NewEmbedder(client *Client, model string) *Embedder {
	return &Embedder{client: client, model: model}
}

func (e *Embedder) Model() string { return e.model }

func (e *Embedder) EmbedDocuments(ctx context.Context, texts []string) ([][]float32, error) {
	input := make([]string, len(texts))
	for i, text := range texts {
		input[i] = e.documentInput(text)
	}
	return e.embed(ctx, input)
}

func (e *Embedder) EmbedQuery(ctx context.Context, text string) ([]float32, error) {
	if e.isEmbeddingGemma() {
		text = "task: search result | query: " + text
	}
	vectors, err := e.embed(ctx, []string{text})
	if err != nil {
		return nil, err
	}
	if len(vectors) != 1 {
		return nil, fmt.Errorf("Ollama returned %d embeddings for one query", len(vectors))
	}
	return vectors[0], nil
}

func (e *Embedder) documentInput(text string) string {
	if e.isEmbeddingGemma() {
		return "title: none | text: " + text
	}
	return text
}

func (e *Embedder) isEmbeddingGemma() bool {
	return strings.Contains(strings.ToLower(e.model), "embeddinggemma")
}

func (e *Embedder) embed(ctx context.Context, input []string) ([][]float32, error) {
	var result struct {
		Embeddings [][]float32 `json:"embeddings"`
	}
	if err := e.client.post(ctx, "/api/embed", map[string]any{"model": e.model, "input": input}, &result); err != nil {
		return nil, err
	}
	if len(result.Embeddings) != len(input) {
		return nil, fmt.Errorf("Ollama returned %d embeddings for %d inputs", len(result.Embeddings), len(input))
	}
	return result.Embeddings, nil
}
