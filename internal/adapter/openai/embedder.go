package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type Embedder struct {
	url   string
	key   string
	model string
	http  *http.Client
}

func New(url, key, model string) *Embedder {
	return &Embedder{url: strings.TrimRight(url, "/"), key: key, model: model, http: &http.Client{Timeout: 5 * time.Minute}}
}

func (e *Embedder) Model() string { return "openai:" + e.model }

func (e *Embedder) EmbedDocuments(ctx context.Context, texts []string) ([][]float32, error) {
	return e.embed(ctx, texts)
}

func (e *Embedder) EmbedQuery(ctx context.Context, text string) ([]float32, error) {
	vectors, err := e.embed(ctx, []string{text})
	if err != nil {
		return nil, err
	}
	return vectors[0], nil
}

func (e *Embedder) embed(ctx context.Context, input []string) ([][]float32, error) {
	body, err := json.Marshal(map[string]any{"model": e.model, "input": input})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, e.url+"/embeddings", bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if e.key != "" {
		req.Header.Set("Authorization", "Bearer "+e.key)
	}
	resp, err := e.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("embedding provider request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("embedding provider returned %s", resp.Status)
	}
	var result struct {
		Data []struct {
			Index     int       `json:"index"`
			Embedding []float32 `json:"embedding"`
		} `json:"data"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode embedding provider response: %w", err)
	}
	if len(result.Data) != len(input) {
		return nil, fmt.Errorf("embedding provider returned %d embeddings for %d inputs", len(result.Data), len(input))
	}
	vectors := make([][]float32, len(input))
	for _, item := range result.Data {
		if item.Index < 0 || item.Index >= len(vectors) || vectors[item.Index] != nil {
			return nil, fmt.Errorf("embedding provider returned invalid index %d", item.Index)
		}
		vectors[item.Index] = item.Embedding
	}
	for i, vector := range vectors {
		if vector == nil {
			return nil, fmt.Errorf("embedding provider omitted index %d", i)
		}
	}
	return vectors, nil
}
