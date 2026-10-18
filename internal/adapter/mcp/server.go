package mcpadapter

import (
	"context"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nicolasmmb/pinax/internal/app/ingest"
	"github.com/nicolasmmb/pinax/internal/app/query"
	"github.com/nicolasmmb/pinax/internal/domain"
)

type IndexInput struct {
	Path string `json:"path" jsonschema_description:"Arquivo ou pasta dentro da pasta permitida para indexação."`
}

type QueryInput struct {
	Query string `json:"query" jsonschema_description:"Pergunta ou texto para buscar na base."`
	Limit int    `json:"limit,omitempty" jsonschema_description:"Quantidade de trechos, de 1 a 20; o padrão é 5."`
}

type Source struct {
	File        string  `json:"file"`
	Page        int     `json:"page"`
	Ordinal     int     `json:"ordinal"`
	Hash        string  `json:"hash"`
	Dimensions  int     `json:"dimensions"`
	StartOffset int     `json:"start_offset"`
	EndOffset   int     `json:"end_offset"`
	CharCount   int     `json:"char_count"`
	Text        string  `json:"text"`
	Distance    float64 `json:"distance"`
}

type SearchOutput struct {
	Sources []Source `json:"sources"`
}

type IngestOutput struct {
	Indexed  int      `json:"indexed"`
	Skipped  int      `json:"skipped"`
	Pending  []string `json:"pending_ocr,omitempty"`
	Failures []string `json:"failures,omitempty"`
}

func NewServer(ingestor *ingest.Service, queryService *query.Service) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{Name: "pinax", Version: "0.1.0"}, nil)
	mcp.AddTool(server, &mcp.Tool{Name: "index_path", Description: "Indexa arquivos Markdown e PDFs com texto dentro da pasta permitida."},
		func(ctx context.Context, _ *mcp.CallToolRequest, input IndexInput) (*mcp.CallToolResult, IngestOutput, error) {
			report, err := ingestor.Index(ctx, input.Path)
			return nil, IngestOutput{Indexed: report.Indexed, Skipped: report.Skipped, Pending: report.Pending, Failures: report.Failures}, err
		})
	mcp.AddTool(server, &mcp.Tool{Name: "search_documents", Description: "Busca trechos relevantes e retorna fontes com arquivo e página."},
		func(ctx context.Context, _ *mcp.CallToolRequest, input QueryInput) (*mcp.CallToolResult, SearchOutput, error) {
			limit := input.Limit
			if limit == 0 {
				limit = 5
			}
			results, err := queryService.Search(ctx, input.Query, limit)
			return nil, SearchOutput{Sources: sources(results)}, err
		})
	return server
}

func sources(results []domain.SearchResult) []Source {
	out := make([]Source, 0, len(results))
	for _, result := range results {
		out = append(out, Source{
			File:        result.Path,
			Page:        result.Page,
			Ordinal:     result.Ordinal,
			Hash:        result.Hash,
			Dimensions:  result.Dimensions,
			StartOffset: result.StartOffset,
			EndOffset:   result.EndOffset,
			CharCount:   result.CharCount,
			Text:        result.Text,
			Distance:    result.Distance,
		})
	}
	return out
}
