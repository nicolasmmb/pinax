package mcpadapter

import (
	"context"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/nicolasmmb/pinax/internal/app/jobs"
	"github.com/nicolasmmb/pinax/internal/app/query"
	"github.com/nicolasmmb/pinax/internal/domain"
)

type IndexInput struct {
	Path string `json:"path" jsonschema_description:"Arquivo ou pasta dentro da pasta permitida para indexação."`
}

type IndexOutput struct {
	JobID        string `json:"job_id"`
	Status       string `json:"status"`
	InputPath    string `json:"input_path"`
	Deduplicated bool   `json:"deduplicated"`
}

type StatusInput struct {
	JobID string `json:"job_id,omitempty" jsonschema_description:"ID do job; omita para listar os jobs recentes."`
	Limit int    `json:"limit,omitempty" jsonschema_description:"Quantidade de jobs recentes, de 1 a 50; o padrão é 10."`
}

type CancelInput struct {
	JobID string `json:"job_id" jsonschema_description:"ID do job que deve ser cancelado."`
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

type WorkerOutput struct {
	Online      bool   `json:"online"`
	WorkerID    string `json:"worker_id,omitempty"`
	PID         int    `json:"pid,omitempty"`
	Hostname    string `json:"hostname,omitempty"`
	HeartbeatAt string `json:"heartbeat_at,omitempty"`
	Stale       bool   `json:"stale"`
}

type ProgressOutput struct {
	FilesTotal int     `json:"files_total"`
	Done       int     `json:"done"`
	Indexed    int     `json:"indexed"`
	Skipped    int     `json:"skipped"`
	PendingOCR int     `json:"pending_ocr"`
	Failed     int     `json:"failed"`
	Canceled   int     `json:"canceled"`
	Chunks     int     `json:"chunks"`
	Percent    float64 `json:"percent"`
}

type FileOutput struct {
	Path       string `json:"path"`
	Status     string `json:"status"`
	Stage      string `json:"stage,omitempty"`
	Chunks     int    `json:"chunks"`
	Attempts   int    `json:"attempts"`
	Error      string `json:"error,omitempty"`
	StartedAt  string `json:"started_at,omitempty"`
	FinishedAt string `json:"finished_at,omitempty"`
}

type JobOutput struct {
	ID                string         `json:"id"`
	InputPath         string         `json:"input_path"`
	InputKind         string         `json:"input_kind"`
	Status            string         `json:"status"`
	CancelRequested   bool           `json:"cancel_requested"`
	WorkerID          string         `json:"worker_id,omitempty"`
	Error             string         `json:"error,omitempty"`
	CreatedAt         string         `json:"created_at"`
	StartedAt         string         `json:"started_at,omitempty"`
	FinishedAt        string         `json:"finished_at,omitempty"`
	Progress          ProgressOutput `json:"progress"`
	ActiveFiles       []FileOutput   `json:"active_files"`
	Failures          []FileOutput   `json:"failures"`
	FailuresTruncated bool           `json:"failures_truncated"`
}

type StatusOutput struct {
	Worker WorkerOutput `json:"worker"`
	Jobs   []JobOutput  `json:"jobs"`
}

func NewServer(jobService *jobs.Service, queryService *query.Service) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{Name: "pinax", Version: "0.2.0"}, nil)
	mcp.AddTool(server, &mcp.Tool{Name: "index_path", Description: "Enfileira arquivos Markdown ou PDFs para indexação assíncrona e retorna o job_id imediatamente. Use ingest_status para acompanhar."},
		func(ctx context.Context, _ *mcp.CallToolRequest, input IndexInput) (*mcp.CallToolResult, IndexOutput, error) {
			job, deduplicated, err := jobService.Enqueue(ctx, input.Path)
			if err != nil {
				return nil, IndexOutput{}, err
			}
			return nil, IndexOutput{
				JobID:        job.ID,
				Status:       string(job.Status),
				InputPath:    job.InputPath,
				Deduplicated: deduplicated,
			}, nil
		})
	mcp.AddTool(server, &mcp.Tool{Name: "ingest_status", Description: "Mostra o worker e o progresso dos jobs de ingestão. Sem job_id lista os jobs recentes; com job_id detalha arquivos ativos e falhas."},
		func(ctx context.Context, _ *mcp.CallToolRequest, input StatusInput) (*mcp.CallToolResult, StatusOutput, error) {
			status, err := jobService.Status(ctx, input.JobID, input.Limit)
			if err != nil {
				return nil, StatusOutput{}, err
			}
			return nil, statusOutput(status), nil
		})
	mcp.AddTool(server, &mcp.Tool{Name: "ingest_cancel", Description: "Cancela um job de ingestão. Jobs na fila são cancelados na hora; jobs em execução param no próximo checkpoint."},
		func(ctx context.Context, _ *mcp.CallToolRequest, input CancelInput) (*mcp.CallToolResult, JobOutput, error) {
			snapshot, err := jobService.Cancel(ctx, input.JobID)
			if err != nil {
				return nil, JobOutput{}, err
			}
			return nil, jobOutput(snapshot), nil
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

func statusOutput(status domain.StatusResult) StatusOutput {
	out := StatusOutput{
		Worker: WorkerOutput{
			Online:   status.Worker.Online,
			WorkerID: status.Worker.WorkerID,
			PID:      status.Worker.PID,
			Hostname: status.Worker.Hostname,
			Stale:    status.Worker.Stale,
		},
		Jobs: make([]JobOutput, 0, len(status.Jobs)),
	}
	if !status.Worker.HeartbeatAt.IsZero() {
		out.Worker.HeartbeatAt = status.Worker.HeartbeatAt.Format(time.RFC3339Nano)
	}
	for _, job := range status.Jobs {
		out.Jobs = append(out.Jobs, jobOutput(job))
	}
	return out
}

func jobOutput(snapshot domain.JobSnapshot) JobOutput {
	out := JobOutput{
		ID:              snapshot.ID,
		InputPath:       snapshot.InputPath,
		InputKind:       string(snapshot.InputKind),
		Status:          string(snapshot.Status),
		CancelRequested: snapshot.CancelRequested,
		WorkerID:        snapshot.WorkerID,
		Error:           snapshot.Error,
		CreatedAt:       snapshot.CreatedAt.Format(time.RFC3339Nano),
		Progress: ProgressOutput{
			FilesTotal: snapshot.Progress.FilesTotal,
			Done:       snapshot.Progress.FilesDone,
			Indexed:    snapshot.Progress.Indexed,
			Skipped:    snapshot.Progress.Skipped,
			PendingOCR: snapshot.Progress.PendingOCR,
			Failed:     snapshot.Progress.Failed,
			Canceled:   snapshot.Progress.Canceled,
			Chunks:     snapshot.Progress.Chunks,
			Percent:    snapshot.Progress.Percent,
		},
		ActiveFiles:       make([]FileOutput, 0, len(snapshot.ActiveFiles)),
		Failures:          make([]FileOutput, 0, len(snapshot.Failures)),
		FailuresTruncated: snapshot.FailuresTruncated,
	}
	if snapshot.StartedAt != nil {
		out.StartedAt = snapshot.StartedAt.Format(time.RFC3339Nano)
	}
	if snapshot.FinishedAt != nil {
		out.FinishedAt = snapshot.FinishedAt.Format(time.RFC3339Nano)
	}
	for _, file := range snapshot.ActiveFiles {
		out.ActiveFiles = append(out.ActiveFiles, fileOutput(file))
	}
	for _, file := range snapshot.Failures {
		out.Failures = append(out.Failures, fileOutput(file))
	}
	return out
}

func fileOutput(file domain.JobFile) FileOutput {
	out := FileOutput{
		Path:     file.Path,
		Status:   string(file.Status),
		Stage:    string(file.Stage),
		Chunks:   file.Chunks,
		Attempts: file.Attempts,
		Error:    file.Error,
	}
	if file.StartedAt != nil {
		out.StartedAt = file.StartedAt.Format(time.RFC3339Nano)
	}
	if file.FinishedAt != nil {
		out.FinishedAt = file.FinishedAt.Format(time.RFC3339Nano)
	}
	return out
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
