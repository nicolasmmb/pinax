package main

import (
	"context"
	"io"
	"log/slog"
	"strings"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	mcpadapter "github.com/nicolasmmb/pinax/internal/adapter/mcp"
	"github.com/nicolasmmb/pinax/internal/app/jobs"
	"github.com/nicolasmmb/pinax/internal/app/query"
)

func runServe(ctx context.Context, deps mcpDeps) error {
	defer deps.Client.Close()
	server := mcpadapter.NewServer(
		jobs.NewService(deps.Client, deps.Validator, jobSnapshot(deps.Embedder.Model())),
		query.New(deps.Embedder, deps.Client),
	)
	return server.Run(ctx, &mcp.StdioTransport{})
}

func newLogger(stderr io.Writer, level string) *slog.Logger {
	var parsed slog.Level
	switch strings.ToLower(level) {
	case "debug":
		parsed = slog.LevelDebug
	case "warn", "warning":
		parsed = slog.LevelWarn
	case "error":
		parsed = slog.LevelError
	default:
		parsed = slog.LevelInfo
	}
	return slog.New(slog.NewTextHandler(stderr, &slog.HandlerOptions{Level: parsed}))
}
