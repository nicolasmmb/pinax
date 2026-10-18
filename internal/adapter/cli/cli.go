package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	mcpadapter "github.com/nicolasmmb/pinax/internal/adapter/mcp"
	"github.com/nicolasmmb/pinax/internal/adapter/turso"
	"github.com/nicolasmmb/pinax/internal/app/ingest"
	"github.com/nicolasmmb/pinax/internal/app/jobs"
	"github.com/nicolasmmb/pinax/internal/app/query"
	"github.com/nicolasmmb/pinax/internal/config"
)

type Runtime struct {
	Config config.Config
	Ingest *ingest.Service
	Query  *query.Service
	Jobs   *jobs.Service
	Store  *turso.Store
}

func Run(ctx context.Context, args []string, stdout, stderr io.Writer, runtime Runtime) error {
	if len(args) == 0 {
		return usage(stderr)
	}
	logger := newLogger(stderr, runtime.Config.LogLevel)
	switch args[0] {
	case "serve":
		if len(args) != 1 {
			return usage(stderr)
		}
		if runtime.Config.WorkerAutostart {
			stop := startWorkerSupervisor(ctx, stderr, logger)
			defer stop()
		}
		server := mcpadapter.NewServer(runtime.Ingest, runtime.Query)
		return server.Run(ctx, &mcp.StdioTransport{})
	case "worker":
		flags := flag.NewFlagSet("worker", flag.ContinueOnError)
		flags.SetOutput(stderr)
		once := flags.Bool("once", false, "process the queue and exit when it is empty")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 {
			return usage(stderr)
		}
		return runWorker(ctx, runtime, *once, logger)
	default:
		return usage(stderr)
	}
}

func runWorker(ctx context.Context, runtime Runtime, once bool, logger *slog.Logger) error {
	heartbeat := runtime.Config.WorkerHeartbeat
	leaseTTL := max(6*heartbeat, 30*time.Second)
	worker, err := jobs.NewWorker(runtime.Store, runtime.Ingest, jobs.WorkerConfig{
		Workers:       runtime.Config.IngestWorkers,
		JobSlots:      runtime.Config.WorkerJobSlots,
		Heartbeat:     heartbeat,
		JobStaleAfter: leaseTTL,
		LeaseTTL:      leaseTTL,
		Retention:     time.Duration(runtime.Config.JobRetentionDays) * 24 * time.Hour,
		Once:          once,
		Logger:        logger,
	})
	if err != nil {
		return err
	}
	return worker.Run(ctx)
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

func Main() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if len(os.Args) < 2 {
		_ = usage(os.Stderr)
		return 2
	}
	switch os.Args[1] {
	case "serve", "worker":
	default:
		_ = usage(os.Stderr)
		return 2
	}
	cfg, err := config.Load()
	if err != nil {
		return reportError(os.Stderr, err)
	}
	runtime, closeRuntime, err := newRuntime(ctx, cfg)
	if err != nil {
		return reportError(os.Stderr, err)
	}
	defer closeRuntime()
	err = Run(ctx, os.Args[1:], os.Stdout, os.Stderr, runtime)
	if err != nil {
		return reportError(os.Stderr, err)
	}
	return 0
}

func reportError(stderr io.Writer, err error) int {
	fmt.Fprintln(stderr, err)
	return 1
}

func usage(stderr io.Writer) error {
	fmt.Fprintln(stderr, "usage: pinax serve | pinax worker [--once]")
	return fmt.Errorf("invalid command")
}
