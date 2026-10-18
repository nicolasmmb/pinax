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
	"github.com/nicolasmmb/pinax/internal/adapter/ipc"
	mcpadapter "github.com/nicolasmmb/pinax/internal/adapter/mcp"
	"github.com/nicolasmmb/pinax/internal/adapter/turso"
	"github.com/nicolasmmb/pinax/internal/app/ingest"
	"github.com/nicolasmmb/pinax/internal/app/jobs"
	"github.com/nicolasmmb/pinax/internal/app/query"
	"github.com/nicolasmmb/pinax/internal/config"
)

type Runtime struct {
	Config    config.Config
	Ingest    *ingest.Service
	Query     *query.Service
	Jobs      *jobs.Service
	Store     *turso.Store
	Embedder  embedder
	Validator *ingest.Validator
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
		socketPath := ipc.SocketPath(runtime.Config.DatabasePath)
		if runtime.Config.WorkerAutostart {
			stop := startWorkerSupervisor(ctx, socketPath, stderr, logger)
			defer stop()
		}
		timeout := 5 * time.Second
		if runtime.Config.WorkerAutostart {
			timeout = 15 * time.Second
		}
		client, err := ipc.DialWithRetry(ctx, socketPath, timeout)
		if err != nil {
			return fmt.Errorf("connect to ingest worker: %w (start `pinax worker` or enable PINAX_WORKER_AUTOSTART)", err)
		}
		defer client.Close()
		version, size, overlap := ingest.ChunkerDefaults()
		jobService := jobs.NewService(client, runtime.Validator, jobs.SnapshotConfig{
			EmbeddingModel: runtime.Embedder.Model(),
			ChunkerVersion: version,
			ChunkSize:      size,
			ChunkOverlap:   overlap,
		})
		queryService := query.New(runtime.Embedder, client)
		server := mcpadapter.NewServer(jobService, queryService)
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
	case "status":
		return runStatus(ctx, args[1:], stdout, stderr, runtime)
	default:
		return usage(stderr)
	}
}

func runWorker(ctx context.Context, runtime Runtime, once bool, logger *slog.Logger) error {
	ipcServer, err := ipc.Listen(ipc.SocketPath(runtime.Config.DatabasePath), runtime.Store, runtime.Store, logger)
	if err != nil {
		return err
	}
	defer ipcServer.Close()
	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()
	serverErr := make(chan error, 1)
	go func() { serverErr <- ipcServer.Serve(workerCtx) }()

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
	workerDone := make(chan error, 1)
	go func() { workerDone <- worker.Run(workerCtx) }()
	select {
	case err := <-workerDone:
		workerCancel()
		return err
	case err := <-serverErr:
		workerCancel()
		<-workerDone
		return err
	}
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
	case "serve", "worker", "status":
	default:
		_ = usage(os.Stderr)
		return 2
	}
	cfg, err := config.Load()
	if err != nil {
		return reportError(os.Stderr, err)
	}
	var (
		runtime      Runtime
		closeRuntime func()
	)
	switch os.Args[1] {
	case "worker":
		runtime, closeRuntime, err = newWorkerRuntime(ctx, cfg)
	case "status":
		runtime, closeRuntime, err = newStatusRuntime(cfg)
	default:
		runtime, closeRuntime, err = newServeRuntime(cfg)
	}
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
	fmt.Fprintln(stderr, "usage: pinax serve | pinax worker [--once] | pinax status [job_id] [--watch]")
	return fmt.Errorf("invalid command")
}
