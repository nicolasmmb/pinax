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
	"github.com/nicolasmmb/pinax/internal/app/ingest"
	"github.com/nicolasmmb/pinax/internal/app/jobs"
	"github.com/nicolasmmb/pinax/internal/app/query"
	"github.com/nicolasmmb/pinax/internal/config"
)

type command struct {
	name       string
	once       bool
	statusArgs []string
}

func parseCommand(args []string, stderr io.Writer) (command, error) {
	if len(args) == 0 {
		return command{}, usage(stderr)
	}
	switch args[0] {
	case "serve":
		if len(args) != 1 {
			return command{}, usage(stderr)
		}
		return command{name: "serve"}, nil
	case "worker":
		flags := flag.NewFlagSet("worker", flag.ContinueOnError)
		flags.SetOutput(stderr)
		once := flags.Bool("once", false, "process the queue and exit when it is empty")
		if err := flags.Parse(args[1:]); err != nil {
			return command{}, err
		}
		if flags.NArg() != 0 {
			return command{}, usage(stderr)
		}
		return command{name: "worker", once: *once}, nil
	case "status":
		return command{name: "status", statusArgs: args[1:]}, nil
	default:
		return command{}, usage(stderr)
	}
}

func runServe(ctx context.Context, deps serveDeps, stderr io.Writer) error {
	logger := newLogger(stderr, deps.Config.LogLevel)
	socketPath := ipc.SocketPath(deps.Config.DatabasePath)
	if deps.Config.WorkerAutostart {
		stop := startWorkerSupervisor(ctx, socketPath, stderr, logger)
		defer stop()
	}
	timeout := 5 * time.Second
	if deps.Config.WorkerAutostart {
		timeout = 15 * time.Second
	}
	client, err := ipc.DialWithRetry(ctx, socketPath, timeout)
	if err != nil {
		return fmt.Errorf("connect to ingest worker: %w (start `pinax worker` or enable PINAX_WORKER_AUTOSTART)", err)
	}
	defer client.Close()
	version, size, overlap := ingest.ChunkerDefaults()
	jobService := jobs.NewService(client, deps.Validator, jobs.SnapshotConfig{
		EmbeddingModel: deps.Embedder.Model(),
		ChunkerVersion: version,
		ChunkSize:      size,
		ChunkOverlap:   overlap,
	})
	queryService := query.New(deps.Embedder, client)
	server := mcpadapter.NewServer(jobService, queryService)
	return server.Run(ctx, &mcp.StdioTransport{})
}

func runWorker(ctx context.Context, deps workerDeps, once bool, stderr io.Writer) error {
	logger := newLogger(stderr, deps.Config.LogLevel)
	ipcServer, err := ipc.Listen(ipc.SocketPath(deps.Config.DatabasePath), deps.Store, deps.Store, logger)
	if err != nil {
		return err
	}
	defer ipcServer.Close()
	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()
	serverErr := make(chan error, 1)
	go func() { serverErr <- ipcServer.Serve(workerCtx) }()

	heartbeat := deps.Config.WorkerHeartbeat
	jobStaleAfter := max(6*heartbeat, 30*time.Second)
	worker, err := jobs.NewWorker(deps.Store, deps.Ingest, jobs.WorkerConfig{
		Workers:       deps.Config.IngestWorkers,
		JobSlots:      deps.Config.WorkerJobSlots,
		Heartbeat:     heartbeat,
		JobStaleAfter: jobStaleAfter,
		Retention:     time.Duration(deps.Config.JobRetentionDays) * 24 * time.Hour,
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
	cmd, err := parseCommand(os.Args[1:], os.Stderr)
	if err != nil {
		return reportError(os.Stderr, err)
	}
	cfg, err := config.Load()
	if err != nil {
		return reportError(os.Stderr, err)
	}
	switch cmd.name {
	case "worker":
		deps, closeDeps, err := newWorkerDeps(ctx, cfg)
		if err != nil {
			return reportError(os.Stderr, err)
		}
		defer closeDeps()
		err = runWorker(ctx, deps, cmd.once, os.Stderr)
		if err != nil {
			return reportError(os.Stderr, err)
		}
	case "status":
		err = runStatus(ctx, statusDeps{Config: cfg}, cmd.statusArgs, os.Stdout, os.Stderr)
		if err != nil {
			return reportError(os.Stderr, err)
		}
	default:
		deps, err := newServeDeps(cfg)
		if err != nil {
			return reportError(os.Stderr, err)
		}
		err = runServe(ctx, deps, os.Stderr)
		if err != nil {
			return reportError(os.Stderr, err)
		}
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
