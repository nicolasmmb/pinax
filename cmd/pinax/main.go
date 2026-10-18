package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/nicolasmmb/pinax/internal/adapter/cli"
	"github.com/nicolasmmb/pinax/internal/config"
)

func main() {
	os.Exit(Main())
}

func Main() int {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cmd, err := cli.Parse(os.Args[1:], os.Stderr)
	if err != nil {
		return reportError(os.Stderr, err)
	}
	cfg, err := config.Load()
	if err != nil {
		return reportError(os.Stderr, err)
	}
	switch cmd.Name {
	case cli.Worker:
		deps, closeDeps, err := newWorkerDeps(ctx, cfg)
		if err == nil {
			defer closeDeps()
			err = runWorker(ctx, deps, cmd.Once, os.Stderr)
		}
		if err != nil {
			return reportError(os.Stderr, err)
		}
	case cli.Status:
		opts := statusOptions{jobID: cmd.StatusJob, watch: cmd.Watch, limit: cmd.Limit}
		if err := runStatus(ctx, cfg, opts, os.Stdout); err != nil {
			return reportError(os.Stderr, err)
		}
	case cli.Index:
		deps, err := newIndexDeps(cfg)
		if err == nil {
			err = runIndex(ctx, deps, cmd.IndexPath, os.Stdout)
		}
		if err != nil {
			return reportError(os.Stderr, err)
		}
	case cli.Serve:
		deps, err := newMCPDeps(cfg)
		if err == nil {
			err = runServe(ctx, deps)
		}
		if err != nil {
			return reportError(os.Stderr, err)
		}
	default:
		return reportError(os.Stderr, fmt.Errorf("unknown command %q", cmd.Name))
	}
	return 0
}

func reportError(stderr io.Writer, err error) int {
	fmt.Fprintln(stderr, err)
	return 1
}
