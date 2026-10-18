package main

import (
	"context"
	"io"
	"time"

	"github.com/nicolasmmb/pinax/internal/adapter/ipc"
	"github.com/nicolasmmb/pinax/internal/app/jobs"
)

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
	worker, err := jobs.NewWorker(deps.Store, deps.Ingest, jobs.WorkerConfig{
		Concurrency: deps.Config.WorkerConcurrency, Heartbeat: heartbeat,
		JobStaleAfter: max(6*heartbeat, 30*time.Second),
		Retention:     time.Duration(deps.Config.JobRetentionDays) * 24 * time.Hour,
		Once:          once, Logger: logger,
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
