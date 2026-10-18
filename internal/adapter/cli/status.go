package cli

import (
	"context"
	"flag"
	"fmt"
	"io"
	"text/tabwriter"
	"time"

	"github.com/nicolasmmb/pinax/internal/adapter/ipc"
	"github.com/nicolasmmb/pinax/internal/domain"
)

func runStatus(ctx context.Context, args []string, stdout, stderr io.Writer, runtime Runtime) error {
	flags := flag.NewFlagSet("status", flag.ContinueOnError)
	flags.SetOutput(stderr)
	var watch bool
	flags.BoolVar(&watch, "watch", false, "refresh the view every second")
	flags.BoolVar(&watch, "w", false, "shorthand for --watch")
	limit := flags.Int("limit", 10, "number of recent jobs to show (1-50)")
	flags.IntVar(limit, "n", 10, "shorthand for --limit")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() > 1 {
		return usage(stderr)
	}
	if *limit < 1 || *limit > 50 {
		return fmt.Errorf("limit must be between 1 and 50")
	}
	jobID := flags.Arg(0)

	client, err := ipc.DialWithRetry(ctx, ipc.SocketPath(runtime.Config.DatabasePath), 2*time.Second)
	if err != nil {
		return fmt.Errorf("worker offline: %w (start `pinax worker` or `pinax serve`)", err)
	}
	defer client.Close()

	printOnce := func() error {
		status, err := client.Status(ctx, jobID, *limit)
		if err != nil {
			return err
		}
		renderStatus(stdout, status)
		return nil
	}
	if !watch {
		return printOnce()
	}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		fmt.Fprint(stdout, "\033[H\033[2J")
		if err := printOnce(); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

func renderStatus(w io.Writer, status domain.StatusResult) {
	if status.Worker.WorkerID == "" {
		fmt.Fprintln(w, "worker: offline")
	} else {
		fmt.Fprintf(w, "worker: online=%t stale=%t id=%s pid=%d heartbeat=%s\n",
			status.Worker.Online, status.Worker.Stale, status.Worker.WorkerID,
			status.Worker.PID, status.Worker.HeartbeatAt.Format(time.RFC3339))
	}
	if len(status.Jobs) == 0 {
		fmt.Fprintln(w, "jobs: none")
		return
	}
	fmt.Fprintln(w)
	table := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(table, "JOB\tSTATUS\tPROGRESS\tFILES\tCHUNKS\tPATH")
	for _, job := range status.Jobs {
		fmt.Fprintf(table, "%s\t%s\t%.0f%%\t%d/%d\t%d\t%s\n",
			shortID(job.ID), job.Status, job.Progress.Percent,
			job.Progress.FilesDone, job.Progress.FilesTotal,
			job.Progress.Chunks, job.InputPath)
		for _, file := range job.ActiveFiles {
			stage := string(file.Stage)
			if stage == "" {
				stage = "running"
			}
			fmt.Fprintf(table, "  ↳\t%s\t\t\t%d\t%s\n", stage, file.Chunks, file.Path)
		}
		for _, failure := range job.Failures {
			fmt.Fprintf(table, "  !\tfailed\t\t\t\t%s: %s\n", failure.Path, failure.Error)
		}
		if job.FailuresTruncated {
			fmt.Fprintln(table, "  !\t...\t\t\t\tmore failures not shown")
		}
	}
	_ = table.Flush()
}

func shortID(id string) string {
	if len(id) <= 12 {
		return id
	}
	return id[:12]
}
