package cli

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/nicolasmmb/pinax/internal/adapter/ipc"
	"github.com/nicolasmmb/pinax/internal/app/jobs"
)

func runIndex(ctx context.Context, deps serveDeps, path string, stdout io.Writer) error {
	if _, _, err := deps.Validator.ResolveInput(path); err != nil {
		return err
	}
	client, err := ipc.DialWithRetry(ctx, ipc.SocketPath(deps.Config.DatabasePath), 2*time.Second)
	if err != nil {
		return fmt.Errorf("worker offline: %w (start `pinax worker` or `pinax serve`)", err)
	}
	defer client.Close()

	service := jobs.NewService(client, deps.Validator, jobSnapshot(deps.Embedder.Model()))
	job, deduplicated, err := service.Enqueue(ctx, path)
	if err != nil {
		return err
	}
	_, err = fmt.Fprintf(stdout, "job=%s status=%s deduplicated=%t path=%s\n",
		job.ID, job.Status, deduplicated, job.InputPath)
	return err
}
