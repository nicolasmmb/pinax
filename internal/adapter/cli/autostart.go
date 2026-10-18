package cli

import (
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"syscall"
	"time"
)

func startWorkerSupervisor(ctx context.Context, socketPath string, stderr io.Writer, logger *slog.Logger) func() {
	childCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	executable, execErr := os.Executable()
	if execErr != nil {
		logger.Error("resolve executable for worker autostart", "error", execErr)
		close(done)
		return cancel
	}
	go func() {
		defer close(done)
		backoff := time.Second
		for {
			if childCtx.Err() != nil {
				return
			}
			if conn, err := net.Dial("unix", socketPath); err == nil {
				_ = conn.Close()
				logger.Info("ingest worker already running, not starting another")
				return
			}
			started := time.Now()
			cmd := exec.CommandContext(childCtx, executable, "worker")
			cmd.Env = os.Environ()
			cmd.Stdout = stderr
			cmd.Stderr = stderr
			cmd.Cancel = func() error {
				if cmd.Process == nil {
					return nil
				}
				return cmd.Process.Signal(syscall.SIGTERM)
			}
			cmd.WaitDelay = 10 * time.Second
			if err := cmd.Start(); err != nil {
				logger.Warn("start ingest worker", "error", err)
			} else {
				logger.Info("ingest worker started", "pid", cmd.Process.Pid)
				err := cmd.Wait()
				if childCtx.Err() != nil {
					return
				}
				if time.Since(started) > 30*time.Second {
					backoff = time.Second
				}
				logger.Warn("ingest worker exited, restarting", "error", err, "backoff", backoff)
			}
			select {
			case <-childCtx.Done():
				return
			case <-time.After(backoff):
			}
			backoff = min(backoff*2, 30*time.Second)
		}
	}()
	return func() {
		cancel()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			logger.Warn("timed out waiting for ingest worker to stop")
		}
	}
}
