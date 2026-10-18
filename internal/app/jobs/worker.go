package jobs

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/nicolasmmb/pinax/internal/app/ingest"
	"github.com/nicolasmmb/pinax/internal/domain"
)

type WorkerConfig struct {
	WorkerID      string
	Workers       int
	JobSlots      int
	Heartbeat     time.Duration
	JobStaleAfter time.Duration
	Retention     time.Duration
	Once          bool
	Logger        *slog.Logger
}

func (c WorkerConfig) withDefaults() WorkerConfig {
	if c.Workers <= 0 {
		c.Workers = 4
	}
	if c.JobSlots <= 0 {
		c.JobSlots = 4
	}
	if c.Heartbeat <= 0 {
		c.Heartbeat = 5 * time.Second
	}
	if c.JobStaleAfter <= 0 {
		c.JobStaleAfter = 30 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return c
}

type fileTask struct {
	jobID string
	path  string
}

type fileOutcome struct {
	jobID string
}

type jobRuntime struct {
	job           domain.Job
	ctx           context.Context
	cancel        context.CancelFunc
	cancelPending bool
}

type Worker struct {
	repo     Repository
	ingester Ingester
	cfg      WorkerConfig
	log      *slog.Logger
	name     string
	hostname string
	pid      int
	started  time.Time

	mu      sync.RWMutex
	active  map[string]*jobRuntime
	feeders sync.WaitGroup

	tasks    chan fileTask
	outcomes chan fileOutcome
}

func NewWorker(repo Repository, ingester Ingester, cfg WorkerConfig) (*Worker, error) {
	cfg = cfg.withDefaults()
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	id := cfg.WorkerID
	if id == "" {
		var raw [8]byte
		if _, err := rand.Read(raw[:]); err != nil {
			return nil, fmt.Errorf("generate worker id: %w", err)
		}
		id = fmt.Sprintf("%s-%d-%s", hostname, os.Getpid(), hex.EncodeToString(raw[:]))
	}
	return &Worker{
		repo:     repo,
		ingester: ingester,
		cfg:      cfg,
		log:      cfg.Logger,
		name:     id,
		hostname: hostname,
		pid:      os.Getpid(),
		started:  time.Now().UTC(),
		active:   make(map[string]*jobRuntime),
		tasks:    make(chan fileTask, cfg.Workers*4),
		outcomes: make(chan fileOutcome, cfg.Workers*4),
	}, nil
}

func (w *Worker) WorkerID() string { return w.name }

func (w *Worker) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	if err := w.touch(); err != nil {
		return err
	}
	w.log.Info("worker started", "worker_id", w.name, "workers", w.cfg.Workers, "job_slots", w.cfg.JobSlots)

	if recovered, err := w.repo.RecoverStaleJobs(runCtx, time.Now().UTC(), w.cfg.JobStaleAfter); err != nil {
		w.log.Warn("recover stale jobs", "error", err)
	} else if recovered > 0 {
		w.log.Info("recovered stale jobs", "count", recovered)
	}
	w.purge(runCtx)

	var background sync.WaitGroup
	background.Add(1)
	go func() {
		defer background.Done()
		w.heartbeat(runCtx, cancel)
	}()
	for i := 0; i < w.cfg.Workers; i++ {
		background.Add(1)
		go func() {
			defer background.Done()
			w.fileWorker(runCtx)
		}()
	}

	err := w.schedule(runCtx)
	cancel()
	background.Wait()
	w.feeders.Wait()
	w.requeueActive()
	clearCtx, clearCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer clearCancel()
	if err := w.repo.ClearWorker(clearCtx); err != nil {
		w.log.Warn("clear worker heartbeat", "error", err)
	}
	return err
}

func (w *Worker) touch() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return w.repo.TouchWorker(ctx, w.info())
}

func (w *Worker) info() domain.WorkerInfo {
	return domain.WorkerInfo{
		WorkerID:    w.name,
		PID:         w.pid,
		Hostname:    w.hostname,
		StartedAt:   w.started,
		HeartbeatAt: time.Now().UTC(),
	}
}

func (w *Worker) heartbeat(ctx context.Context, cancel context.CancelFunc) {
	ticker := time.NewTicker(w.cfg.Heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		now := time.Now().UTC()
		if err := w.repo.TouchWorker(ctx, w.info()); err != nil {
			if ctx.Err() == nil {
				w.log.Warn("touch worker heartbeat", "error", err)
			}
			continue
		}
		runtimes := w.runtimes()
		if len(runtimes) == 0 {
			continue
		}
		ids := make([]string, 0, len(runtimes))
		for _, rt := range runtimes {
			ids = append(ids, rt.job.ID)
		}
		if err := w.repo.HeartbeatJobs(ctx, ids, w.name, now); err != nil && ctx.Err() == nil {
			w.log.Warn("heartbeat jobs", "error", err)
		}
	}
}

func (w *Worker) fileWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-w.tasks:
			w.processFile(ctx, task)
		}
	}
}

func (w *Worker) processFile(runCtx context.Context, task fileTask) {
	rt := w.runtime(task.jobID)
	if rt == nil || rt.ctx.Err() != nil {
		return
	}
	started, err := w.repo.StartJobFile(rt.ctx, task.jobID, task.path)
	if err != nil {
		if rt.ctx.Err() == nil {
			w.log.Warn("start job file", "job_id", task.jobID, "path", task.path, "error", err)
		}
		w.reportOutcome(runCtx, task.jobID)
		return
	}
	if !started {
		w.reportOutcome(runCtx, task.jobID)
		return
	}

	lastChunkWrite := time.Time{}
	hooks := ingest.ProgressHooks{
		OnStage: func(stage domain.Stage) {
			if rt.ctx.Err() != nil {
				return
			}
			if err := w.repo.SetJobFileStage(rt.ctx, task.jobID, task.path, stage); err != nil {
				w.log.Debug("set job file stage", "job_id", task.jobID, "path", task.path, "error", err)
			}
		},
		OnChunks: func(done int) {
			if rt.ctx.Err() != nil {
				return
			}
			if time.Since(lastChunkWrite) < 500*time.Millisecond {
				return
			}
			lastChunkWrite = time.Now()
			if err := w.repo.SetJobFileChunks(rt.ctx, task.jobID, task.path, done); err != nil {
				w.log.Debug("set job file chunks", "job_id", task.jobID, "path", task.path, "error", err)
			}
		},
	}

	result, ingestErr := w.ingester.IndexFile(rt.ctx, task.path, hooks)
	status := result.Status
	chunks := result.Chunks
	message := ""

	finishCtx, finishCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer finishCancel()
	if runCtx.Err() != nil {
		if err := w.repo.RequeueJobFile(finishCtx, task.jobID, task.path); err != nil {
			w.log.Warn("requeue job file", "job_id", task.jobID, "path", task.path, "error", err)
		}
		return
	}
	switch {
	case rt.ctx.Err() != nil:
		status = domain.FileCanceled
	case !status.Terminal():
		status = domain.FileFailed
		if ingestErr != nil {
			message = truncateError(ingestErr)
		} else {
			message = "index failed"
		}
	case status == domain.FileFailed:
		if ingestErr != nil {
			message = truncateError(ingestErr)
		} else {
			message = "index failed"
		}
	}
	if err := w.repo.FinishJobFile(finishCtx, task.jobID, task.path, status, chunks, message); err != nil {
		w.log.Warn("finish job file", "job_id", task.jobID, "path", task.path, "error", err)
	}
	w.reportOutcome(runCtx, task.jobID)
}

func (w *Worker) reportOutcome(ctx context.Context, jobID string) {
	select {
	case w.outcomes <- fileOutcome{jobID: jobID}:
	case <-ctx.Done():
	}
}

func (w *Worker) schedule(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	lastPurge := time.Now()
	w.claimJobs(ctx)
	w.pollCancellations(ctx)

	for {
		select {
		case <-ctx.Done():
			return nil
		case outcome := <-w.outcomes:
			w.finalizeIfDone(ctx, outcome.jobID)
		case <-ticker.C:
			claimed := w.claimJobs(ctx)
			w.pollCancellations(ctx)
			if time.Since(lastPurge) >= 6*time.Hour {
				w.purge(ctx)
				lastPurge = time.Now()
			}
			if w.cfg.Once && claimed == 0 && w.activeCount() == 0 {
				return nil
			}
		}
	}
}

func (w *Worker) claimJobs(ctx context.Context) int {
	claimed := 0
	for w.activeCount() < w.cfg.JobSlots {
		job, err := w.repo.ClaimNextJob(ctx, w.name, time.Now().UTC())
		if err != nil {
			if ctx.Err() == nil {
				w.log.Warn("claim next job", "error", err)
			}
			return claimed
		}
		if job == nil {
			return claimed
		}
		claimed++
		if err := w.addJob(ctx, job); err != nil {
			w.log.Warn("add job", "job_id", job.ID, "error", err)
		}
	}
	return claimed
}

func (w *Worker) addJob(ctx context.Context, job *domain.Job) error {
	model := w.ingester.Model()
	version, size, overlap := w.ingester.Chunker()
	if job.EmbeddingModel != model || job.ChunkerVersion != version || job.ChunkSize != size || job.ChunkOverlap != overlap {
		message := fmt.Sprintf("worker config mismatch: job model=%s chunker=%s/%d/%d, worker model=%s chunker=%s/%d/%d",
			job.EmbeddingModel, job.ChunkerVersion, job.ChunkSize, job.ChunkOverlap,
			model, version, size, overlap)
		_ = w.repo.FailJob(context.Background(), job.ID, message)
		w.log.Warn("job failed because of config mismatch", "job_id", job.ID, "detail", message)
		return nil
	}

	jobCtx, cancel := context.WithCancel(ctx)
	rt := &jobRuntime{job: *job, ctx: jobCtx, cancel: cancel}
	w.mu.Lock()
	w.active[job.ID] = rt
	w.mu.Unlock()
	w.log.Info("job claimed", "job_id", job.ID, "path", job.InputPath)

	if job.CancelRequested {
		rt.cancelPending = true
		cancel()
		w.cancelQueued(job.ID)
		w.finalizeIfDone(ctx, job.ID)
		return nil
	}

	if !job.PlanningDone {
		if err := w.repo.ResetPlanning(jobCtx, job.ID); err != nil {
			w.removeJob(job.ID)
			_ = w.repo.RequeueJob(context.Background(), job.ID, w.name)
			return fmt.Errorf("reset planning: %w", err)
		}
		paths, err := w.ingester.Plan(jobCtx, job.InputPath)
		if err != nil {
			w.removeJob(job.ID)
			if jobCtx.Err() != nil {
				_ = w.repo.RequeueJob(context.Background(), job.ID, w.name)
				return nil
			}
			_ = w.repo.FailJob(context.Background(), job.ID, truncateError(err))
			return fmt.Errorf("plan job files: %w", err)
		}
		if err := w.repo.PlanJobFiles(jobCtx, job.ID, paths); err != nil {
			w.removeJob(job.ID)
			_ = w.repo.RequeueJob(context.Background(), job.ID, w.name)
			return fmt.Errorf("save job files: %w", err)
		}
	}

	queued, err := w.repo.QueuedFiles(jobCtx, job.ID)
	if err != nil {
		w.removeJob(job.ID)
		_ = w.repo.RequeueJob(context.Background(), job.ID, w.name)
		return fmt.Errorf("list queued files: %w", err)
	}
	if len(queued) == 0 {
		w.finalizeIfDone(ctx, job.ID)
		return nil
	}
	w.feeders.Add(1)
	go func() {
		defer w.feeders.Done()
		for _, path := range queued {
			select {
			case w.tasks <- fileTask{jobID: job.ID, path: path}:
			case <-jobCtx.Done():
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	return nil
}

func (w *Worker) pollCancellations(ctx context.Context) {
	for _, rt := range w.runtimes() {
		if rt.cancelPending {
			continue
		}
		requested, err := w.repo.CancelRequested(context.Background(), rt.job.ID)
		if err != nil {
			if ctx.Err() == nil {
				w.log.Warn("check job cancellation", "job_id", rt.job.ID, "error", err)
			}
			continue
		}
		if !requested {
			continue
		}
		rt.cancelPending = true
		rt.cancel()
		w.cancelQueued(rt.job.ID)
		w.log.Info("job cancellation requested", "job_id", rt.job.ID)
		w.finalizeIfDone(ctx, rt.job.ID)
	}
}

func (w *Worker) cancelQueued(jobID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.repo.CancelQueuedFiles(ctx, jobID); err != nil {
		w.log.Warn("cancel queued files", "job_id", jobID, "error", err)
	}
}

func (w *Worker) finalizeIfDone(ctx context.Context, jobID string) {
	if w.runtime(jobID) == nil {
		return
	}
	job, done, err := w.repo.FinalizeJob(context.Background(), jobID)
	if err != nil {
		if ctx.Err() == nil {
			w.log.Warn("finalize job", "job_id", jobID, "error", err)
		}
		return
	}
	if !done && !job.Status.Terminal() {
		return
	}
	w.removeJob(jobID)
	w.log.Info("job finished", "job_id", jobID, "status", job.Status)
}

func (w *Worker) requeueActive() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, rt := range w.runtimes() {
		rt.cancel()
		if rt.cancelPending {
			continue
		}
		if err := w.repo.RequeueJob(ctx, rt.job.ID, w.name); err != nil {
			w.log.Warn("requeue job on shutdown", "job_id", rt.job.ID, "error", err)
		}
	}
}

func (w *Worker) purge(ctx context.Context) {
	if w.cfg.Retention <= 0 {
		return
	}
	purged, err := w.repo.PurgeJobs(ctx, time.Now().UTC().Add(-w.cfg.Retention))
	if err != nil {
		w.log.Warn("purge jobs", "error", err)
		return
	}
	if purged > 0 {
		w.log.Info("purged finished jobs", "count", purged)
	}
}

func (w *Worker) runtime(jobID string) *jobRuntime {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.active[jobID]
}

func (w *Worker) runtimes() []*jobRuntime {
	w.mu.RLock()
	defer w.mu.RUnlock()
	result := make([]*jobRuntime, 0, len(w.active))
	for _, rt := range w.active {
		result = append(result, rt)
	}
	return result
}

func (w *Worker) removeJob(jobID string) {
	w.mu.Lock()
	rt := w.active[jobID]
	delete(w.active, jobID)
	w.mu.Unlock()
	if rt != nil {
		rt.cancel()
	}
}

func (w *Worker) activeCount() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return len(w.active)
}

func truncateError(err error) string {
	if err == nil {
		return ""
	}
	const limit = 500
	message := err.Error()
	if len(message) > limit {
		return message[:limit]
	}
	return message
}
