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
	Concurrency   int
	Heartbeat     time.Duration
	JobStaleAfter time.Duration
	Retention     time.Duration
	Once          bool
	Logger        *slog.Logger
}

func (c WorkerConfig) withDefaults() WorkerConfig {
	if c.Concurrency <= 0 {
		c.Concurrency = 4
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
	ctx   context.Context
}

type fileOutcome struct {
	jobID string
}

type planResult struct {
	jobID string
	paths []string
	err   error
}

type jobState struct {
	id              string
	ctx             context.Context
	cancel          context.CancelFunc
	pending         []string
	inFlight        int
	planned         bool
	cancelRequested bool
}

type Worker struct {
	repo     WorkerStore
	ingester Ingester
	cfg      WorkerConfig
	log      *slog.Logger
	name     string
	hostname string
	pid      int
	started  time.Time

	// Owned by the scheduler goroutine.
	jobs    map[string]*jobState
	order   []*jobState
	next    int
	inFiles int

	runCtx      context.Context
	tasks       chan fileTask
	completions chan fileOutcome
	plans       chan planResult

	runners  sync.WaitGroup
	planners sync.WaitGroup
}

func NewWorker(repo WorkerStore, ingester Ingester, cfg WorkerConfig) (*Worker, error) {
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
		repo:        repo,
		ingester:    ingester,
		cfg:         cfg,
		log:         cfg.Logger,
		name:        id,
		hostname:    hostname,
		pid:         os.Getpid(),
		started:     time.Now().UTC(),
		jobs:        make(map[string]*jobState),
		tasks:       make(chan fileTask, cfg.Concurrency),
		completions: make(chan fileOutcome, cfg.Concurrency*2),
		plans:       make(chan planResult, cfg.Concurrency),
	}, nil
}

func (w *Worker) WorkerID() string { return w.name }

func (w *Worker) Run(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	w.runCtx = runCtx

	if err := w.touch(); err != nil {
		return err
	}
	w.log.Info("worker started", "worker_id", w.name, "concurrency", w.cfg.Concurrency)

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
		w.heartbeat(runCtx)
	}()
	for i := 0; i < w.cfg.Concurrency; i++ {
		w.runners.Add(1)
		go func() {
			defer w.runners.Done()
			w.fileRunner(runCtx)
		}()
	}

	err := w.schedule(runCtx)
	cancel()
	background.Wait()
	w.runners.Wait()
	w.planners.Wait()
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

func (w *Worker) heartbeat(ctx context.Context) {
	ticker := time.NewTicker(w.cfg.Heartbeat)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if err := w.repo.TouchWorker(ctx, w.info()); err != nil {
			if ctx.Err() == nil {
				w.log.Warn("touch worker heartbeat", "error", err)
			}
			continue
		}
		ids, err := w.repo.ActiveJobIDs(ctx, w.name)
		if err != nil {
			if ctx.Err() == nil {
				w.log.Warn("list active jobs", "error", err)
			}
			continue
		}
		if err := w.repo.HeartbeatJobs(ctx, ids, w.name, time.Now().UTC()); err != nil && ctx.Err() == nil {
			w.log.Warn("heartbeat jobs", "error", err)
		}
	}
}

func (w *Worker) fileRunner(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-w.tasks:
			w.processFile(task)
		}
	}
}

func (w *Worker) schedule(ctx context.Context) error {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	lastPurge := time.Now()
	w.claim(ctx)
	w.pollCancellations(ctx)

	for {
		w.dispatch()
		if w.cfg.Once && len(w.jobs) == 0 && w.inFiles == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case outcome := <-w.completions:
			w.finishFile(ctx, outcome.jobID)
		case result := <-w.plans:
			w.applyPlan(ctx, result)
		case <-ticker.C:
			w.claim(ctx)
			w.pollCancellations(ctx)
			if time.Since(lastPurge) >= 6*time.Hour {
				w.purge(ctx)
				lastPurge = time.Now()
			}
		}
	}
}

func (w *Worker) claim(ctx context.Context) {
	for len(w.jobs) < w.cfg.Concurrency {
		job, err := w.repo.ClaimNextJob(ctx, w.name, time.Now().UTC())
		if err != nil {
			if ctx.Err() == nil {
				w.log.Warn("claim next job", "error", err)
			}
			return
		}
		if job == nil {
			return
		}
		w.addJob(ctx, job)
	}
}

func (w *Worker) addJob(ctx context.Context, job *domain.Job) {
	model := w.ingester.Model()
	version, size, overlap := w.ingester.Chunker()
	if job.EmbeddingModel != model || job.ChunkerVersion != version || job.ChunkSize != size || job.ChunkOverlap != overlap {
		message := fmt.Sprintf("worker config mismatch: job model=%s chunker=%s/%d/%d, worker model=%s chunker=%s/%d/%d",
			job.EmbeddingModel, job.ChunkerVersion, job.ChunkSize, job.ChunkOverlap,
			model, version, size, overlap)
		_ = w.repo.FailJob(context.Background(), job.ID, message)
		w.log.Warn("job failed because of config mismatch", "job_id", job.ID, "detail", message)
		return
	}

	jobCtx, cancel := context.WithCancel(ctx)
	state := &jobState{id: job.ID, ctx: jobCtx, cancel: cancel}
	w.jobs[job.ID] = state
	w.order = append(w.order, state)
	w.log.Info("job claimed", "job_id", job.ID, "path", job.InputPath)

	if job.CancelRequested {
		state.cancelRequested = true
		cancel()
		w.cancelQueued(job.ID)
		w.finalizeIfDone(context.Background(), state)
		return
	}
	w.startPlanning(job)
}

func (w *Worker) startPlanning(job *domain.Job) {
	state := w.jobs[job.ID]
	w.planners.Add(1)
	go func() {
		defer w.planners.Done()
		paths, err := w.ingester.Plan(state.ctx, job.InputPath)
		select {
		case w.plans <- planResult{jobID: job.ID, paths: paths, err: err}:
		case <-state.ctx.Done():
		}
	}()
}

func (w *Worker) applyPlan(ctx context.Context, result planResult) {
	state := w.jobs[result.jobID]
	if state == nil {
		return
	}
	if result.err != nil {
		if state.ctx.Err() != nil {
			w.finalizeIfDone(ctx, state)
			return
		}
		_ = w.repo.FailJob(context.Background(), state.id, truncateError(result.err))
		w.log.Warn("plan job files", "job_id", state.id, "error", result.err)
		w.removeJob(state)
		return
	}
	if state.ctx.Err() != nil {
		w.finalizeIfDone(ctx, state)
		return
	}
	if err := w.repo.PlanJobFiles(state.ctx, state.id, result.paths); err != nil {
		w.log.Warn("save job files", "job_id", state.id, "error", err)
		_ = w.repo.RequeueJob(context.Background(), state.id, w.name)
		w.removeJob(state)
		return
	}
	queued, err := w.repo.QueuedFiles(state.ctx, state.id)
	if err != nil {
		w.log.Warn("list queued files", "job_id", state.id, "error", err)
		_ = w.repo.RequeueJob(context.Background(), state.id, w.name)
		w.removeJob(state)
		return
	}
	state.pending = queued
	state.planned = true
	if len(state.pending) == 0 {
		w.finalizeIfDone(ctx, state)
		return
	}
	w.dispatch()
}

func (w *Worker) finishFile(ctx context.Context, jobID string) {
	if w.inFiles > 0 {
		w.inFiles--
	}
	state := w.jobs[jobID]
	if state == nil {
		return
	}
	state.inFlight--
	w.finalizeIfDone(ctx, state)
	w.dispatch()
}

func (w *Worker) dispatch() {
	for w.inFiles < w.cfg.Concurrency {
		state := w.pickState()
		if state == nil {
			return
		}
		path := state.pending[0]
		state.pending = state.pending[1:]
		w.tasks <- fileTask{jobID: state.id, path: path, ctx: state.ctx}
		state.inFlight++
		w.inFiles++
	}
}

func (w *Worker) pickState() *jobState {
	if len(w.order) == 0 {
		return nil
	}
	limit := w.perJobLimit()
	for i := 0; i < len(w.order); i++ {
		index := (w.next + i) % len(w.order)
		state := w.order[index]
		if state.ctx.Err() != nil || len(state.pending) == 0 || state.inFlight >= limit {
			continue
		}
		w.next = (index + 1) % len(w.order)
		return state
	}
	return nil
}

func (w *Worker) perJobLimit() int {
	if len(w.jobs) <= 1 {
		return w.cfg.Concurrency
	}
	return max(1, (w.cfg.Concurrency+1)/2)
}

func (w *Worker) pollCancellations(ctx context.Context) {
	for _, state := range w.order {
		if state.cancelRequested {
			continue
		}
		requested, err := w.repo.CancelRequested(context.Background(), state.id)
		if err != nil {
			if ctx.Err() == nil {
				w.log.Warn("check job cancellation", "job_id", state.id, "error", err)
			}
			continue
		}
		if !requested {
			continue
		}
		state.cancelRequested = true
		state.cancel()
		state.pending = nil
		w.cancelQueued(state.id)
		w.log.Info("job cancellation requested", "job_id", state.id)
		w.finalizeIfDone(ctx, state)
	}
}

func (w *Worker) cancelQueued(jobID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := w.repo.CancelQueuedFiles(ctx, jobID); err != nil {
		w.log.Warn("cancel queued files", "job_id", jobID, "error", err)
	}
}

func (w *Worker) finalizeIfDone(ctx context.Context, state *jobState) {
	if !state.planned && !state.cancelRequested {
		// The plan is still running; wait for its result.
		return
	}
	if len(state.pending) > 0 || state.inFlight > 0 {
		return
	}
	job, done, err := w.repo.FinalizeJob(context.Background(), state.id)
	if err != nil {
		if ctx.Err() == nil {
			w.log.Warn("finalize job", "job_id", state.id, "error", err)
		}
		return
	}
	if !done && !job.Status.Terminal() {
		return
	}
	w.removeJob(state)
	w.log.Info("job finished", "job_id", state.id, "status", job.Status)
}

func (w *Worker) removeJob(state *jobState) {
	state.cancel()
	delete(w.jobs, state.id)
	for i, candidate := range w.order {
		if candidate == state {
			w.order = append(w.order[:i], w.order[i+1:]...)
			break
		}
	}
	w.next = 0
}

func (w *Worker) requeueActive() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, state := range w.order {
		state.cancel()
		if state.cancelRequested {
			continue
		}
		if err := w.repo.RequeueJob(ctx, state.id, w.name); err != nil {
			w.log.Warn("requeue job on shutdown", "job_id", state.id, "error", err)
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

func (w *Worker) processFile(task fileTask) {
	defer w.reportOutcome(task.jobID)
	started, err := w.repo.StartJobFile(task.ctx, task.jobID, task.path)
	if err != nil {
		if task.ctx.Err() == nil {
			w.log.Warn("start job file", "job_id", task.jobID, "path", task.path, "error", err)
		}
		return
	}
	if !started {
		return
	}

	lastChunkWrite := time.Time{}
	hooks := ingest.ProgressHooks{
		OnStage: func(stage domain.Stage) {
			if task.ctx.Err() != nil {
				return
			}
			if err := w.repo.SetJobFileStage(task.ctx, task.jobID, task.path, stage); err != nil {
				w.log.Debug("set job file stage", "job_id", task.jobID, "path", task.path, "error", err)
			}
		},
		OnChunks: func(done int) {
			if task.ctx.Err() != nil {
				return
			}
			if time.Since(lastChunkWrite) < 500*time.Millisecond {
				return
			}
			lastChunkWrite = time.Now()
			if err := w.repo.SetJobFileChunks(task.ctx, task.jobID, task.path, done); err != nil {
				w.log.Debug("set job file chunks", "job_id", task.jobID, "path", task.path, "error", err)
			}
		},
	}

	result, ingestErr := w.ingester.IndexFile(task.ctx, task.path, hooks)
	status := result.Status
	chunks := result.Chunks
	message := ""

	finishCtx, finishCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer finishCancel()
	if w.runCtx.Err() != nil {
		if err := w.repo.RequeueJobFile(finishCtx, task.jobID, task.path); err != nil {
			w.log.Warn("requeue job file", "job_id", task.jobID, "path", task.path, "error", err)
		}
		return
	}
	switch {
	case task.ctx.Err() != nil:
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
}

func (w *Worker) reportOutcome(jobID string) {
	select {
	case w.completions <- fileOutcome{jobID: jobID}:
	case <-w.runCtx.Done():
	}
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
