package ipc

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"

	"github.com/nicolasmmb/pinax/internal/domain"
)

// SocketPath returns a stable, short Unix socket path for a database file.
func SocketPath(databasePath string) string {
	absolute, err := filepath.Abs(databasePath)
	if err != nil {
		absolute = databasePath
	}
	sum := sha256.Sum256([]byte(absolute))
	return filepath.Join(os.TempDir(), "pinax-"+hex.EncodeToString(sum[:8])+".sock")
}

type EnqueueArgs struct {
	Request domain.EnqueueRequest
}

type EnqueueReply struct {
	Job          domain.Job
	Deduplicated bool
}

type StatusArgs struct {
	JobID string
	Limit int
}

type StatusReply struct {
	Status domain.StatusResult
}

type CancelArgs struct {
	JobID string
}

type CancelReply struct {
	Snapshot domain.JobSnapshot
}

type SearchArgs struct {
	Vector []float32
	Model  string
	Limit  int
}

type SearchReply struct {
	Results []domain.SearchResult
}

type Control interface {
	EnqueueJob(context.Context, domain.EnqueueRequest) (domain.Job, bool, error)
	Status(context.Context, string, int) (domain.StatusResult, error)
	Cancel(context.Context, string) (domain.JobSnapshot, error)
}

type Searcher interface {
	Search(context.Context, []float32, string, int) ([]domain.SearchResult, error)
}
