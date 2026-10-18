package turso

import (
	"github.com/nicolasmmb/pinax/internal/app/jobs"
)

var (
	_ jobs.Control     = (*Store)(nil)
	_ jobs.WorkerStore = (*Store)(nil)
)
