package domain

import "errors"

var (
	ErrJobNotFound = errors.New("job not found")
	ErrJobNotOwned = errors.New("job not owned by this worker")
)
