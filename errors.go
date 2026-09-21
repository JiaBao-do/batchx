package batchx

import (
	"errors"
	"fmt"
)

var (
	// ErrFilter is returned by a Processor to drop an item without failing.
	// Filtered items are counted in StepRecord.Filtered and never written.
	ErrFilter = errors.New("batchx: item filtered")

	// ErrSkipLimitExceeded wraps the error that pushed a step past its skip limit.
	ErrSkipLimitExceeded = errors.New("batchx: skip limit exceeded")

	// ErrAlreadyCompleted is returned by Job.Run when a job instance with the
	// same name and parameters already completed.
	ErrAlreadyCompleted = errors.New("batchx: job instance already completed")

	// ErrAlreadyRunning is returned by Job.Run when the same job instance is
	// already running in this process.
	ErrAlreadyRunning = errors.New("batchx: job instance already running")

	// ErrNotFound is returned by Repository.Load when no record exists.
	ErrNotFound = errors.New("batchx: job record not found")

	// ErrCheckpointBeyondEnd is returned when a restart checkpoint lies past
	// the end of the input, meaning the input shrank between runs.
	ErrCheckpointBeyondEnd = errors.New("batchx: checkpoint beyond end of input")

	// ErrInvalidConfig is returned for invalid step or job configuration.
	ErrInvalidConfig = errors.New("batchx: invalid configuration")
)

// SkippableError marks an error as eligible for skipping (see WithSkipLimit).
type SkippableError struct{ Err error }

// Error implements error.
func (e *SkippableError) Error() string { return e.Err.Error() }

// Unwrap returns the wrapped error.
func (e *SkippableError) Unwrap() error { return e.Err }

// Skippable wraps err so the default skip predicate accepts it. It returns
// nil for a nil err.
func Skippable(err error) error {
	if err == nil {
		return nil
	}
	return &SkippableError{Err: err}
}

// IsSkippable is the default skip predicate: it reports whether err wraps a
// *SkippableError.
func IsSkippable(err error) bool {
	var s *SkippableError
	return errors.As(err, &s)
}

// StepError reports which step of which job failed.
type StepError struct {
	Job  string
	Step string
	Err  error
}

// Error implements error.
func (e *StepError) Error() string {
	return fmt.Sprintf("batchx: job %q step %q: %v", e.Job, e.Step, e.Err)
}

// Unwrap returns the underlying error.
func (e *StepError) Unwrap() error { return e.Err }
