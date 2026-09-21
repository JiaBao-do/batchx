package batchx

import "context"

// Phase identifies where in a chunk an event happened.
type Phase string

// Phase values.
const (
	PhaseRead    Phase = "read"
	PhaseProcess Phase = "process"
	PhaseWrite   Phase = "write"
)

// Listener is a set of optional hooks; nil hooks are ignored, so the zero
// value is a no-op. Hooks run synchronously on the step's goroutine (parallel
// steps call them concurrently, so hooks must be goroutine-safe) and must not
// panic.
type Listener struct {
	// BeforeJob runs after the record is loaded, before any step.
	BeforeJob func(ctx context.Context, rec JobRecord)
	// AfterJob runs after the final record was saved.
	AfterJob func(ctx context.Context, rec JobRecord)
	// BeforeStep runs before a step executes (not for steps skipped as completed).
	BeforeStep func(ctx context.Context, job, step string)
	// AfterStep runs after a step ends, whatever the outcome.
	AfterStep func(ctx context.Context, job string, rec StepRecord)
	// AfterChunk runs after a chunk was committed.
	AfterChunk func(ctx context.Context, job string, rec StepRecord)
	// OnSkip runs when an item is skipped. item is nil for read errors.
	OnSkip func(ctx context.Context, step string, phase Phase, item any, err error)
	// OnRetry runs before each retry sleep; attempt is the failed attempt number (1-based).
	OnRetry func(ctx context.Context, step string, phase Phase, attempt int, err error)
}

type listeners []Listener

func (ls listeners) beforeJob(ctx context.Context, r JobRecord) {
	for _, l := range ls {
		if l.BeforeJob != nil {
			l.BeforeJob(ctx, r)
		}
	}
}

func (ls listeners) afterJob(ctx context.Context, r JobRecord) {
	for _, l := range ls {
		if l.AfterJob != nil {
			l.AfterJob(ctx, r)
		}
	}
}

func (ls listeners) beforeStep(ctx context.Context, job, step string) {
	for _, l := range ls {
		if l.BeforeStep != nil {
			l.BeforeStep(ctx, job, step)
		}
	}
}

func (ls listeners) afterStep(ctx context.Context, job string, r StepRecord) {
	for _, l := range ls {
		if l.AfterStep != nil {
			l.AfterStep(ctx, job, r)
		}
	}
}

func (ls listeners) afterChunk(ctx context.Context, job string, r StepRecord) {
	for _, l := range ls {
		if l.AfterChunk != nil {
			l.AfterChunk(ctx, job, r)
		}
	}
}

func (ls listeners) onSkip(ctx context.Context, step string, p Phase, item any, err error) {
	for _, l := range ls {
		if l.OnSkip != nil {
			l.OnSkip(ctx, step, p, item, err)
		}
	}
}

func (ls listeners) onRetry(ctx context.Context, step string, p Phase, attempt int, err error) {
	for _, l := range ls {
		if l.OnRetry != nil {
			l.OnRetry(ctx, step, p, attempt, err)
		}
	}
}
