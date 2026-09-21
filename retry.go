package batchx

import (
	"context"
	"errors"
	"time"
)

// Retry configures retrying of a failing processor call (per item) or writer
// call (per chunk). The zero value never retries.
type Retry struct {
	// MaxAttempts is the total number of attempts including the first.
	// Values below 2 disable retrying.
	MaxAttempts int
	// Backoff returns the wait before the next attempt; attempt is the number
	// of attempts made so far (1-based). Nil means no wait.
	Backoff func(attempt int) time.Duration
	// If decides whether an error is retryable. Nil retries every error
	// except ErrFilter and context cancellation.
	If func(error) bool
}

// ConstantBackoff waits d between attempts.
func ConstantBackoff(d time.Duration) func(int) time.Duration {
	return func(int) time.Duration { return d }
}

// ExponentialBackoff doubles the wait from base each attempt, capped at max
// (max <= 0 means no cap). There is no jitter, keeping behaviour deterministic.
func ExponentialBackoff(base, max time.Duration) func(int) time.Duration {
	return func(attempt int) time.Duration {
		d := base
		for i := 1; i < attempt; i++ {
			d *= 2
			if max > 0 && d >= max {
				return max
			}
			if d <= 0 { // overflow
				return max
			}
		}
		if max > 0 && d > max {
			return max
		}
		return d
	}
}

func (r Retry) retryable(err error) bool {
	if errors.Is(err, ErrFilter) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if r.If != nil {
		return r.If(err)
	}
	return true
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
