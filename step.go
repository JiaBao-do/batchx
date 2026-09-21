package batchx

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"
)

// Reader produces items. Read returns io.EOF (or an error wrapping it) when
// the input is exhausted. A Reader is used by one goroutine at a time.
//
// A read error that the step treats as skippable must leave the reader
// positioned at the next item, so the step can continue.
type Reader[I any] interface {
	Read(ctx context.Context) (I, error)
}

// Seeker is an optional Reader capability: Seek positions the reader after
// its first n positions (items plus skipped read errors) so a restart does not
// have to re-read them. Without it, batchx reads and discards n positions.
type Seeker interface {
	Seek(ctx context.Context, n int64) error
}

// Processor transforms one item. Return ErrFilter to drop the item.
// Implementations must be goroutine-safe only if shared between steps.
type Processor[I, O any] interface {
	Process(ctx context.Context, in I) (O, error)
}

// Writer writes one chunk. A chunk write should be all-or-nothing: if Write
// returns an error, none of the chunk may be considered written, because
// batchx may retry it, or (for skippable errors) re-write its items one at a
// time to isolate the bad one. Because a crash can happen after Write returns
// but before the checkpoint is saved, writers should be idempotent to get
// effectively-once results; batchx itself gives at-least-once per chunk.
type Writer[O any] interface {
	Write(ctx context.Context, items []O) error
}

// ReaderFunc adapts a function to Reader.
type ReaderFunc[I any] func(ctx context.Context) (I, error)

// Read implements Reader.
func (f ReaderFunc[I]) Read(ctx context.Context) (I, error) { return f(ctx) }

// ProcessorFunc adapts a function to Processor.
type ProcessorFunc[I, O any] func(ctx context.Context, in I) (O, error)

// Process implements Processor.
func (f ProcessorFunc[I, O]) Process(ctx context.Context, in I) (O, error) { return f(ctx, in) }

// WriterFunc adapts a function to Writer.
type WriterFunc[O any] func(ctx context.Context, items []O) error

// Write implements Writer.
func (f WriterFunc[O]) Write(ctx context.Context, items []O) error { return f(ctx, items) }

// Step is one unit of a Job. Execute receives the step's persisted record in
// sc.Record (empty counters on a first run, the last committed state on a
// restart) and must return nil only when the step is fully done.
type Step interface {
	Name() string
	Execute(ctx context.Context, sc *StepContext) error
}

// StepContext is handed to Step.Execute.
type StepContext struct {
	// Job is the job name.
	Job string
	// Record is the step's state; steps update it and call Commit.
	Record StepRecord

	run *run
	ls  listeners
}

// Commit persists sc.Record. It uses a context detached from cancellation so
// a checkpoint for work already done is never lost to a shutdown.
func (sc *StepContext) Commit(ctx context.Context) error {
	return sc.run.saveStep(ctx, sc.Record)
}

// StepOption configures a chunk step.
type StepOption func(*stepConfig)

type stepConfig struct {
	chunk     int
	skipLimit int64
	skipIf    func(error) bool
	retry     Retry
	sleep     func(context.Context, time.Duration) error
	listeners listeners
}

// WithChunkSize sets the commit interval: items per chunk (default 10).
func WithChunkSize(n int) StepOption { return func(c *stepConfig) { c.chunk = n } }

// WithSkipLimit allows up to n skipped items per step across all runs of the
// instance (default 0: any error fails the step).
func WithSkipLimit(n int) StepOption { return func(c *stepConfig) { c.skipLimit = int64(n) } }

// WithSkipIf sets which errors are skippable (default IsSkippable).
func WithSkipIf(f func(error) bool) StepOption { return func(c *stepConfig) { c.skipIf = f } }

// WithRetry sets the retry policy for processor and writer calls.
func WithRetry(r Retry) StepOption { return func(c *stepConfig) { c.retry = r } }

// WithSleep replaces the backoff sleeper (useful for deterministic tests).
func WithSleep(f func(context.Context, time.Duration) error) StepOption {
	return func(c *stepConfig) { c.sleep = f }
}

// WithStepListener adds a listener for this step only.
func WithStepListener(l Listener) StepOption {
	return func(c *stepConfig) { c.listeners = append(c.listeners, l) }
}

// chunkStep is the chunk-oriented Step.
type chunkStep[I, O any] struct {
	name string
	r    Reader[I]
	p    Processor[I, O]
	w    Writer[O]
	cfg  stepConfig
}

// NewStep builds a chunk-oriented step: read up to a chunk of items, process
// each, write the chunk, then commit a checkpoint. A Step is single-use per
// run: it holds the reader and writer, so do not run one instance from two
// jobs at once. If r or w implement io.Closer they are closed when the step
// ends.
func NewStep[I, O any](name string, r Reader[I], p Processor[I, O], w Writer[O], opts ...StepOption) (Step, error) {
	cfg := stepConfig{chunk: 10, skipIf: IsSkippable, sleep: sleepCtx}
	for _, o := range opts {
		o(&cfg)
	}
	switch {
	case name == "":
		return nil, fmt.Errorf("%w: empty step name", ErrInvalidConfig)
	case r == nil || p == nil || w == nil:
		return nil, fmt.Errorf("%w: step %q needs reader, processor and writer", ErrInvalidConfig, name)
	case cfg.chunk < 1:
		return nil, fmt.Errorf("%w: chunk size %d", ErrInvalidConfig, cfg.chunk)
	case cfg.skipLimit < 0:
		return nil, fmt.Errorf("%w: negative skip limit", ErrInvalidConfig)
	}
	return &chunkStep[I, O]{name: name, r: r, p: p, w: w, cfg: cfg}, nil
}

// NewCopyStep is NewStep without a processor: items pass through unchanged.
func NewCopyStep[T any](name string, r Reader[T], w Writer[T], opts ...StepOption) (Step, error) {
	return NewStep[T, T](name, r, ProcessorFunc[T, T](func(_ context.Context, in T) (T, error) { return in, nil }), w, opts...)
}

type taskletStep struct {
	name string
	fn   func(context.Context) error
}

// NewTaskletStep builds a step that runs fn once. It is restarted from
// scratch if it did not complete.
func NewTaskletStep(name string, fn func(context.Context) error) Step {
	return &taskletStep{name: name, fn: fn}
}

func (t *taskletStep) Name() string { return t.name }

func (t *taskletStep) Execute(ctx context.Context, _ *StepContext) error { return t.fn(ctx) }

func (s *chunkStep[I, O]) Name() string { return s.name }

// delta accumulates the counters of the chunk in flight; it is applied to the
// record only after the chunk's write succeeds.
type delta struct {
	read, written, filtered, skipped, retries, positions int64
}

func (s *chunkStep[I, O]) skip(ctx context.Context, sc *StepContext, d *delta, ph Phase, item any, err error) error {
	if !s.cfg.skipIf(err) {
		return fmt.Errorf("%s: %w", ph, err)
	}
	if sc.Record.Skipped+d.skipped >= s.cfg.skipLimit {
		return fmt.Errorf("%w: %s: %w", ErrSkipLimitExceeded, ph, err)
	}
	d.skipped++
	sc.ls.onSkip(ctx, s.name, ph, item, err)
	return nil
}

func isCtxErr(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

func withRetry[T any](ctx context.Context, s *stepConfig, sc *StepContext, name string, d *delta, ph Phase, fn func() (T, error)) (T, error) {
	for attempt := 1; ; attempt++ {
		v, err := fn()
		if err == nil {
			return v, nil
		}
		if attempt >= s.retry.MaxAttempts || !s.retry.retryable(err) {
			return v, err
		}
		d.retries++
		sc.ls.onRetry(ctx, name, ph, attempt, err)
		var wait time.Duration
		if s.retry.Backoff != nil {
			wait = s.retry.Backoff(attempt)
		}
		if serr := s.sleep(ctx, wait); serr != nil {
			return v, serr
		}
	}
}

// Execute implements Step.
func (s *chunkStep[I, O]) Execute(ctx context.Context, sc *StepContext) (err error) {
	defer func() {
		for _, c := range []any{s.r, s.w} {
			if cl, ok := c.(io.Closer); ok {
				if cerr := cl.Close(); cerr != nil && err == nil {
					err = fmt.Errorf("close: %w", cerr)
				}
			}
		}
	}()
	sc.ls = append(append(listeners(nil), sc.ls...), s.cfg.listeners...)
	if sc.Record.Position > 0 {
		if err := s.seek(ctx, sc.Record.Position); err != nil {
			return err
		}
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		done, err := s.chunk(ctx, sc)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
}

func (s *chunkStep[I, O]) seek(ctx context.Context, n int64) error {
	if sk, ok := s.r.(Seeker); ok {
		if err := sk.Seek(ctx, n); err != nil {
			return fmt.Errorf("seek to checkpoint %d: %w", n, err)
		}
		return nil
	}
	for i := int64(0); i < n; i++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		_, err := s.r.Read(ctx)
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("%w: %d of %d positions", ErrCheckpointBeyondEnd, i, n)
		}
		if err != nil && !s.cfg.skipIf(err) {
			return fmt.Errorf("replay to checkpoint: %w", err)
		}
	}
	return nil
}

// chunk runs one read/process/write/commit cycle and reports whether the
// input is exhausted.
func (s *chunkStep[I, O]) chunk(ctx context.Context, sc *StepContext) (bool, error) {
	var d delta
	items := make([]I, 0, s.cfg.chunk)
	eof := false
	for len(items) < s.cfg.chunk {
		it, err := s.r.Read(ctx)
		if errors.Is(err, io.EOF) {
			eof = true
			break
		}
		if err != nil {
			if isCtxErr(err) {
				return false, err
			}
			if serr := s.skip(ctx, sc, &d, PhaseRead, nil, err); serr != nil {
				return false, serr
			}
			d.positions++
			continue
		}
		d.positions++
		d.read++
		items = append(items, it)
	}

	outs := make([]O, 0, len(items))
	for _, it := range items {
		o, err := withRetry(ctx, &s.cfg, sc, s.name, &d, PhaseProcess, func() (O, error) { return s.p.Process(ctx, it) })
		switch {
		case err == nil:
			outs = append(outs, o)
		case errors.Is(err, ErrFilter):
			d.filtered++
		case isCtxErr(err):
			return false, err
		default:
			if serr := s.skip(ctx, sc, &d, PhaseProcess, it, err); serr != nil {
				return false, serr
			}
		}
	}

	if len(outs) > 0 {
		_, err := withRetry(ctx, &s.cfg, sc, s.name, &d, PhaseWrite, func() (struct{}, error) { return struct{}{}, s.w.Write(ctx, outs) })
		switch {
		case err == nil:
			d.written += int64(len(outs))
		case isCtxErr(err) || !s.cfg.skipIf(err):
			return false, fmt.Errorf("write: %w", err)
		default:
			// Scan mode: isolate the bad item(s) by writing one at a time.
			for _, o := range outs {
				_, err := withRetry(ctx, &s.cfg, sc, s.name, &d, PhaseWrite, func() (struct{}, error) { return struct{}{}, s.w.Write(ctx, []O{o}) })
				if err == nil {
					d.written++
					continue
				}
				if isCtxErr(err) {
					return false, err
				}
				if serr := s.skip(ctx, sc, &d, PhaseWrite, o, err); serr != nil {
					return false, serr
				}
			}
		}
	}

	if d.positions == 0 && d.filtered == 0 {
		return true, nil
	}
	r := &sc.Record
	r.Position += d.positions
	r.Read += d.read
	r.Written += d.written
	r.Filtered += d.filtered
	r.Skipped += d.skipped
	r.Retries += d.retries
	r.Commits++
	if err := sc.Commit(ctx); err != nil {
		return false, fmt.Errorf("commit checkpoint: %w", err)
	}
	sc.ls.afterChunk(ctx, sc.Job, *r)
	return eof, nil
}
