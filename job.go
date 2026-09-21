package batchx

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Job is an ordered list of stages; each stage is one step or several steps
// run in parallel. Build it with NewJob, Then and ThenParallel, then call Run.
// A Job is safe for concurrent Run calls on different parameters.
type Job struct {
	name      string
	repo      Repository
	stages    [][]Step
	listeners listeners
	now       func() time.Time

	mu      sync.Mutex
	running map[string]bool
}

// JobOption configures a Job.
type JobOption func(*Job)

// WithListener adds a job-level listener; it also receives chunk, skip and
// retry events from every step.
func WithListener(l Listener) JobOption { return func(j *Job) { j.listeners = append(j.listeners, l) } }

// WithClock replaces time.Now (useful for deterministic tests).
func WithClock(now func() time.Time) JobOption { return func(j *Job) { j.now = now } }

// NewJob creates a job named name persisting to repo. A nil repo uses a new
// MemoryRepository, which makes restarts work only within the process.
func NewJob(name string, repo Repository, opts ...JobOption) *Job {
	if repo == nil {
		repo = &MemoryRepository{}
	}
	j := &Job{name: name, repo: repo, now: time.Now, running: map[string]bool{}}
	for _, o := range opts {
		o(j)
	}
	return j
}

// Name returns the job name.
func (j *Job) Name() string { return j.name }

// Then appends a sequential stage with one step and returns j.
func (j *Job) Then(s Step) *Job { return j.ThenParallel(s) }

// ThenParallel appends a stage whose steps run concurrently. The stage ends
// when all steps end; if one fails the others are cancelled.
func (j *Job) ThenParallel(steps ...Step) *Job {
	if len(steps) > 0 {
		j.stages = append(j.stages, steps)
	}
	return j
}

func (j *Job) validate() error {
	if j.name == "" {
		return fmt.Errorf("%w: empty job name", ErrInvalidConfig)
	}
	if len(j.stages) == 0 {
		return fmt.Errorf("%w: job %q has no steps", ErrInvalidConfig, j.name)
	}
	seen := map[string]bool{}
	for _, st := range j.stages {
		for _, s := range st {
			if s == nil || s.Name() == "" || seen[s.Name()] {
				return fmt.Errorf("%w: job %q has a nil, unnamed or duplicate step", ErrInvalidConfig, j.name)
			}
			seen[s.Name()] = true
		}
	}
	return nil
}

// run is the state of one Job.Run call.
type run struct {
	j   *Job
	mu  sync.Mutex
	rec JobRecord
}

func (r *run) saveStep(ctx context.Context, s StepRecord) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	found := false
	for i := range r.rec.Steps {
		if r.rec.Steps[i].Name == s.Name {
			r.rec.Steps[i] = s
			found = true
			break
		}
	}
	if !found {
		r.rec.Steps = append(r.rec.Steps, s)
	}
	r.rec.UpdatedAt = r.j.now()
	return r.j.repo.Save(context.WithoutCancel(ctx), r.rec)
}

func (r *run) saveJob(ctx context.Context, mutate func(*JobRecord)) (JobRecord, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	mutate(&r.rec)
	r.rec.UpdatedAt = r.j.now()
	err := r.j.repo.Save(context.WithoutCancel(ctx), r.rec)
	return r.rec.Clone(), err
}

func (r *run) stepRecord(name string) StepRecord {
	r.mu.Lock()
	defer r.mu.Unlock()
	s, _ := r.rec.Step(name)
	return s
}

func statusFor(err error) Status {
	switch {
	case err == nil:
		return StatusCompleted
	case isCtxErr(err):
		return StatusStopped
	default:
		return StatusFailed
	}
}

// Run executes the job instance identified by the job name and params.
//
// If the repository holds a completed record for the instance, Run returns it
// with ErrAlreadyCompleted. If it holds a failed, stopped or running record
// (a previous crash), Run restarts: completed steps are skipped and the
// failed step resumes from its last committed checkpoint. The returned record
// is the final persisted state, valid even when err is non-nil.
func (j *Job) Run(ctx context.Context, params Params) (JobRecord, error) {
	if err := j.validate(); err != nil {
		return JobRecord{}, err
	}
	key := params.key(j.name)
	j.mu.Lock()
	if j.running[key] {
		j.mu.Unlock()
		return JobRecord{}, ErrAlreadyRunning
	}
	j.running[key] = true
	j.mu.Unlock()
	defer func() { j.mu.Lock(); delete(j.running, key); j.mu.Unlock() }()

	rec, err := j.repo.Load(ctx, key)
	switch {
	case errors.Is(err, ErrNotFound):
		rec = JobRecord{Version: recordVersion, Name: j.name, Key: key, Params: params, StartedAt: j.now()}
	case err != nil:
		return JobRecord{}, fmt.Errorf("batchx: load job record: %w", err)
	case rec.Status == StatusCompleted:
		return rec, ErrAlreadyCompleted
	}
	r := &run{j: j, rec: rec}
	snap, err := r.saveJob(ctx, func(jr *JobRecord) {
		jr.Attempts++
		jr.Status = StatusRunning
		jr.Err = ""
		jr.EndedAt = time.Time{}
	})
	if err != nil {
		return snap, fmt.Errorf("batchx: save job record: %w", err)
	}
	j.listeners.beforeJob(ctx, snap)

	var runErr error
	for _, stage := range j.stages {
		if runErr = j.runStage(ctx, r, stage); runErr != nil {
			break
		}
	}

	final, serr := r.saveJob(ctx, func(jr *JobRecord) {
		jr.Status = statusFor(runErr)
		jr.EndedAt = j.now()
		if runErr != nil {
			jr.Err = runErr.Error()
		}
	})
	if serr != nil && runErr == nil {
		runErr = fmt.Errorf("batchx: save final job record: %w", serr)
	}
	j.listeners.afterJob(ctx, final)
	return final, runErr
}

func (j *Job) runStage(ctx context.Context, r *run, stage []Step) error {
	if len(stage) == 1 {
		return j.runStep(ctx, r, stage[0])
	}
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	errs := make([]error, len(stage))
	var wg sync.WaitGroup
	for i, s := range stage {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if errs[i] = j.runStep(cctx, r, s); errs[i] != nil {
				cancel()
			}
		}()
	}
	wg.Wait()
	// Prefer the root cause over the cancellations it triggered.
	var first error
	for _, e := range errs {
		if e == nil {
			continue
		}
		if !isCtxErr(e) {
			return e
		}
		if first == nil {
			first = e
		}
	}
	return first
}

func (j *Job) runStep(ctx context.Context, r *run, s Step) error {
	rec := r.stepRecord(s.Name())
	if rec.Status == StatusCompleted {
		return nil
	}
	rec.Name = s.Name()
	rec.Status = StatusRunning
	rec.Attempts++
	rec.Err = ""
	rec.StartedAt = j.now()
	rec.EndedAt = time.Time{}
	sc := &StepContext{Job: j.name, Record: rec, run: r, ls: j.listeners}
	if err := r.saveStep(ctx, sc.Record); err != nil {
		return &StepError{Job: j.name, Step: s.Name(), Err: fmt.Errorf("save step record: %w", err)}
	}
	j.listeners.beforeStep(ctx, j.name, s.Name())
	err := s.Execute(ctx, sc)
	sc.Record.Status = statusFor(err)
	sc.Record.EndedAt = j.now()
	if err != nil {
		sc.Record.Err = err.Error()
	}
	if serr := r.saveStep(ctx, sc.Record); serr != nil && err == nil {
		err = fmt.Errorf("save step record: %w", serr)
		sc.Record.Status = StatusFailed
	}
	j.listeners.afterStep(ctx, j.name, sc.Record)
	if err != nil {
		return &StepError{Job: j.name, Step: s.Name(), Err: err}
	}
	return nil
}
