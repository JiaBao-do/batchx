package batchx

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func ints(n int) []int {
	s := make([]int, n)
	for i := range s {
		s[i] = i
	}
	return s
}

func mustStep(t testing.TB) func(Step, error) Step {
	return func(s Step, err error) Step {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
		return s
	}
}

func noSleep(context.Context, time.Duration) error { return nil }

func TestChunkBoundaries(t *testing.T) {
	for _, tc := range []struct{ n, chunk, commits int }{
		{0, 3, 0}, {1, 3, 1}, {3, 3, 1}, {4, 3, 2}, {9, 3, 3}, {10, 1, 10}, {5, 100, 1},
	} {
		t.Run(fmt.Sprintf("n%d_c%d", tc.n, tc.chunk), func(t *testing.T) {
			var sizes []int
			var w SliceWriter[int]
			cw := WriterFunc[int](func(ctx context.Context, items []int) error {
				sizes = append(sizes, len(items))
				return w.Write(ctx, items)
			})
			st, err := NewCopyStep[int]("s", NewSliceReader(ints(tc.n)), cw, WithChunkSize(tc.chunk))
			if err != nil {
				t.Fatal(err)
			}
			rec, err := NewJob("j", nil).Then(st).Run(context.Background(), nil)
			if err != nil {
				t.Fatal(err)
			}
			s, _ := rec.Step("s")
			if s.Commits != int64(tc.commits) || s.Read != int64(tc.n) || s.Written != int64(tc.n) || s.Position != int64(tc.n) {
				t.Fatalf("record %+v", s)
			}
			for i, sz := range sizes {
				if sz > tc.chunk || (i < len(sizes)-1 && sz != tc.chunk) {
					t.Fatalf("chunk sizes %v", sizes)
				}
			}
			if !reflect.DeepEqual(w.Items(), append([]int(nil), ints(tc.n)...)) && tc.n > 0 {
				t.Fatalf("items %v", w.Items())
			}
			if rec.Status != StatusCompleted {
				t.Fatalf("status %s", rec.Status)
			}
		})
	}
}

func TestConfigValidation(t *testing.T) {
	r := NewSliceReader(ints(1))
	w := &SliceWriter[int]{}
	if _, err := NewCopyStep[int]("", r, w); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal(err)
	}
	if _, err := NewCopyStep[int]("s", r, w, WithChunkSize(0)); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal(err)
	}
	if _, err := NewCopyStep[int]("s", r, w, WithSkipLimit(-1)); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal(err)
	}
	if _, err := NewStep[int, int]("s", nil, nil, w); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal(err)
	}
	st, _ := NewCopyStep[int]("s", r, w)
	if _, err := NewJob("", nil).Then(st).Run(context.Background(), nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal(err)
	}
	if _, err := NewJob("j", nil).Run(context.Background(), nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal(err)
	}
	if _, err := NewJob("j", nil).Then(st).Then(st).Run(context.Background(), nil); !errors.Is(err, ErrInvalidConfig) {
		t.Fatal("duplicate step names should be rejected")
	}
}

func TestProcessorFilterAndSkip(t *testing.T) {
	bad := errors.New("bad")
	var skipped []any
	proc := ProcessorFunc[int, int](func(_ context.Context, i int) (int, error) {
		switch {
		case i%5 == 0:
			return 0, ErrFilter
		case i%7 == 0:
			return 0, Skippable(bad)
		}
		return i * 2, nil
	})
	var w SliceWriter[int]
	st, _ := NewStep[int, int]("s", NewSliceReader(ints(20)), proc, &w, WithChunkSize(4), WithSkipLimit(5),
		WithStepListener(Listener{OnSkip: func(_ context.Context, _ string, p Phase, item any, err error) {
			if p != PhaseProcess || !errors.Is(err, bad) {
				t.Errorf("phase %s err %v", p, err)
			}
			skipped = append(skipped, item)
		}}))
	rec, err := NewJob("j", nil).Then(st).Run(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	s, _ := rec.Step("s")
	if s.Filtered != 4 || s.Skipped != 2 || s.Written != 14 || s.Read != 20 {
		t.Fatalf("%+v", s)
	}
	if !reflect.DeepEqual(skipped, []any{7, 14}) {
		t.Fatalf("skipped %v", skipped)
	}
}

func TestSkipLimitExceeded(t *testing.T) {
	proc := ProcessorFunc[int, int](func(_ context.Context, i int) (int, error) {
		if i%2 == 0 {
			return 0, Skippable(errors.New("bad"))
		}
		return i, nil
	})
	st, _ := NewStep[int, int]("s", NewSliceReader(ints(10)), proc, &SliceWriter[int]{}, WithSkipLimit(2))
	rec, err := NewJob("j", nil).Then(st).Run(context.Background(), nil)
	if !errors.Is(err, ErrSkipLimitExceeded) {
		t.Fatal(err)
	}
	var se *StepError
	if !errors.As(err, &se) || se.Step != "s" || rec.Status != StatusFailed {
		t.Fatalf("err %v status %s", err, rec.Status)
	}
}

func TestNonSkippableErrorFails(t *testing.T) {
	boom := errors.New("boom")
	proc := ProcessorFunc[int, int](func(context.Context, int) (int, error) { return 0, boom })
	st, _ := NewStep[int, int]("s", NewSliceReader(ints(3)), proc, &SliceWriter[int]{}, WithSkipLimit(100))
	_, err := NewJob("j", nil).Then(st).Run(context.Background(), nil)
	if !errors.Is(err, boom) || errors.Is(err, ErrSkipLimitExceeded) {
		t.Fatal(err)
	}
}

func TestReadErrorSkip(t *testing.T) {
	i := 0
	r := ReaderFunc[int](func(context.Context) (int, error) {
		i++
		switch {
		case i == 3:
			return 0, Skippable(errors.New("garbled"))
		case i > 6:
			return 0, errors.New("wrapped: " + "eof")
		}
		return i, nil
	})
	// A non-EOF, non-skippable read error fails the step.
	var w SliceWriter[int]
	st, _ := NewCopyStep[int]("s", r, &w, WithChunkSize(2), WithSkipLimit(1))
	rec, err := NewJob("j", nil).Then(st).Run(context.Background(), nil)
	if err == nil {
		t.Fatal("expected read failure")
	}
	s, _ := rec.Step("s")
	// chunks {1,2} and {skip,4,5} commit (skips do not fill a chunk); the third reads 6 then fails on the 7th read, uncommitted.
	if !reflect.DeepEqual(w.Items(), []int{1, 2, 4, 5}) {
		t.Fatalf("items %v", w.Items())
	}
	if s.Skipped != 1 || s.Position != 5 || s.Status != StatusFailed {
		t.Fatalf("%+v", s)
	}
}

func TestRetryProcessAndWrite(t *testing.T) {
	var procCalls, writeCalls atomic.Int32
	var retries []int
	proc := ProcessorFunc[int, int](func(_ context.Context, i int) (int, error) {
		if i == 1 && procCalls.Add(1) < 3 {
			return 0, errors.New("transient")
		}
		return i, nil
	})
	var w SliceWriter[int]
	ww := WriterFunc[int](func(ctx context.Context, items []int) error {
		if writeCalls.Add(1) == 1 {
			return errors.New("transient write")
		}
		return w.Write(ctx, items)
	})
	var waits []time.Duration
	st, _ := NewStep[int, int]("s", NewSliceReader(ints(3)), proc, ww, WithChunkSize(3),
		WithRetry(Retry{MaxAttempts: 3, Backoff: ExponentialBackoff(10*time.Millisecond, 15*time.Millisecond)}),
		WithSleep(func(_ context.Context, d time.Duration) error { waits = append(waits, d); return nil }),
		WithStepListener(Listener{OnRetry: func(_ context.Context, _ string, _ Phase, a int, _ error) { retries = append(retries, a) }}))
	rec, err := NewJob("j", nil).Then(st).Run(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	s, _ := rec.Step("s")
	if s.Retries != 3 || !reflect.DeepEqual(retries, []int{1, 2, 1}) {
		t.Fatalf("retries %d %v", s.Retries, retries)
	}
	if !reflect.DeepEqual(waits, []time.Duration{10 * time.Millisecond, 15 * time.Millisecond, 10 * time.Millisecond}) {
		t.Fatalf("waits %v", waits)
	}
	if !reflect.DeepEqual(w.Items(), []int{0, 1, 2}) {
		t.Fatal(w.Items())
	}
}

func TestRetryExhaustedAndPredicate(t *testing.T) {
	boom := errors.New("boom")
	var calls int
	proc := ProcessorFunc[int, int](func(context.Context, int) (int, error) { calls++; return 0, boom })
	st, _ := NewStep[int, int]("s", NewSliceReader(ints(1)), proc, &SliceWriter[int]{},
		WithRetry(Retry{MaxAttempts: 4}), WithSleep(noSleep))
	if _, err := NewJob("j", nil).Then(st).Run(context.Background(), nil); !errors.Is(err, boom) || calls != 4 {
		t.Fatalf("calls %d err %v", calls, err)
	}
	calls = 0
	st, _ = NewStep[int, int]("s", NewSliceReader(ints(1)), proc, &SliceWriter[int]{},
		WithRetry(Retry{MaxAttempts: 4, If: func(error) bool { return false }}))
	if _, err := NewJob("j2", nil).Then(st).Run(context.Background(), nil); !errors.Is(err, boom) || calls != 1 {
		t.Fatalf("calls %d err %v", calls, err)
	}
}

func TestExponentialBackoff(t *testing.T) {
	b := ExponentialBackoff(time.Second, 5*time.Second)
	want := []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 5 * time.Second, 5 * time.Second}
	for i, w := range want {
		if got := b(i + 1); got != w {
			t.Errorf("attempt %d: %v want %v", i+1, got, w)
		}
	}
	if got := ExponentialBackoff(time.Second, 0)(3); got != 4*time.Second {
		t.Errorf("uncapped: %v", got)
	}
	if got := ConstantBackoff(time.Second)(9); got != time.Second {
		t.Error(got)
	}
	if got := ExponentialBackoff(time.Hour, 0)(200); got < 0 {
		t.Errorf("overflow %v", got)
	}
}

func TestWriteScanIsolatesBadItem(t *testing.T) {
	bad := errors.New("bad row")
	var w SliceWriter[int]
	var skipped []any
	ww := WriterFunc[int](func(ctx context.Context, items []int) error {
		for _, i := range items {
			if i == 5 {
				return Skippable(bad)
			}
		}
		return w.Write(ctx, items)
	})
	st, _ := NewCopyStep[int]("s", NewSliceReader(ints(9)), ww, WithChunkSize(4), WithSkipLimit(1),
		WithStepListener(Listener{OnSkip: func(_ context.Context, _ string, p Phase, item any, _ error) {
			if p != PhaseWrite {
				t.Error(p)
			}
			skipped = append(skipped, item)
		}}))
	rec, err := NewJob("j", nil).Then(st).Run(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(w.Items(), []int{0, 1, 2, 3, 4, 6, 7, 8}) || !reflect.DeepEqual(skipped, []any{5}) {
		t.Fatalf("items %v skipped %v", w.Items(), skipped)
	}
	if s, _ := rec.Step("s"); s.Skipped != 1 || s.Written != 8 {
		t.Fatalf("%+v", s)
	}
}

func TestWriteFailNonSkippable(t *testing.T) {
	boom := errors.New("disk")
	ww := WriterFunc[int](func(context.Context, []int) error { return boom })
	st, _ := NewCopyStep[int]("s", NewSliceReader(ints(3)), ww, WithSkipLimit(9))
	if _, err := NewJob("j", nil).Then(st).Run(context.Background(), nil); !errors.Is(err, boom) {
		t.Fatal(err)
	}
}

func TestCancellationStopsAndRestarts(t *testing.T) {
	repo := &MemoryRepository{}
	ctx, cancel := context.WithCancel(context.Background())
	var w SliceWriter[int]
	var chunks int
	ww := WriterFunc[int](func(c context.Context, items []int) error {
		chunks++
		if chunks == 3 {
			cancel() // cancel while chunk 3 is being written: it still commits
		}
		return w.Write(c, items)
	})
	mk := func() Step {
		return mustStep(t)(NewCopyStep[int]("s", NewSliceReader(ints(10)), ww, WithChunkSize(2)))
	}
	rec, err := NewJob("j", repo).Then(mk()).Run(ctx, Params{"d": "1"})
	if !errors.Is(err, context.Canceled) || rec.Status != StatusStopped {
		t.Fatalf("status %s err %v", rec.Status, err)
	}
	s, _ := rec.Step("s")
	if s.Status != StatusStopped || s.Position != 6 {
		t.Fatalf("%+v", s)
	}
	rec, err = NewJob("j", repo).Then(mk()).Run(context.Background(), Params{"d": "1"})
	if err != nil || rec.Attempts != 2 {
		t.Fatalf("attempts %d err %v", rec.Attempts, err)
	}
	if !reflect.DeepEqual(w.Items(), ints(10)) {
		t.Fatalf("items %v", w.Items())
	}
}

func TestRestartSkipsCompletedSteps(t *testing.T) {
	repo := &MemoryRepository{}
	var aRuns, bRuns int
	fail := true
	a := NewTaskletStep("a", func(context.Context) error { aRuns++; return nil })
	b := NewTaskletStep("b", func(context.Context) error {
		bRuns++
		if fail {
			return errors.New("flaky")
		}
		return nil
	})
	job := NewJob("j", repo).Then(a).Then(b)
	rec, err := job.Run(context.Background(), nil)
	if err == nil || rec.Status != StatusFailed || rec.Err == "" {
		t.Fatalf("%v %+v", err, rec)
	}
	fail = false
	rec, err = job.Run(context.Background(), nil)
	if err != nil || aRuns != 1 || bRuns != 2 || rec.Status != StatusCompleted {
		t.Fatalf("a=%d b=%d err=%v", aRuns, bRuns, err)
	}
	if _, err = job.Run(context.Background(), nil); !errors.Is(err, ErrAlreadyCompleted) {
		t.Fatalf("want ErrAlreadyCompleted, got %v", err)
	}
	// different params = new instance
	if _, err = job.Run(context.Background(), Params{"x": "y"}); err != nil {
		t.Fatal(err)
	}
}

func TestAlreadyRunning(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	job := NewJob("j", nil).Then(NewTaskletStep("a", func(context.Context) error { close(started); <-release; return nil }))
	done := make(chan error)
	go func() { _, err := job.Run(context.Background(), nil); done <- err }()
	<-started
	if _, err := job.Run(context.Background(), nil); !errors.Is(err, ErrAlreadyRunning) {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestParallelStage(t *testing.T) {
	var running, peak atomic.Int32
	mk := func(name string) Step {
		return NewTaskletStep(name, func(ctx context.Context) error {
			n := running.Add(1)
			for {
				p := peak.Load()
				if n <= p || peak.CompareAndSwap(p, n) {
					break
				}
			}
			time.Sleep(30 * time.Millisecond)
			running.Add(-1)
			return nil
		})
	}
	rec, err := NewJob("j", nil).ThenParallel(mk("a"), mk("b"), mk("c")).Then(mk("d")).Run(context.Background(), nil)
	if err != nil || len(rec.Steps) != 4 || peak.Load() < 2 {
		t.Fatalf("err %v steps %d peak %d", err, len(rec.Steps), peak.Load())
	}
}

func TestParallelFailureCancelsSiblings(t *testing.T) {
	boom := errors.New("boom")
	fail := NewTaskletStep("fail", func(context.Context) error { return boom })
	slow := NewTaskletStep("slow", func(ctx context.Context) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(10 * time.Second):
			return nil
		}
	})
	rec, err := NewJob("j", nil).ThenParallel(slow, fail).Run(context.Background(), nil)
	if !errors.Is(err, boom) || rec.Status != StatusFailed {
		t.Fatalf("err %v status %s", err, rec.Status)
	}
	if s, _ := rec.Step("slow"); s.Status != StatusStopped {
		t.Fatalf("slow: %+v", s)
	}
}

func TestListenersOrder(t *testing.T) {
	var mu sync.Mutex
	var ev []string
	add := func(s string) { mu.Lock(); ev = append(ev, s); mu.Unlock() }
	l := Listener{
		BeforeJob:  func(context.Context, JobRecord) { add("bj") },
		AfterJob:   func(_ context.Context, r JobRecord) { add("aj:" + string(r.Status)) },
		BeforeStep: func(_ context.Context, _, s string) { add("bs:" + s) },
		AfterStep:  func(_ context.Context, _ string, r StepRecord) { add("as:" + string(r.Status)) },
		AfterChunk: func(_ context.Context, _ string, r StepRecord) { add(fmt.Sprintf("ac:%d", r.Written)) },
	}
	st, _ := NewCopyStep[int]("s", NewSliceReader(ints(4)), &SliceWriter[int]{}, WithChunkSize(2))
	if _, err := NewJob("j", nil, WithListener(l)).Then(st).Run(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	want := []string{"bj", "bs:s", "ac:2", "ac:4", "as:completed", "aj:completed"}
	if !reflect.DeepEqual(ev, want) {
		t.Fatalf("%v", ev)
	}
	// zero Listener is a no-op
	var z Listener
	st, _ = NewCopyStep[int]("s", NewSliceReader(ints(1)), &SliceWriter[int]{})
	if _, err := NewJob("j", nil, WithListener(z)).Then(st).Run(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
}

type closeTracker struct {
	SliceReader[int]
	closed bool
	err    error
}

func (c *closeTracker) Close() error { c.closed = true; return c.err }

func TestReaderClosed(t *testing.T) {
	r := &closeTracker{SliceReader: *NewSliceReader(ints(2))}
	st, _ := NewCopyStep[int]("s", r, &SliceWriter[int]{})
	if _, err := NewJob("j", nil).Then(st).Run(context.Background(), nil); err != nil || !r.closed {
		t.Fatal(err, r.closed)
	}
	r2 := &closeTracker{SliceReader: *NewSliceReader(ints(2)), err: errors.New("close failed")}
	st, _ = NewCopyStep[int]("s", r2, &SliceWriter[int]{})
	if _, err := NewJob("j2", nil).Then(st).Run(context.Background(), nil); err == nil {
		t.Fatal("close error should surface")
	}
}

// discardReader has no Seek, exercising the replay path.
type discardReader struct{ r *SliceReader[int] }

func (d discardReader) Read(ctx context.Context) (int, error) { return d.r.Read(ctx) }

func TestRestartReplayWithoutSeeker(t *testing.T) {
	repo := &MemoryRepository{}
	var w SliceWriter[int]
	calls := 0
	ww := WriterFunc[int](func(c context.Context, items []int) error {
		calls++
		if calls == 3 {
			return errors.New("crash")
		}
		return w.Write(c, items)
	})
	mk := func() Step {
		return mustStep(t)(NewCopyStep[int]("s", discardReader{NewSliceReader(ints(10))}, ww, WithChunkSize(3)))
	}
	if _, err := NewJob("j", repo).Then(mk()).Run(context.Background(), nil); err == nil {
		t.Fatal("expected crash")
	}
	if _, err := NewJob("j", repo).Then(mk()).Run(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(w.Items(), ints(10)) {
		t.Fatal(w.Items())
	}
}

func TestCheckpointBeyondEnd(t *testing.T) {
	repo := &MemoryRepository{}
	key := Params(nil).key("j")
	_ = repo.Save(context.Background(), JobRecord{Version: recordVersion, Name: "j", Key: key,
		Status: StatusFailed, Steps: []StepRecord{{Name: "s", Status: StatusFailed, Position: 50}}})
	for _, r := range []Reader[int]{NewSliceReader(ints(3)), discardReader{NewSliceReader(ints(3))}} {
		st, _ := NewCopyStep[int]("s", r, &SliceWriter[int]{})
		if _, err := NewJob("j", repo).Then(st).Run(context.Background(), nil); !errors.Is(err, ErrCheckpointBeyondEnd) {
			t.Fatalf("%T: %v", r, err)
		}
	}
}

// ---- property test against a naive reference ----

type kind int

const (
	kOK kind = iota
	kFilter
	kSkip
)

func classify(i int, seed int64) kind {
	h := (int64(i)*2654435761 + seed) % 10
	if h < 0 {
		h = -h
	}
	switch h {
	case 0:
		return kFilter
	case 1:
		return kSkip
	}
	return kOK
}

// naive is the reference implementation: no chunks, no repository.
func naive(n int, seed int64) (out []int, filtered, skipped int64) {
	for i := range n {
		switch classify(i, seed) {
		case kOK:
			out = append(out, i*3)
		case kFilter:
			filtered++
		case kSkip:
			skipped++
		}
	}
	return
}

func TestPropertyMatchesNaiveWithCrashes(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	bad := errors.New("bad")
	for iter := range 300 {
		n := rng.Intn(60)
		chunk := 1 + rng.Intn(9)
		seed := rng.Int63n(1000)
		crashes := rng.Intn(4) // number of crashes before success
		crashAt := make([]int, crashes)
		for i := range crashAt {
			crashAt[i] = 1 + rng.Intn(6) // crash on the k-th write call of that attempt
		}
		useFile := iter%3 == 0
		var repo Repository = &MemoryRepository{}
		if useFile {
			fr, err := NewFileRepository(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			repo = fr
		}
		var w SliceWriter[int]
		proc := ProcessorFunc[int, int](func(_ context.Context, i int) (int, error) {
			switch classify(i, seed) {
			case kFilter:
				return 0, ErrFilter
			case kSkip:
				return 0, Skippable(bad)
			}
			return i * 3, nil
		})
		attempt := 0
		var rec JobRecord
		var err error
		for {
			writes := 0
			crashOn := -1
			if attempt < crashes {
				crashOn = crashAt[attempt]
			}
			ww := WriterFunc[int](func(ctx context.Context, items []int) error {
				writes++
				if writes == crashOn {
					return errors.New("crash")
				}
				return w.Write(ctx, items)
			})
			st, e := NewStep[int, int]("s", NewSliceReader(ints(n)), proc, ww, WithChunkSize(chunk), WithSkipLimit(n+1))
			if e != nil {
				t.Fatal(e)
			}
			rec, err = NewJob("j", repo).Then(st).Run(context.Background(), nil)
			attempt++
			if err == nil {
				break
			}
			if attempt > crashes+1 {
				t.Fatalf("iter %d: unexpected failure %v", iter, err)
			}
		}
		wantOut, wantF, wantS := naive(n, seed)
		got := w.Items()
		if len(got) != len(wantOut) || (len(got) > 0 && !reflect.DeepEqual(got, wantOut)) {
			t.Fatalf("iter %d (n=%d chunk=%d crashes=%v): got %v want %v", iter, n, chunk, crashAt, got, wantOut)
		}
		s, _ := rec.Step("s")
		if s.Filtered != wantF || s.Skipped != wantS || s.Written != int64(len(wantOut)) || s.Read != int64(n) || s.Position != int64(n) {
			t.Fatalf("iter %d: counters %+v want f=%d s=%d w=%d", iter, s, wantF, wantS, len(wantOut))
		}
	}
}
