package batchx

import (
	"context"
	"errors"
	"math/rand"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
)

// These tests back the claims in docs/PITFALLS.md. Each name starts with
// TestPitfall and matches a section of that document.

// failNthSave fails Save calls n..n+2 (1-based), simulating a crash after a
// chunk Write but before its checkpoint reaches the repository.
type failNthSave struct {
	Repository
	mu    sync.Mutex
	n     int
	calls int
}

func (f *failNthSave) Save(ctx context.Context, rec JobRecord) error {
	f.mu.Lock()
	f.calls++
	fail := f.calls >= f.n && f.calls < f.n+3 // the process "dies": the failing step's final saves fail too
	f.mu.Unlock()
	if fail {
		return errors.New("crash before checkpoint")
	}
	return f.Repository.Save(ctx, rec)
}

// Pitfall 2: delivery is at-least-once, so writers must be idempotent.
func TestPitfallAtLeastOnceDuplicatesWithAppendWriter(t *testing.T) {
	// Saves: 1 job start, 2 step start, 3 chunk 1, 4 chunk 2 <- crash here.
	repo := &failNthSave{Repository: &MemoryRepository{}, n: 4}

	var appended []int             // WRONG: append-only sink
	upserted := map[int]struct{}{} // RIGHT: keyed upsert, safe to repeat
	w := WriterFunc[int](func(_ context.Context, items []int) error {
		appended = append(appended, items...)
		for _, i := range items {
			upserted[i] = struct{}{}
		}
		return nil
	})
	run := func() error {
		st, _ := NewCopyStep[int]("s", NewSliceReader(ints(6)), w, WithChunkSize(2))
		_, err := NewJob("j", repo).Then(st).Run(context.Background(), nil)
		return err
	}
	if err := run(); err == nil {
		t.Fatal("expected the simulated crash")
	}
	if err := run(); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(appended, []int{0, 1, 2, 3, 2, 3, 4, 5}) {
		t.Fatalf("append writer saw %v; chunk {2,3} should have been written twice", appended)
	}
	if len(upserted) != 6 {
		t.Fatalf("upsert writer must end with 6 distinct keys, got %d", len(upserted))
	}
}

// Pitfall 3: what counts as committed.
func TestPitfallCountersOnlyReflectCommittedChunks(t *testing.T) {
	calls := 0
	w := WriterFunc[int](func(context.Context, []int) error {
		if calls++; calls == 2 {
			return errors.New("boom")
		}
		return nil
	})
	st, _ := NewCopyStep[int]("s", NewSliceReader(ints(6)), w, WithChunkSize(2))
	rec, _ := NewJob("j", &MemoryRepository{}).Then(st).Run(context.Background(), nil)
	s, _ := rec.Step("s")
	// Chunk 2 was read and processed but never committed: it is not in the counters.
	if s.Read != 2 || s.Written != 2 || s.Position != 2 || s.Commits != 1 {
		t.Fatalf("%+v", s)
	}
}

// Pitfall 4: the skip limit is cumulative for the instance; only committed skips count.
func TestPitfallSkipLimitIsCumulativeAcrossRestarts(t *testing.T) {
	repo := &MemoryRepository{}
	bad := errors.New("bad")
	crash := true
	proc := ProcessorFunc[int, int](func(_ context.Context, i int) (int, error) {
		switch {
		case i == 1 || i == 4:
			return 0, Skippable(bad)
		case i == 5 && crash:
			return 0, errors.New("crash")
		}
		return i, nil
	})
	run := func() error {
		st, _ := NewStep[int, int]("s", NewSliceReader(ints(8)), proc, &SliceWriter[int]{}, WithChunkSize(2), WithSkipLimit(2))
		_, err := NewJob("j", repo).Then(st).Run(context.Background(), nil)
		return err
	}
	_ = run() // skip of item 1 is committed; item 4's skip is in the uncommitted chunk that crashed
	crash = false
	if err := run(); err != nil {
		t.Fatalf("second run should fit the limit of 2 (1 committed + 1 new): %v", err)
	}
	rec, _ := repo.Load(context.Background(), Params(nil).key("j"))
	if s, _ := rec.Step("s"); s.Skipped != 2 {
		t.Fatalf("%+v", s)
	}
}

func TestPitfallSkippableErrorsAreRetriedByDefault(t *testing.T) {
	calls := 0
	proc := ProcessorFunc[int, int](func(context.Context, int) (int, error) {
		calls++
		return 0, Skippable(errors.New("bad row"))
	})
	run := func(name string, r Retry) int {
		calls = 0
		st, _ := NewStep[int, int]("s", NewSliceReader(ints(1)), proc, &SliceWriter[int]{}, WithSkipLimit(1), WithRetry(r), WithSleep(noSleep))
		if _, err := NewJob(name, nil).Then(st).Run(context.Background(), nil); err != nil {
			t.Fatal(err)
		}
		return calls
	}
	if got := run("default", Retry{MaxAttempts: 3}); got != 3 {
		t.Fatalf("default retry policy made %d calls, want 3", got)
	}
	if got := run("with-if", Retry{MaxAttempts: 3, If: func(err error) bool { return !IsSkippable(err) }}); got != 1 {
		t.Fatalf("with If: %d calls, want 1", got)
	}
}

// Pitfall 5: readers must replay the same sequence.
func TestPitfallNonDeterministicReaderBreaksRestart(t *testing.T) {
	data := ints(20)
	run := func(order func() []int) []int {
		var w SliceWriter[int]
		crash := true
		calls := 0
		ww := WriterFunc[int](func(ctx context.Context, items []int) error {
			if calls++; calls == 3 && crash {
				return errors.New("crash")
			}
			return w.Write(ctx, items)
		})
		repo := &MemoryRepository{}
		for range 2 {
			// discardReader has no Seek, so the restart replays by re-reading.
			st, _ := NewCopyStep[int]("s", discardReader{NewSliceReader(order())}, ww, WithChunkSize(4))
			_, _ = NewJob("j", repo).Then(st).Run(context.Background(), nil)
			crash = false
		}
		return w.Items()
	}
	// RIGHT: a stable order restarts to exactly the input.
	if got := run(func() []int { return slices.Clone(data) }); !reflect.DeepEqual(got, data) {
		t.Fatalf("stable order: %v", got)
	}
	// WRONG: a different order on each run loses and duplicates items.
	rng := rand.New(rand.NewSource(1))
	got := run(func() []int {
		s := slices.Clone(data)
		rng.Shuffle(len(s), func(i, j int) { s[i], s[j] = s[j], s[i] })
		return s
	})
	slices.Sort(got)
	if reflect.DeepEqual(got, data) {
		t.Fatal("expected the shuffled reader to break coverage; this pitfall demo no longer demonstrates anything")
	}
}

// Pitfall 6: parallel steps run concurrently (see TestParallelStage); the safe
// pattern is one writer per step.
func TestPitfallParallelStepsWithOwnWriters(t *testing.T) {
	var a, b SliceWriter[int]
	s1, _ := NewCopyStep[int]("a", NewSliceReader(ints(100)), &a, WithChunkSize(7))
	s2, _ := NewCopyStep[int]("b", NewSliceReader(ints(100)), &b, WithChunkSize(9))
	if _, err := NewJob("j", nil).ThenParallel(s1, s2).Run(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if len(a.Items()) != 100 || len(b.Items()) != 100 {
		t.Fatal("both steps must complete")
	}
}

// Pitfall 7: stream readers are consumed; build a fresh one for every Run unless it is a Seeker.
func TestPitfallReusingConsumedStreamReader(t *testing.T) {
	in := strings.Repeat("line\n", 10)
	lr := NewLinesReader(strings.NewReader(in))
	calls, written := 0, 0
	ww := WriterFunc[string](func(_ context.Context, items []string) error {
		if calls++; calls == 2 {
			return errors.New("crash")
		}
		written += len(items)
		return nil
	})
	repo := &MemoryRepository{}
	mk := func(r Reader[string]) Step {
		st, _ := NewCopyStep[string]("s", r, ww, WithChunkSize(3))
		return st
	}
	_, _ = NewJob("j", repo).Then(mk(lr)).Run(context.Background(), nil)
	// WRONG: the reader already consumed chunk 2 (uncommitted), so the replay
	// skips the wrong lines. The job "succeeds" but silently misses data.
	if _, err := NewJob("j", repo).Then(mk(lr)).Run(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if written != 4 { // 3 from run 1, only 1 from run 2, instead of 10 in total
		t.Fatalf("written %d, want 4 (silent data loss)", written)
	}
	// RIGHT: a fresh reader for every run.
	written, calls = 0, 0
	repo = &MemoryRepository{}
	_, _ = NewJob("j", repo).Then(mk(NewLinesReader(strings.NewReader(in)))).Run(context.Background(), nil)
	if _, err := NewJob("j", repo).Then(mk(NewLinesReader(strings.NewReader(in)))).Run(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if written != 10 {
		t.Fatalf("written %d, want 10", written)
	}
}

// Pitfall 9: decoding into interface types.
func TestPitfallJSONLIntoAnyGivesFloat64(t *testing.T) {
	r := NewJSONLReader[map[string]any](strings.NewReader(`{"id":12345678901234567890}` + "\n"))
	v, err := r.Read(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := v["id"].(float64); !ok {
		t.Fatalf("%T", v["id"])
	}
	// RIGHT: a typed struct keeps integers exact.
	type row struct {
		ID uint64 `json:"id"`
	}
	r2 := NewJSONLReader[row](strings.NewReader(`{"id":12345678901234567890}` + "\n"))
	if got, err := r2.Read(context.Background()); err != nil || got.ID != 12345678901234567890 {
		t.Fatalf("%v %v", got, err)
	}
}

// Pitfall 10: identity is job name + params; step names are the checkpoint keys.
func TestPitfallRenamedStepForgetsProgress(t *testing.T) {
	repo := &MemoryRepository{}
	var w SliceWriter[int]
	calls := 0
	ww := WriterFunc[int](func(ctx context.Context, items []int) error {
		if calls++; calls == 2 {
			return errors.New("crash")
		}
		return w.Write(ctx, items)
	})
	mk := func(name string) Step {
		st, _ := NewCopyStep[int](name, NewSliceReader(ints(6)), ww, WithChunkSize(2))
		return st
	}
	_, _ = NewJob("j", repo).Then(mk("import")).Run(context.Background(), nil)
	// Renaming the step in a new release starts it from position 0 again.
	if _, err := NewJob("j", repo).Then(mk("import-v2")).Run(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	if got := w.Items(); len(got) != 8 { // 2 from the first run + all 6 again
		t.Fatalf("%v", got)
	}
}

// Pitfall 8: cancellation only interrupts a read if the reader honours the context.
func TestPitfallCancellationNeedsContextAwareReader(t *testing.T) {
	ch := make(chan int) // never produces
	ctx, cancel := context.WithCancel(context.Background())
	go cancel()
	st, _ := NewCopyStep[int]("s", NewChanReader(ch), &SliceWriter[int]{})
	rec, err := NewJob("j", nil).Then(st).Run(ctx, nil)
	if !errors.Is(err, context.Canceled) || rec.Status != StatusStopped {
		t.Fatalf("%v %s", err, rec.Status)
	}
}

// Pitfall 1: every chunk is one repository save; chunk size trades memory and latency for commit cost.
func TestPitfallChunkSizeControlsSaveCount(t *testing.T) {
	count := func(chunk int) int {
		repo := &countingRepo{Repository: &MemoryRepository{}}
		st, _ := NewCopyStep[int]("s", NewSliceReader(ints(100)), &SliceWriter[int]{}, WithChunkSize(chunk))
		if _, err := NewJob("j", repo).Then(st).Run(context.Background(), nil); err != nil {
			t.Fatal(err)
		}
		return repo.saves
	}
	// 100 items: chunk 1 -> 100 checkpoints, chunk 50 -> 2, plus 4 saves for job/step start and end.
	if a, b := count(1), count(50); a != 104 || b != 6 {
		t.Fatalf("saves: chunk1=%d chunk50=%d", a, b)
	}
}

type countingRepo struct {
	Repository
	saves int
}

func (c *countingRepo) Save(ctx context.Context, rec JobRecord) error {
	c.saves++
	return c.Repository.Save(ctx, rec)
}
