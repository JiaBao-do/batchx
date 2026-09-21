package batchx_test

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/JiaBao-do/batchx"
)

// A chunk step copies items with a processor, and the job records progress.
func Example() {
	var out batchx.SliceWriter[string]
	upper := batchx.ProcessorFunc[string, string](func(_ context.Context, s string) (string, error) {
		return strings.ToUpper(s), nil
	})
	step, _ := batchx.NewStep("shout", batchx.NewSliceReader([]string{"a", "b", "c"}), upper, &out, batchx.WithChunkSize(2))
	rec, err := batchx.NewJob("demo", nil).Then(step).Run(context.Background(), nil)
	s, _ := rec.Step("shout")
	fmt.Println(err, rec.Status, out.Items(), s.Commits)
	// Output: <nil> completed [A B C] 2
}

// A failed job resumes from the last committed chunk when run again with the
// same parameters.
func ExampleJob_Run_restart() {
	repo := &batchx.MemoryRepository{}
	var out batchx.SliceWriter[int]
	calls := 0
	w := batchx.WriterFunc[int](func(ctx context.Context, items []int) error {
		if calls++; calls == 2 {
			return errors.New("connection lost")
		}
		return out.Write(ctx, items)
	})
	build := func() batchx.Step {
		s, _ := batchx.NewCopyStep("copy", batchx.NewSliceReader([]int{1, 2, 3, 4, 5, 6}), w, batchx.WithChunkSize(2))
		return s
	}
	params := batchx.Params{"day": "2026-09-21"}

	rec, err := batchx.NewJob("nightly", repo).Then(build()).Run(context.Background(), params)
	s, _ := rec.Step("copy")
	fmt.Println(rec.Status, "checkpoint:", s.Position, err != nil)

	rec, err = batchx.NewJob("nightly", repo).Then(build()).Run(context.Background(), params)
	fmt.Println(rec.Status, rec.Attempts, out.Items(), err)
	// Output:
	// failed checkpoint: 2 true
	// completed 2 [1 2 3 4 5 6] <nil>
}

// Skippable errors are skipped up to the skip limit and reported to listeners.
func ExampleWithSkipLimit() {
	parse := batchx.ProcessorFunc[string, int](func(_ context.Context, s string) (int, error) {
		var n int
		if _, err := fmt.Sscanf(s, "%d", &n); err != nil {
			return 0, batchx.Skippable(err)
		}
		return n, nil
	})
	var out batchx.SliceWriter[int]
	step, _ := batchx.NewStep("parse", batchx.NewSliceReader([]string{"1", "x", "3"}), parse, &out,
		batchx.WithSkipLimit(1),
		batchx.WithStepListener(batchx.Listener{OnSkip: func(_ context.Context, _ string, p batchx.Phase, item any, _ error) {
			fmt.Printf("skipped %v in %s\n", item, p)
		}}))
	_, err := batchx.NewJob("parse", nil).Then(step).Run(context.Background(), nil)
	fmt.Println(out.Items(), err)
	// Output:
	// skipped x in process
	// [1 3] <nil>
}

// Retry with exponential backoff for flaky writers.
func ExampleWithRetry() {
	attempts := 0
	w := batchx.WriterFunc[int](func(context.Context, []int) error {
		if attempts++; attempts < 3 {
			return errors.New("busy")
		}
		return nil
	})
	step, _ := batchx.NewCopyStep("send", batchx.NewSliceReader([]int{1}), w,
		batchx.WithRetry(batchx.Retry{MaxAttempts: 5, Backoff: batchx.ConstantBackoff(0)}))
	_, err := batchx.NewJob("send", nil).Then(step).Run(context.Background(), nil)
	fmt.Println(attempts, err)
	// Output: 3 <nil>
}

// CSV in, JSON lines out.
func ExampleNewCSVReader() {
	r := batchx.NewCSVReader(strings.NewReader("id,name\n1,ann\n2,bob\n"), true)
	rec, _ := r.Read(context.Background())
	fmt.Println(r.Header(), rec)
	// Output: [id name] [1 ann]
}
