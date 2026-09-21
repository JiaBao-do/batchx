// Skip and retry demo: bad CSV rows are skipped (up to a limit), a flaky
// processor call is retried with backoff, and listeners report both.
// Run: go run ./examples/skipretry
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/JiaBao-do/batchx"
)

func main() {
	in := "id,amount\n1,10\n2,not-a-number\n3,30,extra\n4,40\n5,50\n"
	flaky := 0
	parse := batchx.ProcessorFunc[[]string, int](func(_ context.Context, r []string) (int, error) {
		n, err := strconv.Atoi(r[1])
		if err != nil {
			return 0, batchx.Skippable(err) // bad data: skip, do not retry
		}
		if r[0] == "4" && flaky < 2 { // transient failure: retried
			flaky++
			return 0, errors.New("rate limited")
		}
		return n, nil
	})
	var out batchx.SliceWriter[int]
	step, err := batchx.NewStep("amounts", batchx.NewCSVReader(strings.NewReader(in), true), parse, &out,
		batchx.WithChunkSize(2),
		batchx.WithSkipLimit(2),
		batchx.WithRetry(batchx.Retry{
			MaxAttempts: 3,
			Backoff:     batchx.ExponentialBackoff(time.Second, time.Minute),
			If:          func(err error) bool { return !batchx.IsSkippable(err) },
		}),
		batchx.WithSleep(func(context.Context, time.Duration) error { return nil }), // no real waiting in the demo
		batchx.WithStepListener(batchx.Listener{
			OnSkip: func(_ context.Context, _ string, p batchx.Phase, item any, err error) {
				fmt.Printf("skip  [%s] item=%v\n", p, item)
			},
			OnRetry: func(_ context.Context, _ string, p batchx.Phase, attempt int, err error) {
				fmt.Printf("retry [%s] attempt=%d err=%v\n", p, attempt, err)
			},
		}))
	if err != nil {
		log.Fatal(err)
	}
	rec, err := batchx.NewJob("skipretry", nil).Then(step).Run(context.Background(), nil)
	s, _ := rec.Step("amounts")
	fmt.Printf("status=%s read=%d written=%d skipped=%d retries=%d err=%v\n", rec.Status, s.Read, s.Written, s.Skipped, s.Retries, err)
	fmt.Println("amounts:", out.Items())
}
