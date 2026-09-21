// Restart demo: a job crashes in the middle of the input, then the same job is
// run again with the same parameters and resumes from the last committed chunk.
// Run: go run ./examples/restart
package main

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/JiaBao-do/batchx"
)

func main() {
	repo := &batchx.MemoryRepository{} // use batchx.NewFileRepository to survive a real process kill
	params := batchx.Params{"batch": "42"}
	var out batchx.SliceWriter[int]

	crashOnWrite := 3 // the 3rd chunk write fails, simulating a crash
	writes := 0
	w := batchx.WriterFunc[int](func(ctx context.Context, items []int) error {
		writes++
		if writes == crashOnWrite {
			return errors.New("simulated crash")
		}
		fmt.Println("write", items)
		return out.Write(ctx, items)
	})
	items := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}
	run := func() (batchx.JobRecord, error) {
		// A fresh reader and step each run, as after a real restart.
		step, err := batchx.NewCopyStep("copy", batchx.NewSliceReader(items), w, batchx.WithChunkSize(3))
		if err != nil {
			log.Fatal(err)
		}
		return batchx.NewJob("restart-demo", repo).Then(step).Run(context.Background(), params)
	}

	rec, err := run()
	s, _ := rec.Step("copy")
	fmt.Printf("run 1: %s, checkpoint=%d, err=%v\n", rec.Status, s.Position, err)

	rec, err = run()
	s, _ = rec.Step("copy")
	fmt.Printf("run 2: %s, attempts=%d, written=%d, err=%v\n", rec.Status, rec.Attempts, s.Written, err)
	fmt.Println("output:", out.Items())

	_, err = run()
	fmt.Println("run 3:", errors.Is(err, batchx.ErrAlreadyCompleted))
}
