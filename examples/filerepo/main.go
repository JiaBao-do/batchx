// File repository demo: job state lives in one plain JSON file per job
// instance, so it survives process restarts and can be inspected, backed up or
// deleted with ordinary tools. Run: go run ./examples/filerepo
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/JiaBao-do/batchx"
)

func main() {
	dir, err := os.MkdirTemp("", "batchx-state-")
	if err != nil {
		log.Fatal(err)
	}
	defer func() { _ = os.RemoveAll(dir) }()
	repo, err := batchx.NewFileRepository(dir)
	if err != nil {
		log.Fatal(err)
	}
	clock := func() time.Time { return time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC) } // fixed, for stable output

	fail := true
	writes := 0
	w := batchx.WriterFunc[int](func(context.Context, []int) error {
		if writes++; fail && writes == 2 {
			return errors.New("disk full") // the second chunk fails, the first stays committed
		}
		return nil
	})
	step, err := batchx.NewCopyStep("export", batchx.NewSliceReader([]int{1, 2, 3, 4, 5}), w, batchx.WithChunkSize(2))
	if err != nil {
		log.Fatal(err)
	}
	job := batchx.NewJob("nightly", repo, batchx.WithClock(clock)).Then(step)
	params := batchx.Params{"day": "2026-01-01"}

	_, err = job.Run(context.Background(), params)
	fmt.Println("run 1 error:", err)
	show(dir)

	fail = false // "fix the disk", then restart the same instance
	rec, err := job.Run(context.Background(), params)
	fmt.Println("run 2:", rec.Status, "attempts", rec.Attempts, "err", err)
	show(dir)
}

func show(dir string) {
	files, _ := filepath.Glob(filepath.Join(dir, "*.json"))
	for _, f := range files {
		b, _ := os.ReadFile(f)
		fmt.Printf("%s:\n%s\n", filepath.Base(f), b)
	}
}
