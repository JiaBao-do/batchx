# batchx

[![ci](https://github.com/JiaBao-do/batchx/actions/workflows/ci.yml/badge.svg)](https://github.com/JiaBao-do/batchx/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/JiaBao-do/batchx.svg)](https://pkg.go.dev/github.com/JiaBao-do/batchx)

Chunk-oriented batch processing for Go, in the spirit of Spring Batch. Generic reader, processor and writer,
jobs made of steps, restart from the last committed chunk, skip and retry policies, listeners, and a pluggable
job repository. Standard library only, no CGO. Requires Go 1.24+.

    go get github.com/JiaBao-do/batchx

## Quick start

```go
// Quickstart: read CSV, process each row, write JSON lines, in chunks of 2.
// Run: go run ./examples/quickstart
package main

import (
	"context"
	"log"
	"os"
	"strings"

	"github.com/JiaBao-do/batchx"
)

type User struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func main() {
	in := strings.NewReader("id,name\n1,ann\n2,bob\n3,cy\n")
	toUser := batchx.ProcessorFunc[[]string, User](func(_ context.Context, r []string) (User, error) {
		return User{ID: r[0], Name: strings.ToUpper(r[1])}, nil
	})
	step, err := batchx.NewStep("import", batchx.NewCSVReader(in, true), toUser,
		batchx.NewJSONLWriter[User](os.Stdout), batchx.WithChunkSize(2))
	if err != nil {
		log.Fatal(err)
	}
	if _, err := batchx.NewJob("quickstart", nil).Then(step).Run(context.Background(), nil); err != nil {
		log.Fatal(err)
	}
}
```

This is `examples/quickstart` (a test keeps them identical). If the process dies or `ctx` is cancelled, run the
same job with the same params again: completed steps are skipped and the failed step resumes from its last
committed checkpoint. A completed instance returns `ErrAlreadyCompleted`.

## Examples

Runnable programs, each with an `expected_output.txt` that CI compares against:

| Program | Shows |
|---|---|
| `go run ./examples/quickstart` | CSV to processor to JSON lines, chunked |
| `go run ./examples/restart` | crash mid-run, rerun, resume from the checkpoint |
| `go run ./examples/skipretry` | skipping bad rows, retry with backoff, listeners |
| `go run ./examples/filerepo` | the JSON job state file before and after a restart |
| `go run ./examples/demo` | multi-step job: parallel loads, join, retry, skip, report |

## Things to care about

Full list with wrong/right snippets, each backed by a test: [docs/PITFALLS.md](docs/PITFALLS.md). The important ones:

- **At-least-once.** A crash after `Write` but before the checkpoint re-writes that chunk. Make writers idempotent (upsert by key).
- **Readers must replay the same order** on restart, and non-seekable readers (lines, CSV, JSONL, channels) must be
  created fresh for every run, or the restart silently skips data.
- **Retry retries skippable errors by default.** Set `Retry.If` so bad rows are not retried.
- **Skip limit is per job instance across restarts;** default 0 means any error fails the step.
- **Chunk size** is memory plus one repository save per chunk; the file repository fsyncs on each save.
- **Parallel steps run concurrently;** share nothing without your own locking.
- **No cross-process locking** in `FileRepository`; step names are checkpoint keys (renaming restarts the step).

## Concepts

| Spring Batch | batchx |
|---|---|
| ItemReader / ItemProcessor / ItemWriter | `Reader[I]`, `Processor[I,O]`, `Writer[O]` |
| Step (chunk / tasklet) | `NewStep`, `NewCopyStep`, `NewTaskletStep` |
| Job, JobParameters | `Job` (`Then`, `ThenParallel`), `Params` |
| JobRepository | `Repository` interface (two methods), `MemoryRepository`, `FileRepository` |
| Skip / retry policy | `WithSkipLimit`, `WithSkipIf`, `Skippable(err)`, `Retry` |
| Listeners | `Listener` (struct of optional funcs) |

Guarantees and limits:
- Delivery is at-least-once per chunk. A crash after `Write` returns but before the checkpoint is saved
  re-writes that chunk on restart; make writers idempotent. A chunk `Write` should be all-or-nothing.
- When a skippable error fails a chunk write, batchx re-writes the chunk item by item to isolate the bad item.
- Restart checkpoint is a position count. Readers implementing `Seeker` restart in O(1); others are replayed
  (read and discarded), so the input must be deterministic between runs.
- `FileRepository` is atomic per write but does not lock across processes; run one instance per job key.
- Not in v0.1: partitioning of one step across workers, multi-process coordination, SQL repository
  (implement the two-method `Repository`).

## Comparison with gobatch

Checked 2026-09-21 from github.com/chararch/gobatch source and metadata (I did not run it):
gobatch (MIT, 56 stars, last push 2025-02-27) uses `interface{}` item types with `reflect`, `go 1.16`, and depends on
go-sql-driver/mysql, ants, pkg/errors and an FTP client. Its job repository is package-level functions over a SQL database and
the repository ships only a MySQL schema. batchx differs by being generic, dependency-free and storage-agnostic
(memory and JSON file built in). gobatch has features batchx lacks: SQL persistence, file/FTP components, TSV, task
pools. See STATUS.md for the evidence log.

## Development

    git config core.hooksPath .githooks   # pre-push runs fmt, tidy, vet, race tests, build, lint

## License

MIT
