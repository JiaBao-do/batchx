# batchx

[![ci](https://github.com/JiaBao-do/batchx/actions/workflows/ci.yml/badge.svg)](https://github.com/JiaBao-do/batchx/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/JiaBao-do/batchx.svg)](https://pkg.go.dev/github.com/JiaBao-do/batchx)

Chunk-oriented batch processing for Go, in the spirit of Spring Batch. Generic reader, processor and writer,
jobs made of steps, restart from the last committed chunk, skip and retry policies, listeners, and a pluggable
job repository. Standard library only, no CGO. Requires Go 1.24+.

    go get github.com/JiaBao-do/batchx

## Quick start

```go
repo, _ := batchx.NewFileRepository("./batch-state")          // JSON files; or &batchx.MemoryRepository{}
step, _ := batchx.NewStep("import",
    batchx.NewCSVReader(f, true),                              // Reader[[]string]
    batchx.ProcessorFunc[[]string, User](parse),               // Processor[[]string, User]
    batchx.NewJSONLWriter[User](out),                          // Writer[User]
    batchx.WithChunkSize(500),
    batchx.WithSkipLimit(10),                                  // tolerate 10 bad rows (csv.ParseError is skippable)
    batchx.WithRetry(batchx.Retry{MaxAttempts: 3, Backoff: batchx.ExponentialBackoff(100*time.Millisecond, 2*time.Second)}),
)
rec, err := batchx.NewJob("nightly-import", repo).Then(step).Run(ctx, batchx.Params{"day": "2026-09-21"})
```

If the process dies or `ctx` is cancelled, run the same job with the same params again: completed steps are
skipped and the failed step resumes from its last committed checkpoint. A completed instance returns
`ErrAlreadyCompleted`. Runnable versions are in `example_test.go`.

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
