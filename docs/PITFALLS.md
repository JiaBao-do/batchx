# Things to care about

Every claim below is backed by a test in `pitfalls_test.go` (names start with `TestPitfall`), by the
runnable programs in `examples/`, or by the doc comments. Run `go test -run Pitfall -v .` to see them.

## 1. Chunk size: memory, latency and commit cost

A chunk is held in memory (input items plus processed outputs) and every chunk ends with one
`Repository.Save`. Chunk size 1 means one save per item; chunk 50 over 100 items means 2 checkpoints
(`TestPitfallChunkSizeControlsSaveCount`). With `FileRepository` a save is a write + fsync + rename;
`BenchmarkFileRepositorySave` measured about 2.5 ms per save on the author's Windows laptop (your disk will
differ), so a tiny chunk size on a file repository is slow. Large chunks cost memory and mean more work to redo
after a failure. Default is 10; pick it from item size and how expensive a re-do is.

## 2. At-least-once: make writers idempotent

A crash after `Writer.Write` returns but before the checkpoint is saved re-writes that chunk on restart.

```go
// WRONG: an append-only sink duplicates the chunk that was written but not checkpointed.
w := batchx.WriterFunc[Row](func(ctx context.Context, rows []Row) error {
    return appendToFile(rows)
})

// RIGHT: keyed upsert (INSERT ... ON CONFLICT, PUT by id): repeating a chunk is harmless.
w := batchx.WriterFunc[Row](func(ctx context.Context, rows []Row) error {
    return upsertByID(ctx, rows)
})
```

`TestPitfallAtLeastOnceDuplicatesWithAppendWriter` shows the append writer receiving items `0 1 2 3 2 3 4 5`.
A chunk write should also be all-or-nothing (one transaction): batchx retries a failed write, and for skippable
errors re-writes the chunk item by item, so a half-applied failed write would be applied again.

## 3. What counts as a committed chunk

Progress moves only after the write succeeded and `Repository.Save` accepted the checkpoint. The record's
`Read`, `Written`, `Filtered`, `Skipped`, `Retries` and `Position` are updated at that moment and never before.
If chunk 2 fails, the record still says chunk 1 (`TestPitfallCountersOnlyReflectCommittedChunks`). If `Save`
itself fails, the step fails; the chunk's write has already happened, which is the at-least-once case of section 2.
On cancellation the checkpoint is saved with a context detached from the cancellation, so finished work is not lost.

## 4. Skip limits versus retry limits

- `WithSkipLimit(n)` counts skipped items for the whole job instance across restarts, only from committed chunks.
  Default 0: no skipping, a skippable error fails the step with `ErrSkipLimitExceeded` (`TestPitfallSkipLimitIsCumulativeAcrossRestarts`).
- `Retry.MaxAttempts` is the total number of attempts including the first.
- By default `Retry` retries every error except `ErrFilter` and context errors, **including skippable ones**, so a
  deterministic bad row is retried `MaxAttempts` times before being skipped. Set `If` to avoid it:

```go
// WRONG: bad rows are processed 3 times before being skipped.
batchx.WithRetry(batchx.Retry{MaxAttempts: 3})

// RIGHT: retry only transient errors.
batchx.WithRetry(batchx.Retry{MaxAttempts: 3, If: func(err error) bool { return !batchx.IsSkippable(err) }})
```
(`TestPitfallSkippableErrorsAreRetriedByDefault`: 3 calls versus 1.)
- Retry applies to processor calls (per item) and writer calls (per chunk). Reads are never retried.
- Only errors marked with `Skippable(err)` (or accepted by `WithSkipIf`) can be skipped. A `csv.ParseError` from
  `CSVReader` and a bad line from `JSONLReader` are already skippable.

## 5. Readers must replay the same sequence on restart

Restart resumes at `Position`. `SliceReader` seeks in O(1); other readers are replayed (read and discarded), so
the input must yield the same items in the same order every run. A reader over a map, an unordered query or a
live feed loses and duplicates items after a restart (`TestPitfallNonDeterministicReaderBreaksRestart`).

```go
// WRONG: SELECT * FROM t (no ORDER BY), or ranging over a map.
// RIGHT: SELECT * FROM t ORDER BY id, or sort the keys first.
```

Also: a skippable read error must leave the reader positioned at the next item, otherwise the step would loop.

## 6. Parallel steps and shared state

`ThenParallel` runs its steps on separate goroutines (`TestParallelStage` asserts they overlap). Give each step
its own reader and writer; anything shared (a map, a counter, a `bytes.Buffer`) needs your own locking, and
listeners registered on the job are called from several goroutines. `SliceWriter` is safe for concurrent use.
Data produced by a parallel stage can be read by the next stage, because stages run one after another
(`examples/demo` does this). If one parallel step fails, the others are cancelled and recorded as `stopped`.
Partitioning one step across workers is not in v0.1.

## 7. Build fresh readers for every run

A step holds its reader and writer. Readers that are not `Seeker` (lines, CSV, JSONL, channel, `iter.Seq`) are
consumed by the first run. Reusing the same instance for the restart replays from wherever the old run stopped:
the job then reports success but silently skipped data (`TestPitfallReusingConsumedStreamReader`: 4 of 10 lines
written). Construct the reader (and step) again for each `Run`, as `examples/restart` does.

## 8. Context cancellation and partial chunks

Cancelling the context ends the run with status `stopped` and `context.Canceled`; the job is restartable. The
chunk in flight is not committed unless its `Write` already returned successfully. Cancellation only interrupts
a blocked read if the reader honours the context: the built-in readers do (`ChanReader` selects on `ctx.Done()`),
a custom `ReaderFunc` that blocks on a socket without a deadline will not (`TestPitfallCancellationNeedsContextAwareReader`).
Pass the `ctx` you receive into your I/O.

## 9. Generics and types

- `NewStep` needs typed arguments; wrap plain functions as `batchx.ProcessorFunc[In, Out](fn)`, `ReaderFunc[T]`,
  `WriterFunc[T]`. `NewCopyStep[T]` needs reader and writer of the same `T`.
- `JSONLReader[map[string]any]` decodes numbers as `float64`, so integers above 2^53 lose precision. Decode into a
  struct (`TestPitfallJSONLIntoAnyGivesFloat64`).
- `Skippable(nil)` returns nil.

## 10. Identity: job name, params, step names

A job instance is `name` plus `Params`. Same name and params after completion returns `ErrAlreadyCompleted`; a
different param value is a new instance with fresh checkpoints. Step names are the checkpoint keys, unique within a
job: renaming a step in a new release makes it start again from position 0 (`TestPitfallRenamedStepForgetsProgress`).
Changing `ChunkSize` between runs is fine (the checkpoint is a position, not a chunk number).

## 11. Repository durability

- `MemoryRepository` loses everything at process exit: restart works only inside the process.
- `FileRepository` writes a temp file, fsyncs it, then renames over the record; a reader never sees a torn file.
  It does not fsync the directory, so after power loss the previous checkpoint may reappear; that only causes the
  at-least-once re-write of section 2.
- No cross-process locking. Two processes running the same job key against one directory will both process the
  data; `ErrAlreadyRunning` only guards against the same `Job` value being run twice in one process.
- A record left in `running` by a crashed process is treated as restartable.
- Records carry `"version": 1`; an unknown version is refused rather than misread.
- Implement `Repository` (two methods) for SQL or KV stores; copy records at the boundary.

## 12. Not supported in v0.1

Partitioning a step across worker goroutines, multi-process coordination, a SQL repository (roll your own), job
listing/querying API, metrics (use a `Listener`), and scheduling (use cron or a scheduler around `Job.Run`).

## 13. Listeners

Hooks run synchronously on the step's goroutine and must not panic; a slow hook slows the step. `OnSkip` receives
`nil` as the item for read errors.

## 14. Go version floor

Requires Go 1.24+. CI runs the whole test suite on Go 1.24 and on the latest stable release, and the code avoids
APIs newer than 1.24. Development uses the `go 1.24` directive, so `go vet` flags accidental use of newer stdlib APIs.
