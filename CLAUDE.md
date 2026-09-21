# batchx

Chunk-oriented batch processing (jobs, steps, restart, skip/retry). Go library, module `github.com/JiaBao-do/batchx`.
Fills the gap left by Spring Batch in the Go ecosystem.

## Commands
- Test: `go test -race -shuffle=on ./...` (on Windows run from PowerShell; Git Bash triggers a TSan error 87)
- Lint: `golangci-lint run`
- Bench: `go test -run=^$ -bench=. -benchmem ./...`
- Fuzz: `go test -run=^$ -fuzz=FuzzReaders -fuzztime=30s`
- Vuln: `govulncheck ./...`
- Hook: `git config core.hooksPath .githooks` (pre-push; never bypass)

## Architecture
- `step.go`: Reader/Processor/Writer/Seeker interfaces, `StepContext`, chunk step (read, process w/ retry+skip, write w/ retry+scan-skip, commit).
- `job.go`: `Job` stages (sequential, parallel), restart logic, persistence of job/step records.
- `record.go`: `JobRecord`/`StepRecord` (JSON, versioned), `Params` key hashing.
- `repository.go`: `Repository` interface, `MemoryRepository`, `FileRepository` (atomic rename).
- `readers.go`/`writers.go`: slice, chan, iter.Seq, lines, CSV, JSONL.
- `retry.go`, `listener.go`, `errors.go`.
- `examples/`: runnable programs with `expected_output.txt` (checked by `examples/examples_test.go`; README quick start must equal `examples/quickstart/main.go`).
- `docs/PITFALLS.md`: every claim is backed by a `TestPitfall*` in `pitfalls_test.go`; change behavior => update both.

## Conventions
- Requires Go 1.24+ (go.mod 1.24, CI 1.24 + stable). Do not use APIs newer than 1.24 (no WaitGroup.Go, no errors.AsType), stdlib only, no CGO.
- Exported API has doc comments and Example tests. Errors wrapped with %w; sentinels in errors.go.
- Conventional Commits; update CHANGELOG.md under Unreleased.

## Invariants (do not break)
- Portability: no third-party deps, no CGO, no telemetry, no network; state is plain versioned JSON.
- Checkpoint (`Position`) advances only after a successful chunk write and a successful `Repository.Save`; counters mirror committed chunks only.
- Commits use `context.WithoutCancel` so cancellation never loses a checkpoint for work already written.
- Restart output must equal the naive single-pass reference (see TestPropertyMatchesNaiveWithCrashes).
- Record `Version` bump needs a migration; Repository implementations copy at the boundary.

## Roadmap
- Partitioned/parallel chunk processing inside one step; SQL repository example in docs; job listing/query API; cross-process lock hook; metrics adapter via Listener.
