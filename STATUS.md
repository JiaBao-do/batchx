# batchx status

| stage | attempts | updated | evidence |
|-------|----------|---------|----------|
| tagged v0.1.0 (on c61a71f, CI run 35558757621 green), proxy.golang.org indexed; pkg.go.dev page pending | 0 | 2026-09-21 | repo https://github.com/JiaBao-do/batchx ; see Ledger |

## Step 0 evidence (2026-09-21)

gobatch (github.com/chararch/gobatch), MIT, 56 stars, last push 2025-02-27, no releases, 4 issues total (2 open).
Verified from its source tree via `gh api`:
- go.mod: `go 1.16`; dependencies go-sql-driver/mysql, panjf2000/ants/v2, pkg/errors, jlaffaye/ftp, bmizerany/assert (not zero-dep).
- No generics: `ItemReader` is `ReadKeys() ([]interface{}, error)` / `ReadItem(key interface{}) (interface{}, error)`; implementation uses `reflect`.
- Repository is a set of package-level functions over a SQL database; only `sql/schema_mysql.sql` shipped (MySQL). Not a storage-agnostic interface.
- README: features are modular construction, serial/parallel, file components, listeners. Setup step 1 is "set up a database and create tables".
- Code search in repo: "skip" 10 hits, "retry" 2 hits (no retry policy/backoff file in the tree; no skip-policy file).
- Open issue #2: "like springbatch, suggest increasing the task partition processing capacity" (user asks for partitioning). Issue #3: docs/examples request. Closed #4: how to stop a running job (Chinese).
- README is 4 sentences; docs live on an external site.

Other candidates found (gh search, several queries): supreness/batch (fork of gobatch, 0 stars, 2023), vinchauhan/go-batch-pipe (tiny, 2019), fermat-tech/jobflow (0 stars, scheduler with restart-from-step, no chunking), thalesfsp/etler (ETL with generics, 3 stars, no restart/repository). pkg.go.dev/WebSearch surfaced nothing else.

Verdict: PARTIAL. Differentiators: generics, zero dependencies, storage-agnostic Repository interface with memory and JSON-file stores, restart from last committed chunk, skip/retry policies with backoff, context cancellation that persists a Stopped state, parallel steps.
Not verified: gobatch runtime behaviour (I did not run it); its retry/skip existence is inferred only from code-search hit counts.

## Board

agentboard RUNNING.md did not exist at check time; nothing synced. Sync later: story "batchx" and subtasks.

## Ledger

| unit | commit | evidence |
|------|--------|----------|
| core: chunk step, job, restart, skip/retry, repos, readers/writers, tests, scaffold | 824e77e | local: race tests pass, cov 92.2%, golangci-lint 0 issues, govulncheck clean, go1.24.0 vet+test OK |
| fix: tidy check tolerates missing go.sum | c040399 | hook failed first push (git diff go.sum); fixed |
| chore: hook prepends gcc dir to PATH | f66d529 | Windows/Git Bash: Git's /mingw64/bin DLLs shadow msys2 gcc DLLs -> cgo fails -> TSan error 87; fixed; first push accepted |
| docs: examples, PITFALLS, README | 41bfbda | CI run 35558545744 green: 8 test jobs (ubuntu/windows/macos x Go 1.24/stable), lint, vuln |

Phase 5 gate (2026-09-21, HEAD 41bfbda): gofmt clean, go mod tidy no diff, go vet clean (stdversion on go 1.24 directive), golangci-lint 0 issues,
`go test -race -shuffle=on` ok (core coverage 92.4%), benchmarks with allocs (StepChunk10 4031 allocs/op, StepChunk1000 71 allocs/op, FileRepositorySave ~3ms/op),
fuzz FuzzReaders 15s+20s no findings, govulncheck none, go build ok, no TODO/FIXME.
Property test: TestPropertyMatchesNaiveWithCrashes (300 random cases, 0-3 injected crashes, memory and file repos) equals naive reference.
Skipped: independent verifier agent not spawned. Dependabot opened 3 action-bump branches (checkout-7, setup-go-7, golangci-lint-action-9); not touched.
Board: agentboard RUNNING.md absent; nothing synced.

Tag: v0.1.0 -> c61a71f. proxy.golang.org info: Version v0.1.0, Hash c61a71f (2026-09-21T03:48:00Z). pkg.go.dev page not rendered yet at check time (queued).
pkg.go.dev: page https://pkg.go.dev/github.com/JiaBao-do/batchx@v0.1.0 renders (no 'not found'), README badge links to it. Head 3126124 CI green. Stray file c removed in fef2ea3 (still present in the immutable v0.1.0 archive).

## Post-release verifier findings (2026-09-22) - resolved

Independent verifier tested tag v0.1.0 (c61a71f) fresh-clone and found 2 medium defects. Both reproduced locally, then fixed:

1. **Missing `.gitattributes`**: repro'd with a fresh single-step `git clone --branch v0.1.0` under `core.autocrlf=true`:
   every tracked file checked out CRLF (confirmed via byte scan, 78 CRLF sequences in writers.go) and `gofmt -l .`
   flagged all 20 source files, which would fail the repo's own mandatory `.githooks/pre-push` gate for any normal
   Windows contributor. Root cause: no `.gitattributes` forcing `eol=lf` at that tag. Turns out this was already fixed
   pre-task by chore commit 3126124 (`* text=auto eol=lf`, matching oss/agentboard and oss/collectx's pattern), which
   predates this verifier pass but postdates the v0.1.0 tag. Re-verified now with a fresh single-step clone of current
   main (168797f) under `core.autocrlf=true`: `gofmt -l .` is empty and writers.go checks out LF-only (0 CRLF, 78 LF).
   No new commit needed for this one; the tag itself is intentionally left as-is per policy (v0.1.0 stays immutable).
2. **`CSVWriter` not chunk-atomic**: repro'd with a fault-injection sink (`faultAfterWriter`, refuses any Write once it
   has already accepted 5000 bytes) writing a fixed-width ~12KB chunk (500 rows x 24 bytes): the old implementation
   encoded rows directly through `encoding/csv.Writer`'s internal 4KB `bufio` buffer straight into the sink, so the
   buffer's auto-flush at 4096 bytes reached the sink before the chunk was reported failed, cutting the 171st row
   mid-record (`...00000000000170,xx\n` truncated to `0000000000000000`). Root cause: no chunk-level buffering, so a
   mid-chunk failure could already have leaked a torn row. Fix (writers.go): `CSVWriter.Write` now encodes the whole
   chunk into an in-memory `bytes.Buffer` first and only issues one real `Write` call to the underlying `io.Writer`
   once the entire chunk has encoded without error; on the same fault-injection repro this now leaves 0 bytes on the
   sink (the one real Write call is rejected outright before any bytes land). Regression test:
   `TestCSVWriterChunkAtomicOnFailure` in io_test.go (fails on pre-fix code with "partial row leaked", passes on the
   fix). Documented limitation in the CSVWriter doc comment: a sink that itself performs a genuine short write
   (returns n < len(p) with an error mid-slice) can still see a torn tail; that is outside any single
   `io.Writer.Write` call's control.

Phase 5 gate re-run (2026-09-22, after the writers.go fix): gofmt clean, `go mod tidy` no diff, go vet clean, golangci-lint
0 issues, `go test -race -shuffle=on -count=5 ./...` ok, benchmarks unchanged (StepChunk10 4031 allocs/op, StepChunk1000
71 allocs/op), govulncheck none, go build ok.
