# batchx status

| stage | attempts | updated | evidence |
|-------|----------|---------|----------|
| verified, CI green, ready to tag v0.1.0 | 0 | 2026-09-21 | repo https://github.com/JiaBao-do/batchx ; see Ledger |

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
