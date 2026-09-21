# batchx status

| stage | attempts | updated | evidence |
|-------|----------|---------|----------|
| validated (PARTIAL gap, proceed) | 0 | 2026-09-21 | Step 0 below |

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

(updated below as units are pushed)
