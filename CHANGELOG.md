# Changelog

All notable changes are documented here. Format: Keep a Changelog; versioning: SemVer.

## [Unreleased]
### Fixed
- `CSVWriter` now buffers a whole chunk's CSV output in memory and issues a single `Write` to the
  underlying `io.Writer` only once the chunk has fully and successfully encoded. Previously, `encoding/csv`'s
  internal 4KB `bufio` buffer could auto-flush mid-chunk into the caller's writer, so a chunk that failed
  partway through could leave a row torn mid-record on the sink even though the chunk was reported failed
  and retried, corrupting the stream instead of producing a clean duplicate on retry. Regression test:
  `TestCSVWriterChunkAtomicOnFailure` (fault-injection sink that refuses writes past a byte limit).
- (No code change) Verified `.gitattributes` (`* text=auto eol=lf`, added post-v0.1.0) actually normalizes
  line endings on a fresh Windows clone with `core.autocrlf=true`: `gofmt -l` is now clean and files check
  out as LF-only, where a fresh clone of the v0.1.0 tag (which predates `.gitattributes`) reproducibly
  checks out as CRLF and fails `gofmt -l` on every file.

## [0.1.0]
### Added
- Chunk-oriented steps (generic Reader, Processor, Writer), jobs with sequential and parallel stages.
- Restart from the last committed chunk; MemoryRepository and JSON FileRepository.
- Skip limits with typed skippable errors, retry with backoff, listeners, context cancellation.
- Readers: slice, channel, iter.Seq, lines, CSV, JSONL. Writers: slice, CSV, JSONL.
- examples/ (quickstart, restart, skipretry, filerepo, demo) and docs/PITFALLS.md backed by tests.
- Requires Go 1.24+.
