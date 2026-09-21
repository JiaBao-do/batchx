# Changelog

All notable changes are documented here. Format: Keep a Changelog; versioning: SemVer.

## [Unreleased]

## [0.1.0]
### Added
- Chunk-oriented steps (generic Reader, Processor, Writer), jobs with sequential and parallel stages.
- Restart from the last committed chunk; MemoryRepository and JSON FileRepository.
- Skip limits with typed skippable errors, retry with backoff, listeners, context cancellation.
- Readers: slice, channel, iter.Seq, lines, CSV, JSONL. Writers: slice, CSV, JSONL.
