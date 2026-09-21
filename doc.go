// Package batchx is a chunk-oriented batch processing framework in the spirit
// of Spring Batch, written with generics and only the standard library.
//
// A Job is a list of Steps. A chunk step reads items from a Reader, transforms
// them with a Processor and hands them to a Writer in chunks, then commits a
// checkpoint to a Repository. If the process stops, running the same Job with
// the same Params resumes from the last committed chunk.
//
// Skip limits with typed skippable errors, retry with backoff, listeners,
// context cancellation and parallel steps are supported. Repositories are
// pluggable: MemoryRepository and a JSON FileRepository ship in the box.
//
// Delivery guarantee: at-least-once per chunk. A crash between a successful
// Writer.Write and the checkpoint save re-writes that chunk on restart, so
// writers should be idempotent.
package batchx
