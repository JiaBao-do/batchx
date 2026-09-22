package batchx

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"sync"
)

// SliceWriter collects written items in memory. It is safe for concurrent use.
type SliceWriter[T any] struct {
	mu    sync.Mutex
	items []T
}

// Write implements Writer.
func (s *SliceWriter[T]) Write(_ context.Context, items []T) error {
	s.mu.Lock()
	s.items = append(s.items, items...)
	s.mu.Unlock()
	return nil
}

// Items returns a copy of everything written so far.
func (s *SliceWriter[T]) Items() []T {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]T(nil), s.items...)
}

// CSVWriter writes records with encoding/csv. Each chunk is encoded into an
// in-memory buffer first; only once the whole chunk has encoded
// successfully does CSVWriter issue a single Write call to the underlying
// io.Writer with the complete, already-formed chunk bytes.
//
// This matters because encoding/csv.Writer wraps a bufio.Writer (4KB by
// default) that auto-flushes to its destination as soon as the buffer
// fills, independent of record boundaries. If CSVWriter encoded directly
// into the caller's io.Writer, a chunk that failed partway through (an
// unencodable value, or the underlying writer rejecting a later write)
// could already have leaked a row split mid-record into the sink, even
// though the chunk as a whole is reported as failed and gets retried -- so
// a retry would append the resumed data after a truncated, malformed row
// instead of the clean "the chunk's rows appear a second time" duplicate
// that at-least-once retry semantics promise. Buffering the whole chunk
// first means the real sink sees exactly one Write call per chunk, carrying
// either all of the chunk's rows or (if the encode itself fails) none of
// them.
//
// This guarantee assumes the underlying io.Writer's Write call is itself
// atomic (it either accepts the full slice or accepts none of it before
// returning an error), which holds for the common cases (files, in-memory
// buffers, most network writers for chunk-sized payloads). A sink that
// performs a genuine short write -- returning n < len(p) together with an
// error partway through consuming that one slice -- can still end up with a
// truncated tail; that is a property of the sink itself and is outside what
// any single io.Writer.Write call can guarantee.
//
// It is not safe for concurrent use.
type CSVWriter struct {
	w         io.Writer
	configure []func(*csv.Writer)
}

// NewCSVWriter returns a writer to w. configure may adjust the *csv.Writer
// (Comma, UseCRLF) used to encode each chunk.
func NewCSVWriter(w io.Writer, configure ...func(*csv.Writer)) *CSVWriter {
	return &CSVWriter{w: w, configure: configure}
}

// Write implements Writer. It encodes the whole chunk into an in-memory
// buffer and, only on success, writes that buffer to the underlying
// io.Writer in a single call. See the CSVWriter doc comment for why.
func (c *CSVWriter) Write(_ context.Context, recs [][]string) error {
	var buf bytes.Buffer
	cw := csv.NewWriter(&buf)
	for _, f := range c.configure {
		f(cw)
	}
	for _, r := range recs {
		if err := cw.Write(r); err != nil {
			return err
		}
	}
	cw.Flush()
	if err := cw.Error(); err != nil {
		return err
	}
	_, err := c.w.Write(buf.Bytes())
	return err
}

// JSONLWriter writes each item as one JSON line. It is not safe for
// concurrent use.
type JSONLWriter[T any] struct{ w io.Writer }

// NewJSONLWriter returns a writer to w.
func NewJSONLWriter[T any](w io.Writer) *JSONLWriter[T] { return &JSONLWriter[T]{w: w} }

// Write implements Writer. Items are encoded before any byte is written, so a
// value that cannot be marshalled fails the chunk without a partial write.
func (j *JSONLWriter[T]) Write(_ context.Context, items []T) error {
	var buf []byte
	for _, it := range items {
		b, err := json.Marshal(it)
		if err != nil {
			return fmt.Errorf("jsonl encode: %w", err)
		}
		buf = append(append(buf, b...), '\n')
	}
	_, err := j.w.Write(buf)
	return err
}
