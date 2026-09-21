package batchx

import (
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

// CSVWriter writes records with encoding/csv and flushes after every chunk.
// It is not safe for concurrent use.
type CSVWriter struct{ cw *csv.Writer }

// NewCSVWriter returns a writer to w. configure may adjust the *csv.Writer
// (Comma, UseCRLF).
func NewCSVWriter(w io.Writer, configure ...func(*csv.Writer)) *CSVWriter {
	cw := csv.NewWriter(w)
	for _, f := range configure {
		f(cw)
	}
	return &CSVWriter{cw: cw}
}

// Write implements Writer.
func (c *CSVWriter) Write(_ context.Context, recs [][]string) error {
	for _, r := range recs {
		if err := c.cw.Write(r); err != nil {
			return err
		}
	}
	c.cw.Flush()
	return c.cw.Error()
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
