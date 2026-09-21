package batchx

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"iter"
	"strings"
)

// SliceReader reads from a slice. It implements Seeker (O(1) restart). It is
// not safe for concurrent use.
type SliceReader[T any] struct {
	items []T
	pos   int
}

// NewSliceReader returns a reader over items (not copied).
func NewSliceReader[T any](items []T) *SliceReader[T] { return &SliceReader[T]{items: items} }

// Read implements Reader.
func (s *SliceReader[T]) Read(context.Context) (T, error) {
	if s.pos >= len(s.items) {
		var z T
		return z, io.EOF
	}
	v := s.items[s.pos]
	s.pos++
	return v, nil
}

// Seek implements Seeker.
func (s *SliceReader[T]) Seek(_ context.Context, n int64) error {
	if n < 0 || n > int64(len(s.items)) {
		return fmt.Errorf("%w: %d of %d", ErrCheckpointBeyondEnd, n, len(s.items))
	}
	s.pos = int(n)
	return nil
}

// ChanReader reads from a channel until it is closed. Restart replays by
// discarding, so the channel producer must be deterministic to restart.
type ChanReader[T any] struct{ ch <-chan T }

// NewChanReader returns a reader over ch.
func NewChanReader[T any](ch <-chan T) *ChanReader[T] { return &ChanReader[T]{ch: ch} }

// Read implements Reader; it returns ctx.Err() if ctx ends first.
func (c *ChanReader[T]) Read(ctx context.Context) (T, error) {
	select {
	case v, ok := <-c.ch:
		if !ok {
			var z T
			return z, io.EOF
		}
		return v, nil
	case <-ctx.Done():
		var z T
		return z, ctx.Err()
	}
}

// SeqReader reads from an iter.Seq. Close stops the underlying iterator; the
// step closes it automatically.
type SeqReader[T any] struct {
	next func() (T, bool)
	stop func()
}

// NewSeqReader returns a reader over seq.
func NewSeqReader[T any](seq iter.Seq[T]) *SeqReader[T] {
	next, stop := iter.Pull(seq)
	return &SeqReader[T]{next: next, stop: stop}
}

// Read implements Reader.
func (s *SeqReader[T]) Read(context.Context) (T, error) {
	v, ok := s.next()
	if !ok {
		return v, io.EOF
	}
	return v, nil
}

// Close implements io.Closer.
func (s *SeqReader[T]) Close() error { s.stop(); return nil }

// LinesReader reads lines from an io.Reader, without a line length limit.
// Line terminators ("\n" or "\r\n") are removed. It is not safe for
// concurrent use.
type LinesReader struct{ br *bufio.Reader }

// NewLinesReader returns a reader over the lines of r.
func NewLinesReader(r io.Reader) *LinesReader { return &LinesReader{br: bufio.NewReader(r)} }

// Read implements Reader. An empty final segment after the last newline is
// not a line.
func (l *LinesReader) Read(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	s, err := l.br.ReadString('\n')
	if err != nil && (err != io.EOF || s == "") {
		return "", err
	}
	s = strings.TrimSuffix(s, "\n")
	return strings.TrimSuffix(s, "\r"), nil
}

// CSVReader reads records with encoding/csv. Malformed records (csv.ParseError,
// including wrong field counts) are returned as Skippable errors and the reader
// continues with the next record. It is not safe for concurrent use.
type CSVReader struct {
	cr        *csv.Reader
	hasHeader bool
	headerRd  bool
	header    []string
}

// NewCSVReader returns a reader over r. If hasHeader is true the first record
// is consumed on the first Read and available from Header. configure may adjust
// the underlying *csv.Reader (Comma, Comment, LazyQuotes, ...) and is called
// once.
func NewCSVReader(r io.Reader, hasHeader bool, configure ...func(*csv.Reader)) *CSVReader {
	cr := csv.NewReader(r)
	for _, f := range configure {
		f(cr)
	}
	return &CSVReader{cr: cr, hasHeader: hasHeader}
}

// Header returns the header record, valid after the first Read.
func (c *CSVReader) Header() []string { return c.header }

// Read implements Reader.
func (c *CSVReader) Read(ctx context.Context) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if c.hasHeader && !c.headerRd {
		c.headerRd = true
		h, err := c.cr.Read()
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, Skippable(err)
		}
		c.header = h
	}
	rec, err := c.cr.Read()
	if err != nil {
		var pe *csv.ParseError
		if errors.As(err, &pe) {
			return nil, Skippable(err)
		}
		return nil, err
	}
	return rec, nil
}

// JSONLReader reads one JSON value per line into T. Blank lines are ignored;
// invalid lines are Skippable errors. Lines have no length limit. It is not
// safe for concurrent use.
type JSONLReader[T any] struct {
	lines *LinesReader
	n     int
}

// NewJSONLReader returns a reader over the JSON lines of r.
func NewJSONLReader[T any](r io.Reader) *JSONLReader[T] {
	return &JSONLReader[T]{lines: NewLinesReader(r)}
}

// Read implements Reader.
func (j *JSONLReader[T]) Read(ctx context.Context) (T, error) {
	var v T
	for {
		line, err := j.lines.Read(ctx)
		if err != nil {
			return v, err
		}
		j.n++
		if strings.TrimSpace(line) == "" {
			continue
		}
		if err := json.Unmarshal([]byte(line), &v); err != nil {
			var z T
			return z, Skippable(fmt.Errorf("jsonl line %d: %w", j.n, err))
		}
		return v, nil
	}
}
