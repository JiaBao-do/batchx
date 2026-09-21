package batchx

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

func FuzzReaders(f *testing.F) {
	for _, s := range []string{"", "a,b\n1,2\n", "{\"id\":1}\n{bad\n", "x\r\ny", "\"unterminated", "\n\n\n"} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, in string) {
		ctx := context.Background()
		// Every reader must terminate, never panic, and only return
		// io.EOF, a skippable error, or a plain read error.
		lr := NewLinesReader(strings.NewReader(in))
		lines := 0
		for {
			_, err := lr.Read(ctx)
			if err != nil {
				if !errors.Is(err, io.EOF) {
					t.Fatal(err)
				}
				break
			}
			lines++
		}
		if want := strings.Count(in, "\n") + btoi(len(in) > 0 && !strings.HasSuffix(in, "\n")); lines != want {
			t.Fatalf("lines %d want %d for %q", lines, want, in)
		}
		cr := NewCSVReader(strings.NewReader(in), false)
		for i := 0; i < len(in)+2; i++ {
			if _, err := cr.Read(ctx); err != nil {
				if !errors.Is(err, io.EOF) && !IsSkippable(err) {
					t.Fatal(err)
				}
				if errors.Is(err, io.EOF) {
					break
				}
			}
		}
		jr := NewJSONLReader[map[string]any](strings.NewReader(in))
		for i := 0; i < len(in)+2; i++ {
			if _, err := jr.Read(ctx); err != nil {
				if !errors.Is(err, io.EOF) && !IsSkippable(err) {
					t.Fatal(err)
				}
				if errors.Is(err, io.EOF) {
					break
				}
			}
		}
	})
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}
