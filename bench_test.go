package batchx

import (
	"context"
	"testing"
)

func benchRun(b *testing.B, n, chunk int) {
	items := ints(n)
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		st, _ := NewCopyStep[int]("s", NewSliceReader(items), WriterFunc[int](func(context.Context, []int) error { return nil }), WithChunkSize(chunk))
		if _, err := NewJob("j", nil).Then(st).Run(ctx, nil); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStepChunk10(b *testing.B)   { benchRun(b, 10000, 10) }
func BenchmarkStepChunk1000(b *testing.B) { benchRun(b, 10000, 1000) }

func BenchmarkFileRepositorySave(b *testing.B) {
	f, err := NewFileRepository(b.TempDir())
	if err != nil {
		b.Fatal(err)
	}
	rec := JobRecord{Version: recordVersion, Key: "k", Steps: []StepRecord{{Name: "s"}}}
	b.ReportAllocs()
	for b.Loop() {
		if err := f.Save(context.Background(), rec); err != nil {
			b.Fatal(err)
		}
	}
}
