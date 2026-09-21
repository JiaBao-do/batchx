package batchx

import (
	"bytes"
	"context"
	"encoding/csv"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"testing"
)

func drain[T any](t testing.TB, r Reader[T], skipOK bool) ([]T, int) {
	t.Helper()
	var out []T
	skips := 0
	for {
		v, err := r.Read(context.Background())
		if errors.Is(err, io.EOF) {
			return out, skips
		}
		if err != nil {
			if skipOK && IsSkippable(err) {
				skips++
				continue
			}
			t.Fatalf("read: %v", err)
		}
		out = append(out, v)
	}
}

func TestSliceReaderSeek(t *testing.T) {
	r := NewSliceReader([]string{"a", "b", "c"})
	if err := r.Seek(context.Background(), 2); err != nil {
		t.Fatal(err)
	}
	if got, _ := drain[string](t, r, false); !reflect.DeepEqual(got, []string{"c"}) {
		t.Fatal(got)
	}
	if err := r.Seek(context.Background(), 9); !errors.Is(err, ErrCheckpointBeyondEnd) {
		t.Fatal(err)
	}
	if err := r.Seek(context.Background(), -1); err == nil {
		t.Fatal("negative seek must fail")
	}
}

func TestChanReader(t *testing.T) {
	ch := make(chan int, 3)
	ch <- 1
	ch <- 2
	close(ch)
	if got, _ := drain[int](t, NewChanReader(ch), false); !reflect.DeepEqual(got, []int{1, 2}) {
		t.Fatal(got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewChanReader(make(chan int)).Read(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestSeqReader(t *testing.T) {
	r := NewSeqReader(slices.Values([]int{4, 5, 6}))
	defer func() { _ = r.Close() }()
	if got, _ := drain[int](t, r, false); !reflect.DeepEqual(got, []int{4, 5, 6}) {
		t.Fatal(got)
	}
}

func TestLinesReader(t *testing.T) {
	for in, want := range map[string][]string{
		"":                                 nil,
		"a":                                {"a"},
		"a\n":                              {"a"},
		"a\r\nb\n\nc":                      {"a", "b", "", "c"},
		"\n":                               {""},
		strings.Repeat("x", 200000) + "\n": {strings.Repeat("x", 200000)},
	} {
		got, _ := drain[string](t, NewLinesReader(strings.NewReader(in)), false)
		if !reflect.DeepEqual(got, want) {
			t.Errorf("%.20q: got %d lines want %d", in, len(got), len(want))
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewLinesReader(strings.NewReader("a")).Read(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestCSVReaderHeaderAndBadRows(t *testing.T) {
	in := "id,name\n1,ann\n2,bob,extra\n3,cy\n4,\"un\"closed\n5,eve\n"
	r := NewCSVReader(strings.NewReader(in), true)
	got, skips := drain[[]string](t, r, true)
	if !reflect.DeepEqual(r.Header(), []string{"id", "name"}) {
		t.Fatal(r.Header())
	}
	want := [][]string{{"1", "ann"}, {"3", "cy"}, {"5", "eve"}}
	if !reflect.DeepEqual(got, want) || skips != 2 {
		t.Fatalf("got %v skips %d", got, skips)
	}
	r = NewCSVReader(strings.NewReader("a;b\n"), false, func(c *csv.Reader) { c.Comma = ';' })
	if got, _ := drain[[]string](t, r, false); !reflect.DeepEqual(got, [][]string{{"a", "b"}}) {
		t.Fatal(got)
	}
	if got, _ := drain[[]string](t, NewCSVReader(strings.NewReader(""), true), false); len(got) != 0 {
		t.Fatal(got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := NewCSVReader(strings.NewReader("a"), false).Read(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

type rec struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

func TestJSONLReaderWriterRoundTrip(t *testing.T) {
	in := "{\"id\":1,\"name\":\"a\"}\n\n{bad\n{\"id\":2,\"name\":\"b\"}\n"
	got, skips := drain[rec](t, NewJSONLReader[rec](strings.NewReader(in)), true)
	if !reflect.DeepEqual(got, []rec{{1, "a"}, {2, "b"}}) || skips != 1 {
		t.Fatalf("%v %d", got, skips)
	}
	var buf bytes.Buffer
	if err := NewJSONLWriter[rec](&buf).Write(context.Background(), got); err != nil {
		t.Fatal(err)
	}
	if buf.String() != "{\"id\":1,\"name\":\"a\"}\n{\"id\":2,\"name\":\"b\"}\n" {
		t.Fatalf("%q", buf.String())
	}
	// unmarshalable value fails the chunk with no partial write
	var buf2 bytes.Buffer
	err := NewJSONLWriter[any](&buf2).Write(context.Background(), []any{1, make(chan int)})
	if err == nil || buf2.Len() != 0 {
		t.Fatalf("err %v len %d", err, buf2.Len())
	}
}

func TestCSVWriter(t *testing.T) {
	var buf bytes.Buffer
	w := NewCSVWriter(&buf, func(c *csv.Writer) { c.Comma = '|' })
	if err := w.Write(context.Background(), [][]string{{"a", "b c"}, {"d", "e|f"}}); err != nil {
		t.Fatal(err)
	}
	if buf.String() != "a|b c\nd|\"e|f\"\n" {
		t.Fatalf("%q", buf.String())
	}
}

type errWriter struct{}

func (errWriter) Write([]byte) (int, error) { return 0, errors.New("disk full") }

func TestWritersPropagateErrors(t *testing.T) {
	if err := NewCSVWriter(errWriter{}).Write(context.Background(), [][]string{{"a"}}); err == nil {
		t.Fatal("csv")
	}
	if err := NewJSONLWriter[int](errWriter{}).Write(context.Background(), []int{1}); err == nil {
		t.Fatal("jsonl")
	}
}

func TestCSVToJSONLPipelineRestart(t *testing.T) {
	var csvIn strings.Builder
	csvIn.WriteString("id,name\n")
	for i := range 25 {
		csvIn.WriteString(strings.Join([]string{itoa(i), "n" + itoa(i)}, ",") + "\n")
	}
	repo, err := NewFileRepository(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	crash := true
	calls := 0
	jw := NewJSONLWriter[rec](&out)
	ww := WriterFunc[rec](func(ctx context.Context, items []rec) error {
		calls++
		if calls == 3 && crash {
			return errors.New("crash")
		}
		return jw.Write(ctx, items)
	})
	proc := ProcessorFunc[[]string, rec](func(_ context.Context, r []string) (rec, error) {
		return rec{ID: len(r[0]), Name: r[1]}, nil
	})
	run := func() error {
		st, _ := NewStep[[]string, rec]("convert", NewCSVReader(strings.NewReader(csvIn.String()), true), proc, ww, WithChunkSize(4))
		_, err := NewJob("csv2jsonl", repo).Then(st).Run(context.Background(), Params{"file": "in.csv"})
		return err
	}
	if err := run(); err == nil {
		t.Fatal("expected crash")
	}
	crash = false
	if err := run(); err != nil {
		t.Fatal(err)
	}
	if n := strings.Count(out.String(), "\n"); n != 25 {
		t.Fatalf("lines %d\n%s", n, out.String())
	}
}

func itoa(i int) string { return strconv.Itoa(i) }

func TestMemoryRepositoryCopies(t *testing.T) {
	m := &MemoryRepository{}
	ctx := context.Background()
	if _, err := m.Load(ctx, "k"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	r := JobRecord{Key: "k", Params: Params{"a": "1"}, Steps: []StepRecord{{Name: "s"}}}
	_ = m.Save(ctx, r)
	r.Params["a"] = "mutated"
	r.Steps[0].Name = "mutated"
	got, _ := m.Load(ctx, "k")
	if got.Params["a"] != "1" || got.Steps[0].Name != "s" {
		t.Fatalf("%+v", got)
	}
	got.Steps[0].Name = "x"
	got2, _ := m.Load(ctx, "k")
	if got2.Steps[0].Name != "s" {
		t.Fatal("load must copy")
	}
}

func TestFileRepository(t *testing.T) {
	dir := t.TempDir()
	f, err := NewFileRepository(filepath.Join(dir, "nested", "repo"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := f.Load(ctx, "abc"); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	rec := JobRecord{Version: recordVersion, Name: "j", Key: "abc", Status: StatusFailed, Params: Params{"a": "b"},
		Steps: []StepRecord{{Name: "s", Position: 7, Status: StatusFailed}}}
	if err := f.Save(ctx, rec); err != nil {
		t.Fatal(err)
	}
	got, err := f.Load(ctx, "abc")
	if err != nil || !reflect.DeepEqual(got, rec) {
		t.Fatalf("%v %+v", err, got)
	}
	entries, _ := os.ReadDir(filepath.Join(dir, "nested", "repo"))
	if len(entries) != 1 || entries[0].Name() != "abc.json" {
		t.Fatalf("leftover files %v", entries)
	}
	for _, bad := range []string{"", "../x", "a/b", "a.b", strings.Repeat("a", 200)} {
		if _, err := f.Load(ctx, bad); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("Load(%q): %v", bad, err)
		}
		if err := f.Save(ctx, JobRecord{Key: bad}); !errors.Is(err, ErrInvalidConfig) {
			t.Errorf("Save(%q): %v", bad, err)
		}
	}
	_ = os.WriteFile(filepath.Join(dir, "nested", "repo", "corrupt.json"), []byte("{"), 0o644)
	if _, err := f.Load(ctx, "corrupt"); err == nil || errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
	_ = os.WriteFile(filepath.Join(dir, "nested", "repo", "old.json"), []byte(`{"version":99}`), 0o644)
	if _, err := f.Load(ctx, "old"); err == nil {
		t.Fatal("unsupported version must fail")
	}
	if _, err := NewFileRepository(filepath.Join(dir, "nested", "repo", "abc.json")); err == nil {
		t.Fatal("dir under a file must fail")
	}
}

func TestParamsKey(t *testing.T) {
	a := Params{"x": "1", "y": "2"}.key("j")
	if a != (Params{"y": "2", "x": "1"}).key("j") {
		t.Fatal("key must be order independent")
	}
	seen := map[string]bool{a: true}
	for _, k := range []string{Params{"x": "1"}.key("j"), Params{"x": "1", "y": "2"}.key("k"), Params{"x1": "", "y": "2"}.key("j"),
		Params{"x": "1y", "": "2"}.key("j"), Params(nil).key("j")} {
		if seen[k] {
			t.Fatal("collision")
		}
		seen[k] = true
	}
	if !keyPattern.MatchString(a) {
		t.Fatal(a)
	}
}

func TestErrorsHelpers(t *testing.T) {
	if Skippable(nil) != nil {
		t.Fatal()
	}
	base := errors.New("x")
	e := Skippable(base)
	if !IsSkippable(e) || !errors.Is(e, base) || e.Error() != "x" || IsSkippable(base) {
		t.Fatal()
	}
}
