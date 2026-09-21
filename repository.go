package batchx

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sync"
)

// Repository persists job records. Implementations must copy records at the
// boundary (callers may mutate what they pass or receive) and be safe for
// concurrent use. SQL or KV implementations only need these two methods.
type Repository interface {
	// Load returns the record for key, or ErrNotFound.
	Load(ctx context.Context, key string) (JobRecord, error)
	// Save stores rec under rec.Key, replacing any previous record.
	Save(ctx context.Context, rec JobRecord) error
}

// MemoryRepository is a Repository held in memory. The zero value is ready to
// use and safe for concurrent use.
type MemoryRepository struct {
	mu   sync.Mutex
	recs map[string]JobRecord
}

// Load implements Repository.
func (m *MemoryRepository) Load(_ context.Context, key string) (JobRecord, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.recs[key]
	if !ok {
		return JobRecord{}, ErrNotFound
	}
	return r.Clone(), nil
}

// Save implements Repository.
func (m *MemoryRepository) Save(_ context.Context, rec JobRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.recs == nil {
		m.recs = make(map[string]JobRecord)
	}
	m.recs[rec.Key] = rec.Clone()
	return nil
}

var keyPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

// FileRepository stores one JSON file per job instance in a directory. Writes
// are atomic (temp file, fsync, rename). It is safe for concurrent use within
// one process; it does not lock across processes.
type FileRepository struct {
	dir string
	mu  sync.Mutex
}

// NewFileRepository returns a FileRepository rooted at dir, creating it if needed.
func NewFileRepository(dir string) (*FileRepository, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("batchx: create repository dir: %w", err)
	}
	return &FileRepository{dir: dir}, nil
}

func (f *FileRepository) path(key string) (string, error) {
	if !keyPattern.MatchString(key) {
		return "", fmt.Errorf("%w: bad record key %q", ErrInvalidConfig, key)
	}
	return filepath.Join(f.dir, key+".json"), nil
}

// Load implements Repository.
func (f *FileRepository) Load(_ context.Context, key string) (JobRecord, error) {
	p, err := f.path(key)
	if err != nil {
		return JobRecord{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	b, err := os.ReadFile(p)
	if errors.Is(err, os.ErrNotExist) {
		return JobRecord{}, ErrNotFound
	}
	if err != nil {
		return JobRecord{}, fmt.Errorf("batchx: read record: %w", err)
	}
	var rec JobRecord
	if err := json.Unmarshal(b, &rec); err != nil {
		return JobRecord{}, fmt.Errorf("batchx: decode record %s: %w", key, err)
	}
	if rec.Version != recordVersion {
		return JobRecord{}, fmt.Errorf("batchx: record %s has unsupported version %d", key, rec.Version)
	}
	return rec, nil
}

// Save implements Repository.
func (f *FileRepository) Save(_ context.Context, rec JobRecord) error {
	p, err := f.path(rec.Key)
	if err != nil {
		return err
	}
	b, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return fmt.Errorf("batchx: encode record: %w", err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	tmp, err := os.CreateTemp(f.dir, ".tmp-*")
	if err != nil {
		return fmt.Errorf("batchx: create temp record: %w", err)
	}
	name := tmp.Name()
	cleanup := func(e error) error { _ = tmp.Close(); _ = os.Remove(name); return e }
	if _, err := tmp.Write(b); err != nil {
		return cleanup(fmt.Errorf("batchx: write record: %w", err))
	}
	if err := tmp.Sync(); err != nil {
		return cleanup(fmt.Errorf("batchx: sync record: %w", err))
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(name)
		return fmt.Errorf("batchx: close record: %w", err)
	}
	if err := os.Rename(name, p); err != nil {
		_ = os.Remove(name)
		return fmt.Errorf("batchx: publish record: %w", err)
	}
	return nil
}
