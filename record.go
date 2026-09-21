package batchx

import (
	"crypto/sha256"
	"encoding/hex"
	"maps"
	"slices"
	"strconv"
	"time"
)

// recordVersion is the on-disk schema version of JobRecord.
const recordVersion = 1

// Status is the lifecycle state of a job or step execution.
type Status string

// Status values.
const (
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
	// StatusStopped means the context was cancelled; the run is restartable.
	StatusStopped Status = "stopped"
)

// Params identify a job instance together with the job name: running the same
// job with the same Params is a restart of one logical instance.
type Params map[string]string

// Key returns the stable identity of the instance (name plus params).
func (p Params) key(name string) string {
	keys := slices.Sorted(maps.Keys(p))
	h := sha256.New()
	add := func(s string) {
		h.Write([]byte(strconv.Itoa(len(s))))
		h.Write([]byte{':'})
		h.Write([]byte(s))
	}
	add(name)
	for _, k := range keys {
		add(k)
		add(p[k])
	}
	return hex.EncodeToString(h.Sum(nil))[:32]
}

// StepRecord is the persisted state of one step. Counters reflect committed
// chunks only.
type StepRecord struct {
	Name     string `json:"name"`
	Status   Status `json:"status"`
	Attempts int    `json:"attempts"`
	// Position is the restart checkpoint: the number of reader positions
	// (items plus skipped read errors) consumed by committed chunks.
	Position  int64     `json:"position"`
	Read      int64     `json:"read"`
	Written   int64     `json:"written"`
	Filtered  int64     `json:"filtered"`
	Skipped   int64     `json:"skipped"`
	Retries   int64     `json:"retries"`
	Commits   int64     `json:"commits"`
	Err       string    `json:"err,omitempty"`
	StartedAt time.Time `json:"startedAt,omitzero"`
	EndedAt   time.Time `json:"endedAt,omitzero"`
}

// JobRecord is the persisted state of one job instance. It is the only thing
// a Repository has to store, as JSON if it wants.
type JobRecord struct {
	Version   int          `json:"version"`
	Name      string       `json:"name"`
	Key       string       `json:"key"`
	Params    Params       `json:"params,omitempty"`
	Status    Status       `json:"status"`
	Attempts  int          `json:"attempts"`
	Steps     []StepRecord `json:"steps"`
	Err       string       `json:"err,omitempty"`
	StartedAt time.Time    `json:"startedAt,omitzero"`
	UpdatedAt time.Time    `json:"updatedAt,omitzero"`
	EndedAt   time.Time    `json:"endedAt,omitzero"`
}

// Clone returns a deep copy.
func (r JobRecord) Clone() JobRecord {
	c := r
	if r.Params != nil {
		c.Params = maps.Clone(r.Params)
	}
	c.Steps = append([]StepRecord(nil), r.Steps...)
	return c
}

// Step returns the record of the named step.
func (r JobRecord) Step(name string) (StepRecord, bool) {
	for _, s := range r.Steps {
		if s.Name == name {
			return s, true
		}
	}
	return StepRecord{}, false
}
