package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const defaultEvalHistoryMax = 500

// Raw-output caps for degradation history: answers keep enough context to
// audit the model output, errors keep the full host error message where
// reasonable, without letting the history file grow without bound.
const (
	evalRawAnswerMax = 16000
	evalRawErrorMax  = 4000
)

// evalHistoryEntry records one degradation check for one account. It is
// append-only and grouped by AuthID in the management console.
type evalHistoryEntry struct {
	AuthID                  string                         `json:"auth_id"`
	Timestamp               string                         `json:"timestamp"`
	Status                  string                         `json:"status"` // normal | degraded | unknown
	Candy                   string                         `json:"candy,omitempty"`
	Fingerprint             string                         `json:"fingerprint,omitempty"`
	Prediction              string                         `json:"prediction,omitempty"`
	PredictionName          string                         `json:"prediction_name,omitempty"`
	Probability             string                         `json:"probability,omitempty"`
	Family                  string                         `json:"family,omitempty"`
	CandyAnswer             string                         `json:"candy_answer,omitempty"`
	CandyError              string                         `json:"candy_error,omitempty"`
	FingerprintError        string                         `json:"fingerprint_error,omitempty"`
	FingerprintSamplesError string                         `json:"fingerprint_samples_error,omitempty"`
	FingerprintSamples      []evalHistoryFingerprintSample `json:"fingerprint_samples,omitempty"`
	SkipReason              string                         `json:"skip_reason,omitempty"`
	Model                   string                         `json:"model,omitempty"`
	Effort                  string                         `json:"effort,omitempty"`
	DetectorRev             string                         `json:"detector_revision,omitempty"`
}

// evalHistoryFingerprintSample is one fingerprint challenge's raw output and
// error, persisted so the management console can show the original answer.
type evalHistoryFingerprintSample struct {
	Index    int    `json:"index"`
	Expected int    `json:"expected_count,omitempty"`
	Attempts int    `json:"attempts,omitempty"`
	Parsed   int    `json:"parsed_numbers,omitempty"`
	Valid    bool   `json:"accepted,omitempty"`
	Answer   string `json:"answer,omitempty"`
	Error    string `json:"error,omitempty"`
}

var (
	evalHistoryMu          sync.Mutex
	evalHistory            = make([]evalHistoryEntry, 0, defaultEvalHistoryMax)
	evalHistoryLoaded      bool
	evalHistoryLoadedPath  string
	evalHistoryDirty       bool
	evalHistoryPersistOnce sync.Once
	evalHistoryPersistStop chan struct{}
)

func effectiveEvalHistoryMax() int {
	n := activeConfig().EvalHistoryMaxEntries
	if n <= 0 {
		return defaultEvalHistoryMax
	}
	return n
}

// evalHistoryFilePath returns the persistent eval-history file, next to plugin settings.
func evalHistoryFilePath() string {
	if p := strings.TrimSpace(os.Getenv("CODEX_GUARD_EVAL_HISTORY_FILE")); p != "" {
		return p
	}
	return filepath.Join(filepath.Dir(pluginStatePath()), "codex-guard.eval-history.json")
}

// recordEvalHistory appends one check result and trims to the configured max.
func recordEvalHistory(e evalHistoryEntry) {
	e.Timestamp = time.Now().Format(time.RFC3339)
	evalHistoryMu.Lock()
	evalHistory = append(evalHistory, e)
	trimEvalHistoryLocked()
	evalHistoryDirty = true
	evalHistoryMu.Unlock()
}

func trimEvalHistoryLocked() {
	max := effectiveEvalHistoryMax()
	if len(evalHistory) > max {
		trimmed := make([]evalHistoryEntry, max)
		copy(trimmed, evalHistory[len(evalHistory)-max:])
		evalHistory = trimmed
	}
}

// trimEvalHistoryNow applies the configured max immediately and marks the
// history dirty when anything was dropped, so lowering eval_history_max_entries
// takes effect without waiting for the next check record.
func trimEvalHistoryNow() {
	evalHistoryMu.Lock()
	defer evalHistoryMu.Unlock()
	before := len(evalHistory)
	trimEvalHistoryLocked()
	if len(evalHistory) != before {
		evalHistoryDirty = true
	}
}

// evalHistorySnapshot returns up to limit entries, newest first.
func evalHistorySnapshot(limit int) []evalHistoryEntry {
	evalHistoryMu.Lock()
	defer evalHistoryMu.Unlock()
	n := len(evalHistory)
	if limit <= 0 || limit > n {
		limit = n
	}
	out := make([]evalHistoryEntry, 0, limit)
	for i := n - 1; i >= n-limit; i-- {
		out = append(out, evalHistory[i])
	}
	return out
}

// evalHistoryAllChronological returns a copy of the full history oldest-first.
func evalHistoryAllChronological() []evalHistoryEntry {
	evalHistoryMu.Lock()
	defer evalHistoryMu.Unlock()
	return append([]evalHistoryEntry(nil), evalHistory...)
}

func evalHistoryCount() int {
	evalHistoryMu.Lock()
	defer evalHistoryMu.Unlock()
	return len(evalHistory)
}

func clearEvalHistory() {
	evalHistoryMu.Lock()
	evalHistory = evalHistory[:0]
	evalHistoryDirty = true
	evalHistoryMu.Unlock()
}

// loadEvalHistory restores history from disk. It reloads when the file path
// changes and never clobbers entries recorded in memory before/while loading.
func loadEvalHistory() {
	path := evalHistoryFilePath()
	evalHistoryMu.Lock()
	if evalHistoryLoaded && evalHistoryLoadedPath == path {
		evalHistoryMu.Unlock()
		return
	}
	evalHistoryLoaded = true
	evalHistoryLoadedPath = path
	evalHistoryMu.Unlock()

	raw, err := os.ReadFile(path)
	if err != nil {
		return
	}
	var disk []evalHistoryEntry
	if err := json.Unmarshal(raw, &disk); err != nil {
		logf("warn", "eval history file invalid: %v", err)
		return
	}
	evalHistoryMu.Lock()
	// Re-snapshot after the disk read so entries recorded while the file was
	// being read are not clobbered by the on-disk copy.
	current := append([]evalHistoryEntry(nil), evalHistory...)
	evalHistory = mergeEvalHistoryEntries(disk, current)
	trimEvalHistoryLocked()
	evalHistoryMu.Unlock()
}

// mergeEvalHistoryEntries unions two history slices, deduplicating by account
// and timestamp and keeping the result ordered oldest-first.
func mergeEvalHistoryEntries(base, extra []evalHistoryEntry) []evalHistoryEntry {
	seen := make(map[string]bool, len(base))
	out := make([]evalHistoryEntry, 0, len(base)+len(extra))
	for _, e := range base {
		seen[evalHistoryEntryKey(e)] = true
		out = append(out, e)
	}
	for _, e := range extra {
		k := evalHistoryEntryKey(e)
		if seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, e)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Timestamp < out[j].Timestamp })
	return out
}

func evalHistoryEntryKey(e evalHistoryEntry) string {
	return e.AuthID + "|" + e.Timestamp
}

// startEvalHistoryPersister starts the debounced background writer once.
func startEvalHistoryPersister() {
	evalHistoryPersistOnce.Do(func() {
		evalHistoryPersistStop = make(chan struct{})
		go evalHistoryPersistLoop()
	})
}

// stopEvalHistoryPersister halts the background writer (called from shutdown).
func stopEvalHistoryPersister() {
	if evalHistoryPersistStop == nil {
		return
	}
	select {
	case <-evalHistoryPersistStop:
	default:
		close(evalHistoryPersistStop)
	}
}

func evalHistoryPersistLoop() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-evalHistoryPersistStop:
			return
		case <-ticker.C:
			persistEvalHistoryIfDirty()
		}
	}
}

func persistEvalHistoryIfDirty() {
	evalHistoryMu.Lock()
	if !evalHistoryDirty {
		evalHistoryMu.Unlock()
		return
	}
	evalHistoryDirty = false
	entries := append([]evalHistoryEntry(nil), evalHistory...)
	evalHistoryMu.Unlock()

	path := evalHistoryFilePath()
	data, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		logf("warn", "persist eval history failed: %v", err)
	}
}
