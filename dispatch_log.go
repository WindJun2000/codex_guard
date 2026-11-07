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

const defaultDispatchLogMax = 500

// dispatchPoolItem is one candidate considered during a pick, kept for the
// history visualization so users can see how accounts were ranked.
type dispatchPoolItem struct {
	AuthID   string  `json:"auth_id"`
	Provider string  `json:"provider,omitempty"`
	Priority int     `json:"priority,omitempty"`
	Status   string  `json:"status,omitempty"`
	Role     string  `json:"role"`
	Weight   float64 `json:"weight,omitempty"`
	Score    float64 `json:"score"`
}

// dispatchCandidateDetail records every auth candidate seen at pick time plus
// whether it was selectable and why it was skipped, for full auditability.
type dispatchCandidateDetail struct {
	AuthID     string            `json:"auth_id"`
	Provider   string            `json:"provider,omitempty"`
	Priority   int               `json:"priority,omitempty"`
	Status     string            `json:"status,omitempty"`
	Role       string            `json:"role,omitempty"`
	Weight     float64           `json:"weight,omitempty"`
	Score      float64           `json:"score,omitempty"`
	Selectable bool              `json:"selectable"`
	SkipReason string            `json:"skip_reason,omitempty"`
	Attributes map[string]string `json:"attributes,omitempty"`
}

// dispatchEntry is one scheduling decision recorded for audit / visualization.
type dispatchEntry struct {
	Timestamp       string   `json:"timestamp"`
	Provider        string   `json:"provider,omitempty"`
	Providers       []string `json:"providers,omitempty"`
	Model           string   `json:"model,omitempty"`
	Stream          bool     `json:"stream,omitempty"`
	SessionID       string   `json:"session_id,omitempty"`
	ParentSessionID string   `json:"parent_session_id,omitempty"`
	RequestID       string   `json:"request_id,omitempty"`
	SelectionID     string   `json:"selection_id,omitempty"`

	// Subscription / billing snapshot of the selected account at pick time,
	// persisted so history export can show plan, renewal and credit fields.
	PlanType                string `json:"plan_type,omitempty"`
	PlanLabel               string `json:"plan_label,omitempty"`
	SubscriptionActiveUntil string `json:"subscription_active_until,omitempty"`
	CreditBalance           string `json:"credit_balance,omitempty"`
	ResetCreditsAvailable   *int   `json:"reset_credits_available,omitempty"`

	Strategy string `json:"strategy"`
	Result   string `json:"result"`
	Reason   string `json:"reason,omitempty"`
	// Source marks the origin of this dispatch: empty for normal business
	// scheduling, "degradation_test" for specified (pinned-auth) calls that
	// originate from degradation checks (codex-guard self-eval or candy-eval).
	Source        string                    `json:"source,omitempty"`
	Candidates    int                       `json:"candidates"`
	PoolSize      int                       `json:"pool_size"`
	Selected      string                    `json:"selected,omitempty"`
	SelectedScore float64                   `json:"selected_score,omitempty"`
	Pool          []dispatchPoolItem        `json:"pool,omitempty"`
	CandidateList []dispatchCandidateDetail `json:"candidate_list,omitempty"`
}

var (
	dispatchLogMu          sync.Mutex
	dispatchLog            = make([]dispatchEntry, 0, defaultDispatchLogMax)
	dispatchLogLoaded      bool
	dispatchLogLoadedPath  string
	dispatchLogDirty       bool
	dispatchLogPersistOnce sync.Once
	dispatchLogPersistStop chan struct{}
)

func effectiveHistoryMax() int {
	n := activeConfig().HistoryMaxEntries
	if n <= 0 {
		return defaultDispatchLogMax
	}
	return n
}

// historyFilePath returns the persistent dispatch-history file. It lives next
// to the plugin settings file so it survives container restarts and redeploys.
func historyFilePath() string {
	if p := strings.TrimSpace(os.Getenv("CODEX_GUARD_HISTORY_FILE")); p != "" {
		return p
	}
	return filepath.Join(filepath.Dir(pluginStatePath()), "codex-guard.history.json")
}

// recordDispatch appends one scheduling event, trimming to the configured max.
func recordDispatch(e dispatchEntry) {
	e.Timestamp = time.Now().Format(time.RFC3339)
	fillDispatchSubscription(&e)
	dispatchLogMu.Lock()
	dispatchLog = append(dispatchLog, e)
	trimDispatchLogLocked()
	dispatchLogDirty = true
	dispatchLogMu.Unlock()
}

// fillDispatchSubscription snapshots the selected account's subscription /
// billing fields into a dispatch entry. No-op when no account was selected.
func fillDispatchSubscription(e *dispatchEntry) {
	if e == nil || e.Selected == "" {
		return
	}
	st := stateFor(e.Selected)
	if st == nil {
		return
	}
	q := st.Quota
	e.PlanType = q.PlanType
	e.PlanLabel = planLabel(q.PlanType)
	if !q.SubscriptionActiveUntil.IsZero() {
		e.SubscriptionActiveUntil = q.SubscriptionActiveUntil.Format(time.RFC3339)
	}
	e.CreditBalance = q.CreditBalance
	if q.ResetCreditsAvailableCount != nil {
		c := *q.ResetCreditsAvailableCount
		e.ResetCreditsAvailable = &c
	}
}

func trimDispatchLogLocked() {
	max := effectiveHistoryMax()
	if len(dispatchLog) > max {
		trimmed := make([]dispatchEntry, max)
		copy(trimmed, dispatchLog[len(dispatchLog)-max:])
		dispatchLog = trimmed
	}
}

// trimDispatchLogNow applies the configured max immediately and marks the log
// dirty when anything was dropped, so lowering history_max_entries takes effect
// without waiting for the next scheduling record.
func trimDispatchLogNow() {
	dispatchLogMu.Lock()
	defer dispatchLogMu.Unlock()
	before := len(dispatchLog)
	trimDispatchLogLocked()
	if len(dispatchLog) != before {
		dispatchLogDirty = true
	}
}

// dispatchLogSnapshot returns up to limit entries, newest first.
func dispatchLogSnapshot(limit int) []dispatchEntry {
	dispatchLogMu.Lock()
	defer dispatchLogMu.Unlock()
	n := len(dispatchLog)
	if limit <= 0 || limit > n {
		limit = n
	}
	out := make([]dispatchEntry, 0, limit)
	for i := n - 1; i >= n-limit; i-- {
		out = append(out, dispatchLog[i])
	}
	return out
}

// dispatchLogAllChronological returns a copy of the full history in
// oldest-first order, used for CSV export.
func dispatchLogAllChronological() []dispatchEntry {
	dispatchLogMu.Lock()
	defer dispatchLogMu.Unlock()
	return append([]dispatchEntry(nil), dispatchLog...)
}

func dispatchLogCount() int {
	dispatchLogMu.Lock()
	defer dispatchLogMu.Unlock()
	return len(dispatchLog)
}

func clearDispatchLog() {
	dispatchLogMu.Lock()
	dispatchLog = dispatchLog[:0]
	dispatchLogDirty = true
	dispatchLogMu.Unlock()
}

// loadDispatchLog restores history from disk. It reloads when the history file
// path changes (e.g. a different CODEX_GUARD_HISTORY_FILE after a reconfigure)
// and never clobbers entries that were recorded in memory before/while loading.
func loadDispatchLog() {
	path := historyFilePath()
	dispatchLogMu.Lock()
	if dispatchLogLoaded && dispatchLogLoadedPath == path {
		dispatchLogMu.Unlock()
		return
	}
	dispatchLogLoaded = true
	dispatchLogLoadedPath = path
	dispatchLogMu.Unlock()

	raw, err := os.ReadFile(path)
	if err != nil {
		return
	}
	var disk []dispatchEntry
	if err := json.Unmarshal(raw, &disk); err != nil {
		logf("warn", "dispatch history file invalid: %v", err)
		return
	}
	dispatchLogMu.Lock()
	// Re-snapshot after the disk read so entries recorded while the file was
	// being read are not clobbered by the on-disk copy.
	current := append([]dispatchEntry(nil), dispatchLog...)
	dispatchLog = mergeDispatchEntries(disk, current)
	trimDispatchLogLocked()
	dispatchLogMu.Unlock()
}

// mergeDispatchEntries unions two history slices, deduplicating by identity
// fingerprint and keeping the result ordered oldest-first.
func mergeDispatchEntries(base, extra []dispatchEntry) []dispatchEntry {
	seen := make(map[string]bool, len(base))
	out := make([]dispatchEntry, 0, len(base)+len(extra))
	for _, e := range base {
		seen[dispatchEntryKey(e)] = true
		out = append(out, e)
	}
	for _, e := range extra {
		k := dispatchEntryKey(e)
		if seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, e)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Timestamp < out[j].Timestamp })
	return out
}

func dispatchEntryKey(e dispatchEntry) string {
	return e.Timestamp + "|" + e.RequestID + "|" + e.SelectionID + "|" +
		e.SessionID + "|" + e.Selected + "|" + e.Result + "|" + e.Reason
}

// startDispatchLogPersister starts the debounced background writer once.
func startDispatchLogPersister() {
	dispatchLogPersistOnce.Do(func() {
		dispatchLogPersistStop = make(chan struct{})
		go dispatchLogPersistLoop()
	})
}

// stopDispatchLogPersister halts the background writer (called from shutdown).
func stopDispatchLogPersister() {
	if dispatchLogPersistStop == nil {
		return
	}
	select {
	case <-dispatchLogPersistStop:
	default:
		close(dispatchLogPersistStop)
	}
}

func dispatchLogPersistLoop() {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-dispatchLogPersistStop:
			return
		case <-ticker.C:
			persistDispatchLogIfDirty()
		}
	}
}

func persistDispatchLogIfDirty() {
	dispatchLogMu.Lock()
	if !dispatchLogDirty {
		dispatchLogMu.Unlock()
		return
	}
	dispatchLogDirty = false
	entries := append([]dispatchEntry(nil), dispatchLog...)
	dispatchLogMu.Unlock()

	path := historyFilePath()
	data, err := json.MarshalIndent(entries, "", "  ")
	if err != nil {
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		logf("warn", "persist dispatch history failed: %v", err)
	}
}
