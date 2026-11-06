package main

import (
	"strconv"
	"sync"
	"time"
)

// QuotaSnapshot is a normalized view of one account's remaining quota.
type QuotaSnapshot struct {
	RemainingPercent float64
	ResetAt          time.Time
	WindowSeconds    int64
	ObservedAt       time.Time
}

// AccountState holds transient runtime state only. Role / weight / maxParallel /
// enabled live in Config and are resolved per-pick via the effective* helpers.
type AccountState struct {
	AuthID              string
	CooldownUntil       time.Time
	CircuitOpenUntil    time.Time
	OverloadUntil       time.Time
	ConsecutiveFailures int
	CooldownEvents      []time.Time
	Disabled            bool
	Quota               QuotaSnapshot
	InFlight            map[string]time.Time
	LastUsed            time.Time
}

var (
	stateMu             sync.Mutex
	globalAccountStates = map[string]*AccountState{}
)

// stateFor returns a copy of one account's state, or nil if unseen.
func stateFor(id string) *AccountState {
	stateMu.Lock()
	defer stateMu.Unlock()
	st, ok := globalAccountStates[id]
	if !ok {
		return nil
	}
	sweepInFlightLocked(st, time.Now(), inFlightTTL())
	cp := *st
	cp.CooldownEvents = append([]time.Time(nil), st.CooldownEvents...)
	cp.InFlight = copyInFlight(st.InFlight)
	return &cp
}

// snapshotStates returns copies of all account states.
func snapshotStates() map[string]*AccountState {
	stateMu.Lock()
	defer stateMu.Unlock()
	out := make(map[string]*AccountState, len(globalAccountStates))
	for k, v := range globalAccountStates {
		sweepInFlightLocked(v, time.Now(), inFlightTTL())
		cp := *v
		cp.CooldownEvents = append([]time.Time(nil), v.CooldownEvents...)
		cp.InFlight = copyInFlight(v.InFlight)
		out[k] = &cp
	}
	return out
}

// ensureLocked returns (or creates) an account state; stateMu must be held.
func ensureLocked(id string) *AccountState {
	st, ok := globalAccountStates[id]
	if !ok {
		st = &AccountState{AuthID: id}
		globalAccountStates[id] = st
	}
	return st
}

// applyCooldown puts an account into cooldown, with escalation doubling when the
// account has repeatedly cooled within the escalation window.
func applyCooldown(id string, seconds int) {
	if seconds <= 0 {
		return
	}
	cfg := activeConfig()
	if cfg.MaxCooldownSeconds > 0 && seconds > cfg.MaxCooldownSeconds {
		seconds = cfg.MaxCooldownSeconds
	}
	now := time.Now()
	stateMu.Lock()
	defer stateMu.Unlock()
	st := ensureLocked(id)

	cutoff := now.Add(-time.Duration(cfg.EscalationWindowSeconds) * time.Second)
	kept := st.CooldownEvents[:0]
	for _, t := range st.CooldownEvents {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	st.CooldownEvents = kept
	if cfg.EscalationThreshold > 0 && len(st.CooldownEvents) >= cfg.EscalationThreshold {
		seconds = int(float64(seconds) * cfg.EscalationMultiplier)
		if cfg.MaxCooldownSeconds > 0 && seconds > cfg.MaxCooldownSeconds {
			seconds = cfg.MaxCooldownSeconds
		}
	}
	st.CooldownEvents = append(st.CooldownEvents, now)
	st.CooldownUntil = now.Add(time.Duration(seconds) * time.Second)
	logf("info", "account %s cooldown %ds until %s", id, seconds, st.CooldownUntil.Format(time.RFC3339))
}

// recordFailure increments the circuit-breaker counter and opens the breaker if
// the threshold is reached. Returns true if the breaker just opened.
func recordFailure(id string) bool {
	cfg := activeConfig()
	stateMu.Lock()
	defer stateMu.Unlock()
	st := ensureLocked(id)
	if cfg.CircuitFailureThreshold <= 0 {
		return false
	}
	st.ConsecutiveFailures++
	if st.ConsecutiveFailures >= cfg.CircuitFailureThreshold {
		st.CircuitOpenUntil = time.Now().Add(time.Duration(cfg.CircuitOpenDurationSeconds) * time.Second)
		st.ConsecutiveFailures = 0
		return true
	}
	return false
}

// recordSuccess clears failure counters and cooldown for an account.
func recordSuccess(id string) {
	stateMu.Lock()
	defer stateMu.Unlock()
	st, ok := globalAccountStates[id]
	if !ok {
		return
	}
	st.ConsecutiveFailures = 0
	st.CooldownUntil = time.Time{}
	st.LastUsed = time.Now()
}

func disableAccount(id string) {
	stateMu.Lock()
	defer stateMu.Unlock()
	st := ensureLocked(id)
	st.Disabled = true
	logf("warn", "account %s disabled", id)
}

func reEnableAccount(id string) {
	stateMu.Lock()
	defer stateMu.Unlock()
	st, ok := globalAccountStates[id]
	if !ok {
		return
	}
	st.Disabled = false
	st.ConsecutiveFailures = 0
	st.CooldownUntil = time.Time{}
	logf("info", "account %s re-enabled", id)
}

// applyOverload parks an account for seconds after an overload rule triggers.
func applyOverload(id string, seconds int) {
	if seconds <= 0 {
		return
	}
	now := time.Now()
	stateMu.Lock()
	defer stateMu.Unlock()
	st := ensureLocked(id)
	st.OverloadUntil = now.Add(time.Duration(seconds) * time.Second)
	logf("warn", "account %s overload %ds until %s", id, seconds, st.OverloadUntil.Format(time.RFC3339))
}

func resetCooldown(id string) {
	stateMu.Lock()
	defer stateMu.Unlock()
	if st, ok := globalAccountStates[id]; ok {
		st.CooldownUntil = time.Time{}
		st.CooldownEvents = nil
	}
}

func resetOverload(id string) {
	stateMu.Lock()
	defer stateMu.Unlock()
	if st, ok := globalAccountStates[id]; ok {
		st.OverloadUntil = time.Time{}
	}
}

// updateQuota stores a fresh quota snapshot and feeds the overload tracker.
func updateQuota(id string, q QuotaSnapshot) {
	now := time.Now()
	if ws := effectiveWindowSeconds(activeConfig(), id); q.WindowSeconds <= 0 && ws > 0 {
		q.WindowSeconds = ws
	}
	if q.ResetAt.IsZero() && q.WindowSeconds > 0 {
		// Endpoint did not disclose a reset time; fall back to the account's
		// declared window length (a user-supplied tier hint, not a hardcoded
		// guess). This is an upper-bound estimate of remaining time.
		q.ResetAt = now.Add(time.Duration(q.WindowSeconds) * time.Second)
	}
	stateMu.Lock()
	st := ensureLocked(id)
	q.ObservedAt = now
	st.Quota = q
	stateMu.Unlock()

	recordQuotaObservation(id, q.RemainingPercent, now)
	maybeApplyOverload(id, now)
}

// acquireSlot reserves one in-flight slot for an account. Slots are keyed by
// selection_id so request.complete can release the exact slot it owns; a
// missing selection id falls back to an anonymous unique lease.
func acquireSlot(id, selectionID string) {
	stateMu.Lock()
	defer stateMu.Unlock()
	st := ensureLocked(id)
	now := time.Now()
	sweepInFlightLocked(st, now, inFlightTTL())
	if st.InFlight == nil {
		st.InFlight = make(map[string]time.Time)
	}
	if selectionID == "" {
		selectionID = "anon:" + strconv.FormatInt(now.UnixNano(), 10)
	}
	st.InFlight[selectionID] = now
	st.LastUsed = now
}

// releaseSlot returns the in-flight slot owned by selectionID. Without a
// selection id it releases the oldest lease as a best-effort fallback.
func releaseSlot(id, selectionID string) {
	stateMu.Lock()
	defer stateMu.Unlock()
	st, ok := globalAccountStates[id]
	if !ok || len(st.InFlight) == 0 {
		return
	}
	if selectionID != "" {
		delete(st.InFlight, selectionID)
		return
	}
	var oldestKey string
	var oldestAt time.Time
	for k, t := range st.InFlight {
		if oldestAt.IsZero() || t.Before(oldestAt) {
			oldestKey = k
			oldestAt = t
		}
	}
	if oldestKey != "" {
		delete(st.InFlight, oldestKey)
	}
}

// sweepInFlightLocked removes expired slot leases; stateMu must be held.
func sweepInFlightLocked(st *AccountState, now time.Time, ttl time.Duration) {
	if len(st.InFlight) == 0 {
		return
	}
	for k, t := range st.InFlight {
		if now.Sub(t) >= ttl {
			delete(st.InFlight, k)
		}
	}
}

// inFlightTTL returns the configured slot lease TTL, falling back to 300s.
func inFlightTTL() time.Duration {
	ttl := time.Duration(activeConfig().InFlightTTLSeconds) * time.Second
	if ttl <= 0 {
		ttl = 300 * time.Second
	}
	return ttl
}

// inFlightCount returns the number of live (non-expired) in-flight slots.
func inFlightCount(id string) int {
	stateMu.Lock()
	defer stateMu.Unlock()
	st, ok := globalAccountStates[id]
	if !ok {
		return 0
	}
	sweepInFlightLocked(st, time.Now(), inFlightTTL())
	return len(st.InFlight)
}

// copyInFlight deep-copies the slot lease map.
func copyInFlight(src map[string]time.Time) map[string]time.Time {
	if len(src) == 0 {
		return nil
	}
	out := make(map[string]time.Time, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}
