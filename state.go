package main

import (
	"strconv"
	"sync"
	"time"
)

// QuotaWindow is one rate-limit window reported by the usage endpoint.
type QuotaWindow struct {
	Name             string    `json:"name"`
	UsedPercent      float64   `json:"used_percent"`
	RemainingPercent float64   `json:"remaining_percent"`
	WindowSeconds    int64     `json:"window_seconds"`
	ResetAt          time.Time `json:"reset_at"`
}

// ResetCredit is one proactive rate-limit reset credit disclosed by OpenAI.
type ResetCredit struct {
	ID        string    `json:"id,omitempty"`
	Status    string    `json:"status,omitempty"`
	Title     string    `json:"title,omitempty"`
	ResetType string    `json:"reset_type,omitempty"`
	GrantedAt time.Time `json:"granted_at,omitempty"`
	ExpiresAt time.Time `json:"expires_at,omitempty"`
}

// QuotaSnapshot is a normalized view of one account's remaining quota.
type QuotaSnapshot struct {
	RemainingPercent float64       `json:"remaining_percent"`
	UsedPercent      float64       `json:"used_percent"`
	ResetAt          time.Time     `json:"reset_at"`
	WindowSeconds    int64         `json:"window_seconds"`
	ObservedAt       time.Time     `json:"observed_at"`
	PlanType         string        `json:"plan_type"`
	Allowed          bool          `json:"allowed"`
	LimitReached     bool          `json:"limit_reached"`
	LimitReachedType string        `json:"limit_reached_type"`
	Windows          []QuotaWindow `json:"windows,omitempty"`

	// Subscription metadata decoded from the credential id_token.
	SubscriptionPlanType    string    `json:"subscription_plan_type,omitempty"`
	SubscriptionActiveStart time.Time `json:"subscription_active_start,omitempty"`
	SubscriptionActiveUntil time.Time `json:"subscription_active_until,omitempty"`

	// Credit balance from the wham/usage "credits" object.
	CreditBalance             string `json:"credit_balance,omitempty"`
	CreditHasCredits          bool   `json:"credit_has_credits,omitempty"`
	CreditUnlimited           bool   `json:"credit_unlimited,omitempty"`
	CreditOverageLimitReached bool   `json:"credit_overage_limit_reached,omitempty"`

	// Proactive reset-credit counts from wham/usage "rate_limit_reset_credits".
	ResetCreditsAvailableCount  *int `json:"reset_credits_available_count,omitempty"`
	ResetCreditsApplicableCount *int `json:"reset_credits_applicable_count,omitempty"`

	// Detailed proactive reset credits from wham/rate-limit-reset-credits.
	ResetCredits []ResetCredit `json:"reset_credits,omitempty"`
}

// protectionMeta records why/when/how long one protection was applied so the
// management console can show trigger details on hover. It is transient runtime
// state only, like the Until fields it accompanies.
type protectionMeta struct {
	Reason    string
	StartedAt time.Time
	Duration  time.Duration
}

// AccountState holds transient runtime state only. Role / weight / maxParallel /
// enabled live in Config and are resolved per-pick via the effective* helpers.
type AccountState struct {
	AuthID              string
	CooldownUntil       time.Time
	TransientPauseUntil time.Time
	CircuitOpenUntil    time.Time
	OverloadUntil       time.Time
	CooldownMeta        protectionMeta
	TransientPauseMeta  protectionMeta
	CircuitOpenMeta     protectionMeta
	OverloadMeta        protectionMeta
	ConsecutiveFailures int
	CooldownEvents      []time.Time
	Disabled            bool
	Quota               QuotaSnapshot
	InFlight            map[string]time.Time
	ParallelSamples     []parallelSample
	LastUsed            time.Time
}

// parallelSample records one in-flight concurrency observation for the
// high-pressure skip heuristic used by the degradation eval loop.
type parallelSample struct {
	At       time.Time
	Parallel int
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
	cp.ParallelSamples = copyParallelSamples(st.ParallelSamples)
	cp.Quota.Windows = append([]QuotaWindow(nil), st.Quota.Windows...)
	cp.Quota.ResetCredits = append([]ResetCredit(nil), st.Quota.ResetCredits...)
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
		cp.ParallelSamples = copyParallelSamples(v.ParallelSamples)
		cp.Quota.Windows = append([]QuotaWindow(nil), v.Quota.Windows...)
		cp.Quota.ResetCredits = append([]ResetCredit(nil), v.Quota.ResetCredits...)
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
func applyCooldown(id string, seconds int, reason string) {
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
	until := now.Add(time.Duration(seconds) * time.Second)
	if until.After(st.CooldownUntil) {
		st.CooldownUntil = until
		st.CooldownMeta = protectionMeta{Reason: reason, StartedAt: now, Duration: time.Duration(seconds) * time.Second}
	}
	logf("info", "account %s cooldown %ds until %s", id, seconds, st.CooldownUntil.Format(time.RFC3339))
}

// applyTransientPause parks an account for a short non-escalating interval after
// a transient admission 429 (turn_rate_limit_exceeded / at capacity). Unlike
// applyCooldown it never appends to CooldownEvents, so a burst of these safe
// retries cannot inflate the escalation multiplier or trip the breaker.
func applyTransientPause(id string, seconds int, reason string) {
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
	until := now.Add(time.Duration(seconds) * time.Second)
	if until.After(st.TransientPauseUntil) {
		st.TransientPauseUntil = until
		st.TransientPauseMeta = protectionMeta{Reason: reason, StartedAt: now, Duration: time.Duration(seconds) * time.Second}
	}
	logf("info", "account %s transient pause %ds until %s", id, seconds, st.TransientPauseUntil.Format(time.RFC3339))
}

// recordFailure increments the circuit-breaker counter and opens the breaker if
// the threshold is reached. Returns true if the breaker just opened.
func recordFailure(id string, reason string) bool {
	cfg := activeConfig()
	stateMu.Lock()
	defer stateMu.Unlock()
	st := ensureLocked(id)
	if cfg.CircuitFailureThreshold <= 0 {
		return false
	}
	st.ConsecutiveFailures++
	if st.ConsecutiveFailures >= cfg.CircuitFailureThreshold {
		now := time.Now()
		until := now.Add(time.Duration(cfg.CircuitOpenDurationSeconds) * time.Second)
		if until.After(st.CircuitOpenUntil) {
			st.CircuitOpenUntil = until
			st.CircuitOpenMeta = protectionMeta{Reason: reason, StartedAt: now, Duration: time.Duration(cfg.CircuitOpenDurationSeconds) * time.Second}
		}
		st.ConsecutiveFailures = 0
		return true
	}
	return false
}

// recordSuccess clears the failure counter and the transient pause for an
// account. Cooldown / circuit / overload windows are intentionally left intact
// so a single success cannot reset a rate-limit backoff that other requests
// just triggered.
func recordSuccess(id string) {
	stateMu.Lock()
	defer stateMu.Unlock()
	st, ok := globalAccountStates[id]
	if !ok {
		return
	}
	st.ConsecutiveFailures = 0
	st.TransientPauseUntil = time.Time{}
	st.TransientPauseMeta = protectionMeta{}
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
	st.TransientPauseUntil = time.Time{}
	st.CircuitOpenUntil = time.Time{}
	st.OverloadUntil = time.Time{}
	st.CooldownEvents = nil
	st.CooldownMeta = protectionMeta{}
	st.TransientPauseMeta = protectionMeta{}
	st.CircuitOpenMeta = protectionMeta{}
	st.OverloadMeta = protectionMeta{}
	logf("info", "account %s re-enabled", id)
}

// applyOverload parks an account for seconds after an overload rule triggers.
func applyOverload(id string, seconds int, reason string) {
	if seconds <= 0 {
		return
	}
	now := time.Now()
	stateMu.Lock()
	defer stateMu.Unlock()
	st := ensureLocked(id)
	until := now.Add(time.Duration(seconds) * time.Second)
	if until.After(st.OverloadUntil) {
		st.OverloadUntil = until
		st.OverloadMeta = protectionMeta{Reason: reason, StartedAt: now, Duration: time.Duration(seconds) * time.Second}
	}
	logf("warn", "account %s overload %ds until %s", id, seconds, st.OverloadUntil.Format(time.RFC3339))
}

func resetCooldown(id string) {
	stateMu.Lock()
	defer stateMu.Unlock()
	if st, ok := globalAccountStates[id]; ok {
		st.CooldownUntil = time.Time{}
		st.TransientPauseUntil = time.Time{}
		st.CooldownEvents = nil
		st.CooldownMeta = protectionMeta{}
		st.TransientPauseMeta = protectionMeta{}
	}
}

func resetOverload(id string) {
	stateMu.Lock()
	defer stateMu.Unlock()
	if st, ok := globalAccountStates[id]; ok {
		st.OverloadUntil = time.Time{}
		st.OverloadMeta = protectionMeta{}
	}
}

func resetCircuit(id string) {
	stateMu.Lock()
	defer stateMu.Unlock()
	if st, ok := globalAccountStates[id]; ok {
		st.CircuitOpenUntil = time.Time{}
		st.ConsecutiveFailures = 0
		st.CircuitOpenMeta = protectionMeta{}
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
	markQuotaDirty()

	// Only feed the overload tracker when the snapshot carries real usage
	// evidence; a payload without used_percent leaves RemainingPercent at its
	// zero value and would otherwise look like an instant 100% drop.
	if q.RemainingPercent > 0 || q.UsedPercent > 0 || len(q.Windows) > 0 {
		recordQuotaObservation(id, q.RemainingPercent, now)
	}
	maybeApplyOverload(id, now)
}

// tryAcquireSlot atomically reserves one in-flight slot for an account when it
// is still under its concurrency limit. Slots are keyed by selection_id so
// request.complete can release the exact slot it owns; a missing selection id
// falls back to an anonymous unique lease. It returns false when the limit has
// already been reached so concurrent picks cannot over-allocate.
func tryAcquireSlot(id, selectionID string) bool {
	stateMu.Lock()
	defer stateMu.Unlock()
	st := ensureLocked(id)
	now := time.Now()
	sweepInFlightLocked(st, now, inFlightTTL())
	if len(st.InFlight) >= effectiveMaxParallel(activeConfig(), id) {
		return false
	}
	if st.InFlight == nil {
		st.InFlight = make(map[string]time.Time)
	}
	if selectionID == "" {
		selectionID = "anon:" + strconv.FormatInt(now.UnixNano(), 10)
	}
	st.InFlight[selectionID] = now
	st.ParallelSamples = appendParallelSample(st.ParallelSamples, now, len(st.InFlight))
	st.LastUsed = now
	return true
}

// releaseSlot returns the in-flight slot owned by selectionID. Without a
// selection id it releases the lease only when the account holds exactly one
// slot; otherwise it does nothing and defers to the TTL sweep. Guessing the
// "oldest" lease could drop a slot still owned by a concurrent request.
func releaseSlot(id, selectionID string) {
	stateMu.Lock()
	defer stateMu.Unlock()
	st, ok := globalAccountStates[id]
	if !ok || len(st.InFlight) == 0 {
		return
	}
	now := time.Now()
	if selectionID != "" {
		delete(st.InFlight, selectionID)
		st.ParallelSamples = appendParallelSample(st.ParallelSamples, now, len(st.InFlight))
		return
	}
	if len(st.InFlight) == 1 {
		for k := range st.InFlight {
			delete(st.InFlight, k)
		}
		st.ParallelSamples = appendParallelSample(st.ParallelSamples, now, 0)
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

// parallelSampleRetention caps the per-account concurrency history length and
// drops observations older than the retention window.
const parallelSampleRetention = time.Hour

func appendParallelSample(samples []parallelSample, at time.Time, parallel int) []parallelSample {
	samples = append(samples, parallelSample{At: at, Parallel: parallel})
	if len(samples) > 8192 {
		samples = samples[len(samples)-8192:]
	}
	return samples
}

func copyParallelSamples(src []parallelSample) []parallelSample {
	if len(src) == 0 {
		return nil
	}
	out := make([]parallelSample, len(src))
	copy(out, src)
	return out
}

// parallelPeakWithin returns the current concurrency and the highest concurrency
// observed within the trailing window for one account.
func parallelPeakWithin(id string, window time.Duration) (current, peak int) {
	if window <= 0 {
		window = 600 * time.Second
	}
	stateMu.Lock()
	defer stateMu.Unlock()
	st, ok := globalAccountStates[id]
	if !ok {
		return 0, 0
	}
	now := time.Now()
	sweepInFlightLocked(st, now, inFlightTTL())
	current = len(st.InFlight)
	peak = current
	cutoff := now.Add(-window)
	for _, s := range st.ParallelSamples {
		if s.At.Before(cutoff) {
			continue
		}
		if s.Parallel > peak {
			peak = s.Parallel
		}
	}
	return current, peak
}
