package main

import (
	"sync"
	"time"
)

// Overload tracking uses two event streams per account:
//   - token events from UsageRecord.Detail.TotalTokens (metric "token")
//   - quota observations from updateQuota (metric "quota_percent", as the
//     remaining-percent drop within a window)

type tokenEvent struct {
	At     time.Time
	Tokens float64
}

type quotaEvent struct {
	At               time.Time
	RemainingPercent float64
}

var (
	overloadMu  sync.Mutex
	tokenEvents = map[string][]tokenEvent{}
	quotaEvents = map[string][]quotaEvent{}
)

const overloadEventRetention = time.Hour

func recordTokenUsage(authID string, tokens float64, now time.Time) {
	if tokens <= 0 {
		return
	}
	overloadMu.Lock()
	defer overloadMu.Unlock()
	tokenEvents[authID] = append(tokenEvents[authID], tokenEvent{At: now, Tokens: tokens})
	tokenEvents[authID] = pruneToken(tokenEvents[authID], now.Add(-overloadEventRetention))
}

func recordQuotaObservation(authID string, remainingPercent float64, now time.Time) {
	overloadMu.Lock()
	defer overloadMu.Unlock()
	quotaEvents[authID] = append(quotaEvents[authID], quotaEvent{At: now, RemainingPercent: remainingPercent})
	quotaEvents[authID] = pruneQuota(quotaEvents[authID], now.Add(-overloadEventRetention))
}

// maybeApplyOverload evaluates the account's effective overload rules and parks
// it for the longest triggered stop duration.
func maybeApplyOverload(authID string, now time.Time) {
	cfg := activeConfig()
	rules := effectiveOverloadRules(cfg, authID)
	if len(rules) == 0 {
		return
	}
	if stop := evaluateOverload(rules, authID, now); stop > 0 {
		applyOverload(authID, stop)
	}
}

// evaluateOverload returns the longest stop_seconds among triggered rules, or 0.
func evaluateOverload(rules []OverloadRule, authID string, now time.Time) int {
	var maxWindow int64
	for _, r := range rules {
		if r.WindowSeconds > maxWindow {
			maxWindow = r.WindowSeconds
		}
	}
	if maxWindow <= 0 {
		return 0
	}
	cutoffAll := now.Add(-time.Duration(maxWindow) * time.Second)

	overloadMu.Lock()
	defer overloadMu.Unlock()

	tokenEvents[authID] = pruneToken(tokenEvents[authID], cutoffAll)
	quotaEvents[authID] = pruneQuota(quotaEvents[authID], cutoffAll)

	stop := 0
	for _, r := range rules {
		if r.WindowSeconds <= 0 || r.StopSeconds <= 0 {
			continue
		}
		cutoff := now.Add(-time.Duration(r.WindowSeconds) * time.Second)
		triggered := false
		switch r.Metric {
		case "token":
			var sum float64
			for _, e := range tokenEvents[authID] {
				if e.At.After(cutoff) {
					sum += e.Tokens
				}
			}
			triggered = sum > r.Limit
		case "quota_percent":
			triggered = quotaDropExceeds(quotaEvents[authID], cutoff, r.Limit)
		}
		if triggered && r.StopSeconds > stop {
			stop = r.StopSeconds
		}
	}
	if stop > 0 {
		delete(tokenEvents, authID)
		delete(quotaEvents, authID)
	}
	return stop
}

func quotaDropExceeds(evs []quotaEvent, cutoff time.Time, limit float64) bool {
	var maxP, minP float64
	first := true
	for _, e := range evs {
		if !e.At.After(cutoff) {
			continue
		}
		if first {
			maxP, minP = e.RemainingPercent, e.RemainingPercent
			first = false
			continue
		}
		if e.RemainingPercent > maxP {
			maxP = e.RemainingPercent
		}
		if e.RemainingPercent < minP {
			minP = e.RemainingPercent
		}
	}
	return !first && (maxP-minP) > limit
}

func pruneToken(evs []tokenEvent, cutoff time.Time) []tokenEvent {
	out := evs[:0]
	for _, e := range evs {
		if e.At.After(cutoff) {
			out = append(out, e)
		}
	}
	return out
}

func pruneQuota(evs []quotaEvent, cutoff time.Time) []quotaEvent {
	out := evs[:0]
	for _, e := range evs {
		if e.At.After(cutoff) {
			out = append(out, e)
		}
	}
	return out
}
