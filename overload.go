package main

import (
	"fmt"
	"sort"
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
	if stop, reason := evaluateOverload(rules, authID, now); stop > 0 {
		applyOverload(authID, stop, reason)
	}
}

// evaluateOverload returns the longest stop_seconds among triggered rules (or 0)
// plus a human-readable reason describing the winning rule.
func evaluateOverload(rules []OverloadRule, authID string, now time.Time) (int, string) {
	var maxWindow int64
	for _, r := range rules {
		if r.WindowSeconds > maxWindow {
			maxWindow = r.WindowSeconds
		}
	}
	if maxWindow <= 0 {
		return 0, ""
	}
	cutoffAll := now.Add(-time.Duration(maxWindow) * time.Second)

	overloadMu.Lock()
	defer overloadMu.Unlock()

	tokenEvents[authID] = pruneToken(tokenEvents[authID], cutoffAll)
	quotaEvents[authID] = pruneQuota(quotaEvents[authID], cutoffAll)
	sort.Slice(quotaEvents[authID], func(i, j int) bool {
		return quotaEvents[authID][i].At.Before(quotaEvents[authID][j].At)
	})

	stop := 0
	reason := ""
	triggeredToken := false
	triggeredQuota := false
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
		if !triggered {
			continue
		}
		if r.Metric == "token" {
			triggeredToken = true
		}
		if r.Metric == "quota_percent" {
			triggeredQuota = true
		}
		if r.StopSeconds > stop {
			stop = r.StopSeconds
			if r.Metric == "token" {
				reason = fmt.Sprintf("令牌窗口超限（%d秒内 > %.0f tokens）", r.WindowSeconds, r.Limit)
			} else {
				reason = fmt.Sprintf("额度窗口超限（%d秒内下降 > %.1f%%）", r.WindowSeconds, r.Limit)
			}
		}
	}
	if stop > 0 {
		// Only clear the metric stream(s) that actually triggered, so a quota
		// trigger never wipes token history (and vice versa).
		if triggeredToken {
			delete(tokenEvents, authID)
		}
		if triggeredQuota {
			delete(quotaEvents, authID)
		}
	}
	return stop, reason
}

// quotaDropExceeds reports whether the account consumed more than limit
// percentage points within the window. It sums consecutive decreases and
// re-anchors whenever remaining percent rises (a quota reset), so a window
// reset or a transient zero observation cannot inflate the measured drop.
func quotaDropExceeds(evs []quotaEvent, cutoff time.Time, limit float64) bool {
	var drop, prev float64
	havePrev := false
	for _, e := range evs {
		if !e.At.After(cutoff) {
			continue
		}
		// A non-positive remaining percent is treated as "no evidence" (the
		// host may report a pseudo zero before the first real snapshot). It
		// re-anchors the measurement instead of counting as an instant 100%
		// drop, so light usage does not falsely trip the rule.
		if e.RemainingPercent <= 0 {
			drop = 0
			havePrev = false
			continue
		}
		if !havePrev {
			prev = e.RemainingPercent
			havePrev = true
			continue
		}
		if e.RemainingPercent < prev {
			drop += prev - e.RemainingPercent
		} else {
			drop = 0
		}
		prev = e.RemainingPercent
		if drop > limit {
			return true
		}
	}
	return false
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
