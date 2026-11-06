package main

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

// schedulerService implements pluginapi.Scheduler.
type schedulerService struct{}

// Verified metadata keys (sdk/cliproxy/executor):
//
//	CanonicalSessionIDMetadataKey = "canonical_session_id"
//	ParentSessionIDMetadataKey    = "parent_session_id"
const (
	metadataCanonicalSessionID = "canonical_session_id"
	metadataParentSessionID    = "parent_session_id"
)

// handleSchedulerPick decodes and routes a scheduler.pick RPC.
func handleSchedulerPick(raw []byte) ([]byte, error) {
	var req pluginapi.SchedulerPickRequest
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &req); err != nil {
			return nil, err
		}
	}
	resp, err := schedulerService{}.Pick(context.Background(), req)
	if err != nil {
		return nil, err
	}
	return okEnvelope(resp), nil
}

// Pick implements the full scheduling flow: affinity → filter → tier → pressure
// × weight → concurrency cap → fallback.
func (schedulerService) Pick(ctx context.Context, req pluginapi.SchedulerPickRequest) (pluginapi.SchedulerPickResponse, error) {
	cfg := activeConfig()
	now := time.Now()
	sessionID := sessionIDFromRequest(req)
	selectionID := metadataString(req.Options.Metadata, "selection_id")

	// 1. Session affinity (path A: self-maintained sticky binding).
	if cfg.SessionAffinityEnabled && sessionID != "" {
		if authID, ok := affinityLookup(req.Provider, sessionID, now); ok {
			if c := findCandidate(req.Candidates, authID); c != nil && selectable(*c, now, cfg) {
				acquireSlot(authID, selectionID)
				affinityBind(req.Provider, sessionID, authID, now)
				return pluginapi.SchedulerPickResponse{AuthID: authID, Handled: true}, nil
			}
			affinityForget(req.Provider, sessionID)
		}
	}

	// 2-5. Filter + tier + sort + concurrency.
	pool := buildPool(req, now, cfg)
	if len(pool) == 0 {
		if cfg.FallbackReject {
			return pluginapi.SchedulerPickResponse{
				Handled:      true,
				Reject:       true,
				RejectCode:   "auth_unavailable",
				RejectReason: "all codex accounts unavailable",
			}, nil
		}
		return pluginapi.SchedulerPickResponse{Handled: false}, nil
	}

	chosen := pool[0]
	acquireSlot(chosen.auth.ID, selectionID)
	if sessionID != "" {
		affinityBind(req.Provider, sessionID, chosen.auth.ID, now)
	}
	logf("info", "scheduler.pick model=%s -> auth=%s (score=%.3f, %d candidates)", req.Model, chosen.auth.ID, chosen.score, len(pool))
	return pluginapi.SchedulerPickResponse{AuthID: chosen.auth.ID, Handled: true}, nil
}

// sessionIDFromRequest reads the canonical session id the host resolved before
// pick (Options.Metadata), falling back to well-known session headers.
func sessionIDFromRequest(req pluginapi.SchedulerPickRequest) string {
	if m := req.Options.Metadata; m != nil {
		if v, ok := m[metadataCanonicalSessionID].(string); ok && v != "" {
			return v
		}
		if v, ok := m[metadataParentSessionID].(string); ok && v != "" {
			return v
		}
	}
	for _, k := range []string{"Session-Id", "Session_id", "X-Session-ID", "X-Session-Affinity"} {
		if vals := req.Options.Headers[k]; len(vals) > 0 && vals[0] != "" {
			return vals[0]
		}
	}
	return ""
}

// selectable reports whether a candidate may currently be scheduled.
func selectable(c pluginapi.SchedulerAuthCandidate, now time.Time, cfg Config) bool {
	if !effectiveEnabled(cfg, c.ID) {
		return false
	}
	st := stateFor(c.ID)
	if st == nil {
		return true
	}
	if st.Disabled {
		return false
	}
	if now.Before(st.CooldownUntil) {
		return false
	}
	if now.Before(st.CircuitOpenUntil) {
		return false
	}
	if now.Before(st.OverloadUntil) {
		return false
	}
	if inFlightCount(c.ID) >= effectiveMaxParallel(cfg, c.ID) {
		return false
	}
	if !st.Quota.ObservedAt.IsZero() && st.Quota.RemainingPercent <= cfg.QuotaHardFloorPercent {
		return false
	}
	return true
}

type poolItem struct {
	auth     pluginapi.SchedulerAuthCandidate
	quota    QuotaSnapshot
	hasQuota bool
	role     string
	weight   float64
}

type candidate struct {
	auth  pluginapi.SchedulerAuthCandidate
	score float64
}

// buildPool filters candidates, splits primary/backup (hard tiering), and
// orders within the tier by pressure × weight.
func buildPool(req pluginapi.SchedulerPickRequest, now time.Time, cfg Config) []candidate {
	var primary, backup []poolItem
	for _, c := range req.Candidates {
		if c.Provider != "" && c.Provider != cfg.Provider {
			continue
		}
		if !selectable(c, now, cfg) {
			continue
		}
		it := poolItem{
			auth:   c,
			role:   effectiveRole(cfg, c.ID),
			weight: effectiveWeight(cfg, c.ID),
		}
		if st := stateFor(c.ID); st != nil {
			it.quota = st.Quota
			it.hasQuota = !st.Quota.ObservedAt.IsZero()
		}
		if !it.hasQuota {
			if q, ok := quotaFromMetadata(c.Metadata); ok {
				it.quota = q
				it.hasQuota = true
			}
		}
		if it.role == "backup" {
			backup = append(backup, it)
		} else {
			primary = append(primary, it)
		}
	}

	pool := primary
	if len(pool) == 0 {
		pool = backup
	}

	out := make([]candidate, 0, len(pool))
	for _, it := range pool {
		out = append(out, candidate{
			auth:  it.auth,
			score: pressure(cfg, it.quota, it.hasQuota, it.auth.ID, now) * it.weight,
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].score != out[j].score {
			return out[i].score > out[j].score
		}
		si, sj := stateFor(out[i].auth.ID), stateFor(out[j].auth.ID)
		if si != nil && sj != nil {
			if !si.Quota.ResetAt.Equal(sj.Quota.ResetAt) {
				return si.Quota.ResetAt.Before(sj.Quota.ResetAt)
			}
			if !si.LastUsed.Equal(sj.LastUsed) {
				return si.LastUsed.Before(sj.LastUsed)
			}
		}
		return out[i].auth.ID < out[j].auth.ID
	})
	return out
}

// pressure computes the ordering key for one account under the strategy, then
// applies the subscription-expiry boost (use-it-or-lose-it).
func pressure(cfg Config, q QuotaSnapshot, hasQuota bool, authID string, now time.Time) float64 {
	var base float64
	switch cfg.Strategy {
	case "expiry":
		if hasQuota && !q.ResetAt.IsZero() {
			base = -q.ResetAt.Sub(now).Minutes()
		}
	case "quota":
		if hasQuota {
			base = q.RemainingPercent
		}
	default: // quota_pressure
		if hasQuota && !q.ResetAt.IsZero() {
			hours := q.ResetAt.Sub(now).Hours()
			if hours < 0.5 {
				hours = 0.5
			}
			base = q.RemainingPercent / hours
		} else if hasQuota {
			base = q.RemainingPercent
		}
	}

	if cfg.ExpiryPriorityEnabled {
		if exp := effectiveExpiresAt(cfg, authID); !exp.IsZero() {
			until := exp.Sub(now).Hours()
			if until <= cfg.ExpiryPriorityHorizonHours && until > 0 {
				factor := 1.0 + (cfg.ExpiryPriorityHorizonHours-until)/cfg.ExpiryPriorityHorizonHours
				base *= factor
			}
		}
	}
	return base
}

func findCandidate(candidates []pluginapi.SchedulerAuthCandidate, authID string) *pluginapi.SchedulerAuthCandidate {
	for i := range candidates {
		if candidates[i].ID == authID {
			return &candidates[i]
		}
	}
	return nil
}

// quotaFromMetadata reads a normalized quota snapshot from the candidate's
// host-managed metadata (best-effort; keys vary by host version).
func quotaFromMetadata(md map[string]any) (QuotaSnapshot, bool) {
	if md == nil {
		return QuotaSnapshot{}, false
	}
	q := QuotaSnapshot{}
	found := false
	for _, k := range []string{"remaining_percent", "remaining", "quota_remaining", "usage_remaining"} {
		if v, ok := numberFromAny(md[k]); ok {
			q.RemainingPercent = v
			found = true
			break
		}
	}
	for _, k := range []string{"reset_at", "quota_reset_at", "resets_at"} {
		if v, ok := md[k].(string); ok && v != "" {
			if t, err := time.Parse(time.RFC3339, v); err == nil {
				q.ResetAt = t
				found = true
				break
			}
		}
	}
	return q, found
}

func numberFromAny(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(n, 64)
		return f, err == nil
	}
	return 0, false
}
