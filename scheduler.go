package main

import (
	"context"
	"encoding/json"
	"fmt"
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
//	IsForkMetadataKey             = "is_fork"
//	IsCompactionMetadataKey       = "is_compaction"
//	NodeKindMetadataKey           = "node_kind"
//	ExecutionSessionMetadataKey   = "execution_session_id"
//	DerivedSessionIDMetadataKey   = "derived_session_id"
const (
	metadataCanonicalSessionID = "canonical_session_id"
	metadataParentSessionID    = "parent_session_id"
	metadataRequestID          = "request_id"
	metadataNodeKind           = "node_kind"
	metadataIsFork             = "is_fork"
	metadataIsCompaction       = "is_compaction"
	metadataExecutionSessionID = "execution_session_id"
	metadataDerivedSessionID   = "derived_session_id"
	metadataPinnedAuthID       = "pinned_auth_id"
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

// specifiedAuthID returns the pinned auth ID carried in scheduler metadata.
// The host sets "pinned_auth_id" when host.model.execute_stream is invoked with
// AuthID, which is exactly how degradation checks pin a specific credential
// (codex-guard self-eval and cpa-codex-candy-eval both do this).
func specifiedAuthID(req pluginapi.SchedulerPickRequest) string {
	return metadataString(req.Options.Metadata, metadataPinnedAuthID)
}

// Pick implements the full scheduling flow: affinity → filter → tier → pressure
// × weight → concurrency cap → fallback.
func (schedulerService) Pick(ctx context.Context, req pluginapi.SchedulerPickRequest) (pluginapi.SchedulerPickResponse, error) {
	cfg := activeConfig()
	now := time.Now()
	sessionID := sessionIDFromRequest(req)
	selectionID := metadataString(req.Options.Metadata, "selection_id")
	pinnedAuthID := specifiedAuthID(req)
	allowDegraded := pinnedAuthID != ""
	source := ""
	if allowDegraded {
		source = "degradation_test"
	}
	base := dispatchEntry{
		Provider:        req.Provider,
		Providers:       append([]string(nil), req.Providers...),
		Model:           req.Model,
		Stream:          req.Stream,
		SessionID:       sessionID,
		ParentSessionID: parentSessionIDFromRequest(req),
		RequestID:       requestIDFromRequest(req),
		SelectionID:     selectionID,
		Strategy:        cfg.Strategy,
		Candidates:      len(req.Candidates),
		Source:          source,
	}

	// Record every candidate with full attributes before any early return so
	// the history stays complete even on affinity hits / rejects / fallbacks.
	base.CandidateList = make([]dispatchCandidateDetail, 0, len(req.Candidates))
	for _, c := range req.Candidates {
		base.CandidateList = append(base.CandidateList, describeCandidate(c, now, cfg, allowDegraded))
	}

	// 1. Session affinity (path A: self-maintained sticky binding).
	if cfg.SessionAffinityEnabled && sessionID != "" {
		if authID, ok := affinityLookup(req.Provider, sessionID, now); ok {
			if c := findCandidate(req.Candidates, authID); c != nil && selectable(*c, now, cfg, allowDegraded) && tryAcquireSlot(authID, selectionID) {
				affinityBind(req.Provider, sessionID, authID, now)
				base.Result = "affinity"
				base.Reason = "会话亲和命中"
				base.Selected = authID
				base.PoolSize = 1
				base.Pool = []dispatchPoolItem{{AuthID: authID, Provider: c.Provider, Priority: c.Priority, Status: c.Status, Role: effectiveRole(cfg, authID), Weight: effectiveWeight(cfg, authID)}}
				recordDispatch(base)
				return pluginapi.SchedulerPickResponse{AuthID: authID, Handled: true}, nil
			}
			affinityForget(req.Provider, sessionID)
		}
	}

	// 2-5. Filter + tier + sort + concurrency.
	pool := buildPool(req, now, cfg, allowDegraded)
	base.PoolSize = len(pool)
	if len(pool) == 0 {
		if cfg.FallbackReject {
			base.Result = "rejected"
			base.Reason = emptyPoolReason(base.CandidateList)
			recordDispatch(base)
			return pluginapi.SchedulerPickResponse{
				Handled:      true,
				Reject:       true,
				RejectCode:   "auth_unavailable",
				RejectReason: "all codex accounts unavailable",
			}, nil
		}
		base.Result = "fallback"
		base.Reason = "无可用账号，交回内置调度"
		recordDispatch(base)
		return pluginapi.SchedulerPickResponse{Handled: false}, nil
	}

	chosen := -1
	for i := range pool {
		if tryAcquireSlot(pool[i].auth.ID, selectionID) {
			chosen = i
			break
		}
	}
	if chosen < 0 {
		if cfg.FallbackReject {
			base.Result = "rejected"
			base.Reason = "所有账号并发已满"
			recordDispatch(base)
			return pluginapi.SchedulerPickResponse{
				Handled:      true,
				Reject:       true,
				RejectCode:   "auth_unavailable",
				RejectReason: "all codex accounts at max parallel",
			}, nil
		}
		base.Result = "fallback"
		base.Reason = "无账号可占用并发槽，交回内置调度"
		recordDispatch(base)
		return pluginapi.SchedulerPickResponse{Handled: false}, nil
	}
	selected := pool[chosen]
	if sessionID != "" {
		affinityBind(req.Provider, sessionID, selected.auth.ID, now)
	}
	base.Result = "assigned"
	if pinnedAuthID != "" {
		base.Reason = "降智测试指定调用"
	}
	base.Selected = selected.auth.ID
	base.SelectedScore = selected.score
	base.Pool = make([]dispatchPoolItem, 0, len(pool))
	for _, it := range pool {
		base.Pool = append(base.Pool, dispatchPoolItem{
			AuthID:   it.auth.ID,
			Provider: it.auth.Provider,
			Priority: it.auth.Priority,
			Status:   it.auth.Status,
			Role:     effectiveRole(cfg, it.auth.ID),
			Weight:   effectiveWeight(cfg, it.auth.ID),
			Score:    it.score,
		})
	}
	recordDispatch(base)
	logf("info", "scheduler.pick model=%s -> auth=%s (score=%.3f, %d candidates)", req.Model, selected.auth.ID, selected.score, len(pool))
	return pluginapi.SchedulerPickResponse{AuthID: selected.auth.ID, Handled: true}, nil
}

// emptyPoolReason explains why the scheduling pool ended up empty. It returns a
// dedicated message when every candidate was excluded solely by the degraded
// tier, so rejections are not misreported as "unavailable".
func emptyPoolReason(candidates []dispatchCandidateDetail) string {
	if len(candidates) == 0 {
		return "所有账号不可用"
	}
	for _, c := range candidates {
		if c.SkipReason != "降智层不参与调度" {
			return "所有账号不可用"
		}
	}
	return "所有候选账号均处于降智层"
}

// sessionIDFromRequest reads the session identity the host resolved before pick.
// The host sets canonical_session_id for explicit sessions (and for execution /
// LCP sessions), but for requests without an explicit session it only derives a
// stable derived_session_id ("ctx:v1:..."). We fall back across all of them, then
// to well-known session headers, so the history stays populated as often as
// possible.
func sessionIDFromRequest(req pluginapi.SchedulerPickRequest) string {
	if m := req.Options.Metadata; m != nil {
		for _, key := range []string{
			metadataCanonicalSessionID,
			metadataParentSessionID,
			metadataDerivedSessionID,
			metadataExecutionSessionID,
		} {
			if v, ok := m[key].(string); ok && v != "" {
				return v
			}
		}
	}
	for _, k := range []string{"Session-Id", "Session_id", "X-Session-ID", "X-Session-Affinity"} {
		if vals := req.Options.Headers[k]; len(vals) > 0 && vals[0] != "" {
			return vals[0]
		}
	}
	return ""
}

// parentSessionIDFromRequest extracts only the parent session identity. It
// intentionally does not fall back to derived_session_id, because a derived ID
// is a session identity, not a parent.
func parentSessionIDFromRequest(req pluginapi.SchedulerPickRequest) string {
	return metadataString(req.Options.Metadata, metadataParentSessionID)
}

// requestIDFromRequest best-effort extracts a request identifier from metadata
// or request headers (the host does not guarantee a stable request_id key).
func requestIDFromRequest(req pluginapi.SchedulerPickRequest) string {
	for _, k := range []string{metadataRequestID, "request-id", "requestId", "request_id"} {
		if v := metadataString(req.Options.Metadata, k); v != "" {
			return v
		}
	}
	for _, k := range []string{"X-Request-ID", "X-Request-Id", "Request-Id", "Request-ID"} {
		if vals := req.Options.Headers[k]; len(vals) > 0 && vals[0] != "" {
			return vals[0]
		}
	}
	return ""
}

// describeCandidate builds one auditable candidate row with selectability and
// skip reasons, independent of the pool that buildPool eventually produced.
func describeCandidate(c pluginapi.SchedulerAuthCandidate, now time.Time, cfg Config, allowDegraded bool) dispatchCandidateDetail {
	d := dispatchCandidateDetail{
		AuthID:     c.ID,
		Provider:   c.Provider,
		Priority:   c.Priority,
		Status:     c.Status,
		Role:       effectiveRole(cfg, c.ID),
		Weight:     effectiveWeight(cfg, c.ID),
		Attributes: cloneStringMap(c.Attributes),
	}
	if c.Provider != "" && c.Provider != cfg.Provider {
		d.Selectable = false
		d.SkipReason = "提供商不匹配"
		return d
	}
	q, hasQuota := candidateQuota(c)
	d.Score = pressure(cfg, q, hasQuota, c.ID, now) * d.Weight
	d.Selectable = selectable(c, now, cfg, allowDegraded)
	if !d.Selectable {
		d.SkipReason = skipReasonForCandidate(c, now, cfg, allowDegraded)
	}
	return d
}

// candidateQuota returns the most recent quota snapshot for a candidate,
// preferring plugin-managed state then host-managed metadata.
func candidateQuota(c pluginapi.SchedulerAuthCandidate) (QuotaSnapshot, bool) {
	if st := stateFor(c.ID); st != nil && !st.Quota.ObservedAt.IsZero() {
		return st.Quota, true
	}
	return quotaFromMetadata(c.Metadata)
}

// skipReasonForCandidate explains why selectable returned false.
func skipReasonForCandidate(c pluginapi.SchedulerAuthCandidate, now time.Time, cfg Config, allowDegraded bool) string {
	if !effectiveEnabled(cfg, c.ID) {
		return "插件配置或运行状态禁用"
	}
	if accountTier(cfg, c.ID) == TierDegraded && !allowDegraded {
		return "降智层不参与调度"
	}
	st := stateFor(c.ID)
	if st == nil {
		return ""
	}
	if st.Disabled {
		return "插件状态禁用"
	}
	if now.Before(st.CooldownUntil) {
		return fmt.Sprintf("冷却中，直到 %s", fmtTime(st.CooldownUntil))
	}
	if now.Before(st.TransientPauseUntil) {
		return fmt.Sprintf("临时暂停中，直到 %s", fmtTime(st.TransientPauseUntil))
	}
	if now.Before(st.CircuitOpenUntil) {
		return fmt.Sprintf("熔断中，直到 %s", fmtTime(st.CircuitOpenUntil))
	}
	if now.Before(st.OverloadUntil) {
		return fmt.Sprintf("超负荷，直到 %s", fmtTime(st.OverloadUntil))
	}
	if inFlightCount(c.ID) >= effectiveMaxParallel(cfg, c.ID) {
		return fmt.Sprintf("达到最大并行 %d", effectiveMaxParallel(cfg, c.ID))
	}
	if !st.Quota.ObservedAt.IsZero() && st.Quota.RemainingPercent <= cfg.QuotaHardFloorPercent {
		return fmt.Sprintf("额度低于硬下限 %.1f%%", cfg.QuotaHardFloorPercent)
	}
	return ""
}

func cloneStringMap(src map[string]string) map[string]string {
	if len(src) == 0 {
		return nil
	}
	out := make(map[string]string, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

// selectable reports whether a candidate may currently be scheduled.
func selectable(c pluginapi.SchedulerAuthCandidate, now time.Time, cfg Config, allowDegraded bool) bool {
	if !effectiveEnabled(cfg, c.ID) {
		return false
	}
	if accountTier(cfg, c.ID) == TierDegraded && !allowDegraded {
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
	if now.Before(st.TransientPauseUntil) {
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
	tier     string
	weight   float64
}

type candidate struct {
	auth  pluginapi.SchedulerAuthCandidate
	score float64
}

// buildPool filters candidates, splits primary/backup (hard tiering), and
// orders within the tier by pressure × weight.
func buildPool(req pluginapi.SchedulerPickRequest, now time.Time, cfg Config, allowDegraded bool) []candidate {
	var primary, backup []poolItem
	for _, c := range req.Candidates {
		if c.Provider != "" && c.Provider != cfg.Provider {
			continue
		}
		if !selectable(c, now, cfg, allowDegraded) {
			continue
		}
		it := poolItem{
			auth:   c,
			tier:   accountTier(cfg, c.ID),
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
		if it.tier == TierBackup {
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
