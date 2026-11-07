package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
	"gopkg.in/yaml.v3"
)

const (
	// managementBasePath is the full URL prefix for authenticated management requests.
	managementBasePath = "/v0/management/codex-guard"
	// resourceBasePath is the full URL prefix for browser-navigable resources.
	resourceBasePath = "/v0/resource/plugins/codex-guard"
	// managementRoutePath is the route path registered relative to /v0/management.
	managementRoutePath = "/codex-guard"
)

// RegisterManagement declares the plugin-owned management API and resource routes.
func RegisterManagement() pluginapi.ManagementRegistrationResponse {
	return pluginapi.ManagementRegistrationResponse{
		Resources: []pluginapi.ResourceRoute{
			{Path: "/status", Menu: "Codex Guard", Description: "Codex account scheduling console."},
		},
		Routes: []pluginapi.ManagementRoute{
			{Method: http.MethodGet, Path: managementRoutePath + "/settings", Description: "Read plugin settings."},
			{Method: http.MethodPut, Path: managementRoutePath + "/settings", Description: "Update plugin settings."},
			{Method: http.MethodGet, Path: managementRoutePath + "/status", Description: "Account runtime status."},
			{Method: http.MethodGet, Path: managementRoutePath + "/models", Description: "Available codex models for degradation checks."},
			{Method: http.MethodPost, Path: managementRoutePath + "/account", Description: "Per-account action."},
			{Method: http.MethodPost, Path: managementRoutePath + "/refresh", Description: "Manually refresh all account quotas."},
			{Method: http.MethodPost, Path: managementRoutePath + "/eval/run", Description: "Manually run degradation checks for all accounts."},
			{Method: http.MethodGet, Path: managementRoutePath + "/eval/history", Description: "Read degradation check history."},
			{Method: http.MethodGet, Path: managementRoutePath + "/eval/history/export", Description: "Export degradation check history as CSV."},
			{Method: http.MethodPost, Path: managementRoutePath + "/eval/history/clear", Description: "Clear degradation check history."},
			{Method: http.MethodGet, Path: managementRoutePath + "/history", Description: "Read dispatch history."},
			{Method: http.MethodGet, Path: managementRoutePath + "/history/export", Description: "Export dispatch history as CSV."},
			{Method: http.MethodPost, Path: managementRoutePath + "/history/clear", Description: "Clear dispatch history."},
		},
	}
}

// managementHandler dispatches management requests by method + path.
type managementHandler struct{}

func (managementHandler) HandleManagement(ctx context.Context, req pluginapi.ManagementRequest) (pluginapi.ManagementResponse, error) {
	switch {
	case req.Method == http.MethodGet && req.Path == managementBasePath+"/settings":
		if strings.EqualFold(req.Query.Get("format"), "yaml") {
			return yamlManagementResponse(http.StatusOK, activeConfig()), nil
		}
		return jsonManagementResponse(http.StatusOK, activeConfig()), nil
	case req.Method == http.MethodPut && req.Path == managementBasePath+"/settings":
		return handlePutSettings(req)
	case req.Method == http.MethodGet && req.Path == managementBasePath+"/status":
		return jsonManagementResponse(http.StatusOK, statusPayload()), nil
	case req.Method == http.MethodGet && req.Path == managementBasePath+"/models":
		return handleEvalModels(), nil
	case req.Method == http.MethodPost && req.Path == managementBasePath+"/account":
		return handleAccountAction(req)
	case req.Method == http.MethodPost && req.Path == managementBasePath+"/refresh":
		return handleRefreshQuotas(), nil
	case req.Method == http.MethodPost && req.Path == managementBasePath+"/eval/run":
		return handleEvalRun(), nil
	case req.Method == http.MethodGet && req.Path == managementBasePath+"/eval/history":
		return handleEvalHistory(req), nil
	case req.Method == http.MethodGet && req.Path == managementBasePath+"/eval/history/export":
		return handleEvalHistoryExport(), nil
	case req.Method == http.MethodPost && req.Path == managementBasePath+"/eval/history/clear":
		clearEvalHistory()
		return jsonManagementResponse(http.StatusOK, map[string]bool{"ok": true}), nil
	case req.Method == http.MethodGet && req.Path == managementBasePath+"/history":
		return handleDispatchHistory(req), nil
	case req.Method == http.MethodGet && req.Path == managementBasePath+"/history/export":
		return handleDispatchHistoryExport(), nil
	case req.Method == http.MethodPost && req.Path == managementBasePath+"/history/clear":
		clearDispatchLog()
		return jsonManagementResponse(http.StatusOK, map[string]bool{"ok": true}), nil
	case req.Method == http.MethodGet && (req.Path == resourceBasePath || req.Path == resourceBasePath+"/status"):
		return htmlManagementResponse(managementHTML), nil
	default:
		return jsonManagementResponse(http.StatusNotFound, map[string]string{"error": "not found"}), nil
	}
}

func handleRefreshQuotas() pluginapi.ManagementResponse {
	if globalRefresher.triggerRefresh() {
		return jsonManagementResponse(http.StatusOK, map[string]any{"ok": true, "refreshing": true})
	}
	return jsonManagementResponse(http.StatusOK, map[string]any{"ok": true, "refreshing": false, "message": "额度刷新已在进行中"})
}

// handleEvalModels aggregates the models registered for every Codex credential
// via host.auth.models so the settings form can render a model picker for
// degradation checks.
func handleEvalModels() pluginapi.ManagementResponse {
	var resp pluginapi.HostAuthModelsResponse
	if err := callHostJSON(pluginabi.MethodHostAuthModels, pluginapi.HostAuthModelsRequest{}, &resp); err != nil {
		return jsonManagementResponse(http.StatusInternalServerError, map[string]string{"error": "获取可用模型失败: " + err.Error()})
	}
	seen := map[string]bool{}
	models := make([]string, 0)
	for _, cred := range resp.Credentials {
		if !strings.EqualFold(strings.TrimSpace(cred.Provider), "codex") {
			continue
		}
		for _, m := range cred.Models {
			m = strings.TrimSpace(m)
			if m == "" || seen[m] {
				continue
			}
			seen[m] = true
			models = append(models, m)
		}
	}
	sort.Strings(models)
	return jsonManagementResponse(http.StatusOK, map[string]any{"models": models})
}

func handleEvalRun() pluginapi.ManagementResponse {
	if triggerEvalRun() {
		return jsonManagementResponse(http.StatusOK, map[string]any{"ok": true, "running": true})
	}
	return jsonManagementResponse(http.StatusOK, map[string]any{"ok": true, "running": false, "message": "降智测试已在进行中"})
}

// handleEvalHistory returns the degradation-check history, newest first.
func handleEvalHistory(req pluginapi.ManagementRequest) pluginapi.ManagementResponse {
	// The history tab lists every Codex account and shows a per-account sliding
	// window of the five most recent checks, so the frontend needs the full
	// chronological history plus the account roster (including untested accounts).
	accounts := []accountStatusView{}
	if payload := statusPayload(); payload != nil {
		if a, ok := payload["accounts"].([]accountStatusView); ok {
			accounts = a
		}
	}
	return jsonManagementResponse(http.StatusOK, map[string]any{
		"entries":  evalHistoryAllChronological(),
		"accounts": accounts,
		"total":    evalHistoryCount(),
	})
}

// handleEvalHistoryExport streams the full degradation-check history as CSV,
// oldest first, one row per account check.
func handleEvalHistoryExport() pluginapi.ManagementResponse {
	entries := evalHistoryAllChronological()
	var buf strings.Builder
	w := csv.NewWriter(&buf)
	if err := w.Write([]string{
		"时间", "账号", "状态", "糖果题", "糖果回答", "指纹", "指纹样本", "预测", "检测到模型", "置信度", "家族", "糖果失败原因", "指纹失败原因", "指纹样本错误", "跳过原因", "调用模型", "思考强度", "检测器版本",
	}); err != nil {
		return jsonManagementResponse(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	for _, e := range entries {
		fpSamples := ""
		if b, err := json.Marshal(e.FingerprintSamples); err == nil {
			fpSamples = string(b)
		}
		if err := w.Write([]string{
			e.Timestamp, e.AuthID, e.Status, e.Candy, e.CandyAnswer, e.Fingerprint, fpSamples, e.Prediction,
			e.PredictionName, e.Probability, e.Family, e.CandyError, e.FingerprintError, e.FingerprintSamplesError, e.SkipReason, e.Model, e.Effort, e.DetectorRev,
		}); err != nil {
			return jsonManagementResponse(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return jsonManagementResponse(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return pluginapi.ManagementResponse{
		StatusCode: http.StatusOK,
		Headers: http.Header{
			"Content-Type":        []string{"text/csv; charset=utf-8"},
			"Content-Disposition": []string{`attachment; filename="codex-guard-eval-history.csv"`},
		},
		Body: []byte(buf.String()),
	}
}

func handleDispatchHistory(req pluginapi.ManagementRequest) pluginapi.ManagementResponse {
	limit := 0
	if v := req.Query.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			limit = n
		}
	}
	return jsonManagementResponse(http.StatusOK, map[string]any{
		"entries": dispatchLogSnapshot(limit),
		"total":   dispatchLogCount(),
	})
}

// handleDispatchHistoryExport streams the full dispatch history as CSV,
// oldest first, with one row per scheduling decision.
func handleDispatchHistoryExport() pluginapi.ManagementResponse {
	entries := dispatchLogAllChronological()
	var buf strings.Builder
	w := csv.NewWriter(&buf)
	if err := w.Write([]string{
		"时间", "结果", "来源", "提供商", "提供商列表", "模型", "流式", "会话ID", "父会话ID", "请求ID", "选择ID",
		"策略", "候选数", "池大小", "选中账号", "选中分数", "套餐", "订阅续期", "Credit余额", "主动重置", "原因", "候选明细",
	}); err != nil {
		return jsonManagementResponse(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	for _, e := range entries {
		providers := strings.Join(e.Providers, "|")
		stream := "否"
		if e.Stream {
			stream = "是"
		}
		score := ""
		if e.SelectedScore != 0 || e.Selected != "" {
			score = strconv.FormatFloat(e.SelectedScore, 'f', 4, 64)
		}
		candidates := formatCandidateDetailsCSV(e.CandidateList)
		resetCredits := ""
		if e.ResetCreditsAvailable != nil {
			resetCredits = strconv.Itoa(*e.ResetCreditsAvailable)
		}
		planName := e.PlanLabel
		if planName == "" {
			planName = e.PlanType
		}
		if err := w.Write([]string{
			e.Timestamp, e.Result, dispatchSourceLabel(e.Source), e.Provider, providers, e.Model, stream,
			e.SessionID, e.ParentSessionID, e.RequestID, e.SelectionID,
			e.Strategy, strconv.Itoa(e.Candidates), strconv.Itoa(e.PoolSize),
			e.Selected, score, planName, e.SubscriptionActiveUntil, e.CreditBalance, resetCredits,
			e.Reason, candidates,
		}); err != nil {
			return jsonManagementResponse(http.StatusInternalServerError, map[string]string{"error": err.Error()})
		}
	}
	w.Flush()
	if err := w.Error(); err != nil {
		return jsonManagementResponse(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return pluginapi.ManagementResponse{
		StatusCode: http.StatusOK,
		Headers: http.Header{
			"Content-Type":        []string{"text/csv; charset=utf-8"},
			"Content-Disposition": []string{`attachment; filename="codex-guard-history.csv"`},
		},
		Body: []byte(buf.String()),
	}
}

// dispatchSourceLabel renders the dispatch source for CSV/UI display.
func dispatchSourceLabel(source string) string {
	switch source {
	case "degradation_test":
		return "降智测试指定调用"
	case "":
		return "正常调度"
	default:
		return source
	}
}

func formatCandidateDetailsCSV(list []dispatchCandidateDetail) string {
	if len(list) == 0 {
		return ""
	}
	parts := make([]string, 0, len(list))
	for _, c := range list {
		selectable := "可用"
		if !c.Selectable {
			selectable = "跳过"
			if c.SkipReason != "" {
				selectable += ":" + c.SkipReason
			}
		}
		score := strconv.FormatFloat(c.Score, 'f', 4, 64)
		parts = append(parts, c.AuthID+"|"+c.Role+"|"+score+"|"+selectable)
	}
	return strings.Join(parts, "; ")
}

func handleManagementHandle(raw []byte) ([]byte, error) {
	var req pluginapi.ManagementRequest
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &req); err != nil {
			return nil, err
		}
	}
	resp, err := managementHandler{}.HandleManagement(context.Background(), req)
	if err != nil {
		return nil, err
	}
	return okEnvelope(resp), nil
}

func jsonManagementResponse(status int, body any) pluginapi.ManagementResponse {
	b, _ := json.Marshal(body)
	return pluginapi.ManagementResponse{
		StatusCode: status,
		Headers:    http.Header{"Content-Type": []string{"application/json"}},
		Body:       b,
	}
}

func yamlManagementResponse(status int, body any) pluginapi.ManagementResponse {
	b, _ := yaml.Marshal(body)
	return pluginapi.ManagementResponse{
		StatusCode: status,
		Headers:    http.Header{"Content-Type": []string{"text/yaml; charset=utf-8"}},
		Body:       b,
	}
}

func htmlManagementResponse(html string) pluginapi.ManagementResponse {
	return pluginapi.ManagementResponse{
		StatusCode: http.StatusOK,
		Headers:    http.Header{"Content-Type": []string{"text/html; charset=utf-8"}},
		Body:       []byte(html),
	}
}

func handlePutSettings(req pluginapi.ManagementRequest) (pluginapi.ManagementResponse, error) {
	if len(req.Body) == 0 {
		return jsonManagementResponse(http.StatusBadRequest, map[string]string{"error": "body required"}), nil
	}
	// decodeConfig uses YAML, which is a superset of JSON, so both are accepted.
	cfg, err := decodeConfig(req.Body)
	if err != nil {
		return jsonManagementResponse(http.StatusBadRequest, map[string]string{"error": err.Error()}), nil
	}
	cfg = applyConfig(cfg)
	return jsonManagementResponse(http.StatusOK, cfg), nil
}

func handleAccountAction(req pluginapi.ManagementRequest) (pluginapi.ManagementResponse, error) {
	var action struct {
		ID          string   `json:"id"`
		Action      string   `json:"action"`
		Enabled     *bool    `json:"enabled"`
		Role        string   `json:"role"`
		Weight      *float64 `json:"weight"`
		MaxParallel *int     `json:"max_parallel"`
	}
	if err := json.Unmarshal(req.Body, &action); err != nil || action.ID == "" {
		return jsonManagementResponse(http.StatusBadRequest, map[string]string{"error": "id and action required"}), nil
	}
	switch action.Action {
	case "enable":
		reEnableAccount(action.ID)
	case "disable":
		disableAccount(action.ID)
	case "reset_cooldown":
		resetCooldown(action.ID)
	case "reset_overload":
		resetOverload(action.ID)
	case "reset_circuit":
		resetCircuit(action.ID)
	case "reset_eval_tier":
		resetEvalTier(action.ID)
	case "eval_test":
		running, err := triggerEvalRunFor(action.ID)
		if err != nil {
			return jsonManagementResponse(http.StatusBadRequest, map[string]string{"error": err.Error()}), nil
		}
		if !running {
			return jsonManagementResponse(http.StatusOK, map[string]any{"ok": true, "running": false, "message": "降智测试已在进行中"}), nil
		}
		return jsonManagementResponse(http.StatusOK, map[string]any{"ok": true, "running": true}), nil
	case "redeem_reset_credit":
		msg, err := redeemResetCredit(action.ID)
		if err != nil {
			return jsonManagementResponse(http.StatusInternalServerError, map[string]string{"error": err.Error()}), nil
		}
		return jsonManagementResponse(http.StatusOK, map[string]any{"ok": true, "message": msg}), nil
	case "set_cpa":
		if action.Enabled == nil {
			return jsonManagementResponse(http.StatusBadRequest, map[string]string{"error": "enabled is required"}), nil
		}
		if err := setHostAuthDisabled(action.ID, !*action.Enabled); err != nil {
			return jsonManagementResponse(http.StatusInternalServerError, map[string]string{"error": err.Error()}), nil
		}
	case "update_account":
		if action.Role == "" && action.Weight == nil && action.MaxParallel == nil && action.Enabled == nil {
			return jsonManagementResponse(http.StatusBadRequest, map[string]string{"error": "nothing to update"}), nil
		}
		cfg := upsertAccountOverride(action.ID, AccountConfig{
			ID:          action.ID,
			Role:        action.Role,
			Weight:      action.Weight,
			MaxParallel: action.MaxParallel,
			Enabled:     action.Enabled,
		})
		if err := persistConfig(cfg); err != nil {
			logf("warn", "persist settings failed: %v", err)
		}
		if action.Role != "" {
			resetEvalTier(action.ID)
		}
		return jsonManagementResponse(http.StatusOK, map[string]any{"ok": true, "config": cfg}), nil
	default:
		return jsonManagementResponse(http.StatusBadRequest, map[string]string{"error": "unknown action"}), nil
	}
	return jsonManagementResponse(http.StatusOK, map[string]bool{"ok": true}), nil
}

type accountStatusView struct {
	AuthID                      string            `json:"auth_id"`
	Email                       string            `json:"email,omitempty"`
	Role                        string            `json:"role"`
	Tier                        string            `json:"tier,omitempty"`
	Weight                      float64           `json:"weight"`
	Enabled                     bool              `json:"enabled"`
	MaxParallel                 int               `json:"max_parallel"`
	InFlight                    int               `json:"in_flight"`
	RemainingPercent            float64           `json:"remaining_percent"`
	UsedPercent                 float64           `json:"used_percent"`
	ResetAt                     string            `json:"reset_at,omitempty"`
	SubscriptionExpiresAt       string            `json:"subscription_expires_at,omitempty"`
	SubscriptionActiveUntil     string            `json:"subscription_active_until,omitempty"`
	SubscriptionActiveStart     string            `json:"subscription_active_start,omitempty"`
	CreditBalance               string            `json:"credit_balance,omitempty"`
	CreditHasCredits            bool              `json:"credit_has_credits,omitempty"`
	CreditUnlimited             bool              `json:"credit_unlimited,omitempty"`
	CreditOverageLimitReached   bool              `json:"credit_overage_limit_reached,omitempty"`
	ResetCreditsAvailable       *int              `json:"reset_credits_available,omitempty"`
	ResetCreditsApplicable      *int              `json:"reset_credits_applicable,omitempty"`
	ResetCredits                []resetCreditView `json:"reset_credits,omitempty"`
	PlanType                    string            `json:"plan_type,omitempty"`
	PlanLabel                   string            `json:"plan_label,omitempty"`
	QuotaWindow                 int64             `json:"quota_window_seconds,omitempty"`
	QuotaWindowLabel            string            `json:"quota_window_label,omitempty"`
	QuotaAllowed                bool              `json:"quota_allowed,omitempty"`
	LimitReached                bool              `json:"limit_reached,omitempty"`
	LimitReachedType            string            `json:"limit_reached_type,omitempty"`
	QuotaObservedAt             string            `json:"quota_observed_at,omitempty"`
	CooldownUntil               string            `json:"cooldown_until,omitempty"`
	CircuitOpenUntil            string            `json:"circuit_open_until,omitempty"`
	OverloadUntil               string            `json:"overload_until,omitempty"`
	Disabled                    bool              `json:"disabled"`
	HostDisabled                bool              `json:"host_disabled,omitempty"`
	HostStatus                  string            `json:"host_status,omitempty"`
	StatusMessage               string            `json:"status_message,omitempty"`
	Unavailable                 bool              `json:"unavailable,omitempty"`
	QuotaWindows                []quotaWindowView `json:"quota_windows,omitempty"`
	EvalStatus                  string            `json:"eval_status,omitempty"`
	EvalLabel                   string            `json:"eval_label,omitempty"`
	EvalCandy                   string            `json:"eval_candy,omitempty"`
	EvalFingerprint             string            `json:"eval_fingerprint,omitempty"`
	EvalPrediction              string            `json:"eval_prediction,omitempty"`
	EvalPredictionName          string            `json:"eval_prediction_name,omitempty"`
	EvalProbability             string            `json:"eval_probability,omitempty"`
	EvalFamily                  string            `json:"eval_family,omitempty"`
	EvalCandyError              string            `json:"eval_candy_error,omitempty"`
	EvalFingerprintError        string            `json:"eval_fingerprint_error,omitempty"`
	EvalFingerprintSamplesError string            `json:"eval_fingerprint_samples_error,omitempty"`
	EvalLastRunAt               string            `json:"eval_last_run_at,omitempty"`
	EvalSkipReason              string            `json:"eval_skip_reason,omitempty"`
	EvalConsecutiveNormal       int               `json:"eval_consecutive_normal,omitempty"`
	EvalRunning                 bool              `json:"eval_running,omitempty"`
	EvalStage                   string            `json:"eval_stage,omitempty"`
	EvalStartedAt               string            `json:"eval_started_at,omitempty"`
	Protections                 []protectionView  `json:"protections,omitempty"`
}

// protectionView describes one account protection for the status card hover UI.
type protectionView struct {
	Kind      string `json:"kind"`  // cooldown | transient | circuit | overload
	Label     string `json:"label"` // 冷却 | 熔断 | 超负荷
	Active    bool   `json:"active"`
	Reason    string `json:"reason,omitempty"`
	StartedAt string `json:"started_at,omitempty"`
	Duration  string `json:"duration,omitempty"`
	Until     string `json:"until,omitempty"`
}

type quotaWindowView struct {
	Name             string  `json:"name"`
	UsedPercent      float64 `json:"used_percent"`
	RemainingPercent float64 `json:"remaining_percent"`
	WindowSeconds    int64   `json:"window_seconds"`
	WindowLabel      string  `json:"window_label,omitempty"`
	ResetAt          string  `json:"reset_at,omitempty"`
}

type resetCreditView struct {
	ID        string `json:"id,omitempty"`
	Status    string `json:"status,omitempty"`
	Title     string `json:"title,omitempty"`
	ResetType string `json:"reset_type,omitempty"`
	GrantedAt string `json:"granted_at,omitempty"`
	ExpiresAt string `json:"expires_at,omitempty"`
}

func statusPayload() map[string]any {
	cfg := activeConfig()
	states := snapshotStates()
	ids := make(map[string]struct{}, len(states))
	for id := range states {
		ids[id] = struct{}{}
	}
	for _, a := range cfg.Accounts {
		if id := strings.TrimSpace(a.ID); id != "" {
			ids[id] = struct{}{}
		}
	}
	hostByID := make(map[string]rosterEntry)
	if entries, err := listHostAuths(); err == nil {
		for _, e := range entries {
			if !strings.EqualFold(strings.TrimSpace(e.Provider), "codex") {
				continue
			}
			if id := strings.TrimSpace(accountKey(e)); id != "" {
				ids[id] = struct{}{}
				hostByID[id] = e
			}
		}
	} else {
		logf("warn", "management.status host.auth.list failed: %v", err)
	}

	views := make([]accountStatusView, 0, len(ids))
	for id := range ids {
		v := accountStatusView{
			AuthID:      id,
			Role:        effectiveRole(cfg, id),
			Tier:        accountTier(cfg, id),
			Weight:      effectiveWeight(cfg, id),
			Enabled:     effectiveEnabled(cfg, id),
			MaxParallel: effectiveMaxParallel(cfg, id),
			InFlight:    inFlightCount(id),
		}
		if exp := effectiveExpiresAt(cfg, id); !exp.IsZero() {
			v.SubscriptionExpiresAt = fmtTime(exp)
		}
		if st := states[id]; st != nil {
			q := st.Quota
			v.RemainingPercent = q.RemainingPercent
			v.UsedPercent = q.UsedPercent
			v.ResetAt = fmtTime(q.ResetAt)
			v.PlanType = q.PlanType
			v.PlanLabel = planLabel(q.PlanType)
			v.QuotaWindow = q.WindowSeconds
			v.QuotaWindowLabel = windowLabel(q.WindowSeconds)
			v.QuotaAllowed = q.Allowed
			v.LimitReached = q.LimitReached
			v.LimitReachedType = q.LimitReachedType
			if !q.ObservedAt.IsZero() {
				v.QuotaObservedAt = fmtTime(q.ObservedAt)
			}
			v.CooldownUntil = fmtTime(st.CooldownUntil)
			v.CircuitOpenUntil = fmtTime(st.CircuitOpenUntil)
			v.OverloadUntil = fmtTime(st.OverloadUntil)
			v.Disabled = st.Disabled
			if !q.SubscriptionActiveUntil.IsZero() {
				v.SubscriptionActiveUntil = fmtTime(q.SubscriptionActiveUntil)
			}
			if !q.SubscriptionActiveStart.IsZero() {
				v.SubscriptionActiveStart = fmtTime(q.SubscriptionActiveStart)
			}
			v.CreditBalance = q.CreditBalance
			v.CreditHasCredits = q.CreditHasCredits
			v.CreditUnlimited = q.CreditUnlimited
			v.CreditOverageLimitReached = q.CreditOverageLimitReached
			if q.ResetCreditsAvailableCount != nil {
				c := *q.ResetCreditsAvailableCount
				v.ResetCreditsAvailable = &c
			}
			if q.ResetCreditsApplicableCount != nil {
				c := *q.ResetCreditsApplicableCount
				v.ResetCreditsApplicable = &c
			}
			for _, rc := range q.ResetCredits {
				v.ResetCredits = append(v.ResetCredits, resetCreditView{
					ID:        rc.ID,
					Status:    rc.Status,
					Title:     rc.Title,
					ResetType: rc.ResetType,
					GrantedAt: fmtTime(rc.GrantedAt),
					ExpiresAt: fmtTime(rc.ExpiresAt),
				})
			}
			for _, w := range q.Windows {
				v.QuotaWindows = append(v.QuotaWindows, quotaWindowView{
					Name:             w.Name,
					UsedPercent:      w.UsedPercent,
					RemainingPercent: w.RemainingPercent,
					WindowSeconds:    w.WindowSeconds,
					WindowLabel:      windowLabel(w.WindowSeconds),
					ResetAt:          fmtTime(w.ResetAt),
				})
			}
			v.Protections = buildProtections(st)
		}
		if len(v.Protections) == 0 {
			v.Protections = buildProtections(&AccountState{})
		}
		if p, ok := evalProgressFor(id); ok {
			v.EvalRunning = true
			v.EvalStage = p.Stage
			if !p.StartedAt.IsZero() {
				v.EvalStartedAt = fmtTime(p.StartedAt)
			}
		}
		if host, ok := hostByID[id]; ok {
			v.HostDisabled = host.Disabled
			v.HostStatus = host.Status
			v.StatusMessage = host.StatusMessage
			v.Unavailable = host.Unavailable
			v.Email = extractEmail(host.Email, host.Name, id)
		}
		if v.Email == "" {
			v.Email = extractEmail("", "", id)
		}
		if er, ok := evalResultFor(id); ok {
			v.EvalStatus = er.Status
			v.EvalLabel = evalLabel(er.Status)
			v.EvalCandy = er.Candy
			v.EvalFingerprint = er.Fingerprint
			v.EvalPrediction = er.Prediction
			v.EvalPredictionName = er.PredictionName
			v.EvalProbability = er.Probability
			v.EvalFamily = er.Family
			v.EvalCandyError = er.CandyError
			v.EvalFingerprintError = er.FingerprintError
			v.EvalFingerprintSamplesError = er.FingerprintSamplesError
			if !er.LastRunAt.IsZero() {
				v.EvalLastRunAt = fmtTime(er.LastRunAt)
			}
			v.EvalSkipReason = er.SkipReason
			v.EvalConsecutiveNormal = er.ConsecutiveNormal
		}
		views = append(views, v)
	}
	sort.Slice(views, func(i, j int) bool { return views[i].AuthID < views[j].AuthID })
	return map[string]any{"accounts": views, "config": cfg}
}

func fmtTime(t time.Time) string {
	if t.IsZero() {
		return ""
	}
	return t.Format(time.RFC3339)
}

// fmtDuration renders a protection duration as a compact Chinese string.
func fmtDuration(d time.Duration) string {
	if d <= 0 {
		return ""
	}
	secs := int(d.Seconds())
	if secs < 60 {
		return fmt.Sprintf("%d秒", secs)
	}
	days := secs / 86400
	hours := (secs % 86400) / 3600
	mins := (secs % 3600) / 60
	rem := secs % 60
	var b strings.Builder
	if days > 0 {
		b.WriteString(fmt.Sprintf("%d天", days))
	}
	if hours > 0 {
		b.WriteString(fmt.Sprintf("%d小时", hours))
	}
	if mins > 0 {
		b.WriteString(fmt.Sprintf("%d分", mins))
	}
	if rem > 0 {
		b.WriteString(fmt.Sprintf("%d秒", rem))
	}
	return b.String()
}

// buildProtections renders the timed protections shown on the status card.
func buildProtections(st *AccountState) []protectionView {
	now := time.Now()
	mk := func(kind, label string, until time.Time, meta protectionMeta) protectionView {
		v := protectionView{Kind: kind, Label: label}
		if !until.IsZero() && until.After(now) {
			v.Active = true
			v.Until = fmtTime(until)
		}
		if !meta.StartedAt.IsZero() {
			v.StartedAt = fmtTime(meta.StartedAt)
		}
		v.Reason = meta.Reason
		v.Duration = fmtDuration(meta.Duration)
		return v
	}
	return []protectionView{
		mk("cooldown", "冷却", st.CooldownUntil, st.CooldownMeta),
		mk("transient", "瞬时停用", st.TransientPauseUntil, st.TransientPauseMeta),
		mk("circuit", "熔断", st.CircuitOpenUntil, st.CircuitOpenMeta),
		mk("overload", "超负荷", st.OverloadUntil, st.OverloadMeta),
	}
}

// evalLabel maps a degradation verdict to a human-readable Chinese label.
func evalLabel(status string) string {
	switch status {
	case EvalStatusNormal:
		return "正常"
	case EvalStatusDegraded:
		return "降智"
	default:
		return "未知"
	}
}

// planLabel maps the upstream plan_type value to a short human-readable label.
func planLabel(plan string) string {
	switch strings.ToLower(strings.TrimSpace(plan)) {
	case "plus":
		return "Plus"
	case "pro":
		return "Pro"
	case "pro100", "pro_100", "prolite", "pro_lite":
		return "Pro100"
	case "pro200", "pro_200":
		return "Pro200"
	case "pro500", "pro_500":
		return "Pro500"
	case "free":
		return "Free"
	case "go":
		return "Go"
	case "team":
		return "Team"
	case "business":
		return "Business"
	case "self_serve_business_prolite":
		return "Business ProLite"
	case "self_serve_business_usage_based":
		return "Business Usage"
	case "enterprise":
		return "Enterprise"
	case "enterprise_cbp_usage_based", "ent26":
		return "Enterprise"
	case "edu":
		return "Edu"
	case "unlimited":
		return "Unlimited"
	default:
		if plan != "" {
			return plan
		}
		return ""
	}
}

var emailPattern = regexp.MustCompile(`[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}`)

// extractEmail returns the first email address found across the candidates.
// Host-provided metadata is preferred, falling back to the auth id / file name.
func extractEmail(candidates ...string) string {
	for _, c := range candidates {
		c = strings.TrimSpace(c)
		if c == "" {
			continue
		}
		if m := emailPattern.FindString(c); m != "" {
			return m
		}
	}
	return ""
}

// windowLabel renders a quota window length in the largest whole unit.
func windowLabel(seconds int64) string {
	if seconds <= 0 {
		return ""
	}
	switch {
	case seconds%86400 == 0:
		return fmt.Sprintf("%d 天", seconds/86400)
	case seconds%3600 == 0:
		return fmt.Sprintf("%d 小时", seconds/3600)
	case seconds%60 == 0:
		return fmt.Sprintf("%d 分钟", seconds/60)
	default:
		return fmt.Sprintf("%d 秒", seconds)
	}
}

const managementHTML = `<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Codex Guard 控制台</title>
<style>
:root{--bg:#f3f5f9;--card:#ffffff;--line:#e6e9f0;--text:#1f2430;--muted:#6b7280;--primary:#3b6ef6;--primary-dark:#2f5ad8;--ok:#16a34a;--warn:#d97706;--danger:#dc2626;--radius:14px;--shadow:0 1px 2px rgba(16,24,40,.05),0 4px 16px rgba(16,24,40,.06)}
*{box-sizing:border-box}
html,body{margin:0;padding:0}
body{background:var(--bg);color:var(--text);font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif;font-size:14px}
.topbar{position:sticky;top:0;z-index:20;display:flex;align-items:center;justify-content:space-between;gap:16px;padding:12px 24px;background:#10172a;color:#fff;box-shadow:0 2px 12px rgba(0,0,0,.18)}
.brand{display:flex;align-items:center;gap:12px}
.logo{width:40px;height:40px;border-radius:12px;background:linear-gradient(135deg,#3b6ef6,#7c3aed);display:flex;align-items:center;justify-content:center;font-size:20px}
.brand h1{margin:0;font-size:17px;font-weight:700;letter-spacing:.2px}
.brand .sub{margin:0;font-size:12px;color:#9aa5bd}
.auth-actions{display:flex;align-items:center;gap:10px}
.refresh-select{display:flex;align-items:center;gap:6px}
.refresh-select label{font-size:12px;color:#cbd5e1}
.refresh-select select{padding:6px 8px;border-radius:10px;border:1px solid rgba(255,255,255,.18);background:rgba(255,255,255,.08);color:#fff;font-size:13px;cursor:pointer}
.refresh-select select option{color:#1f2430}
.loginbar{display:flex;align-items:center;gap:8px}
.loginbar input{padding:8px 12px;border-radius:10px;border:1px solid rgba(255,255,255,.15);background:rgba(255,255,255,.08);color:#fff;width:200px}
.loginbar input::placeholder{color:#8b95ac}
.loginmsg{color:#fca5a5;font-size:12px}
.btn{display:inline-flex;align-items:center;justify-content:center;gap:6px;border:0;border-radius:10px;padding:8px 14px;font-size:13px;font-weight:600;cursor:pointer;transition:all .15s}
.btn-primary{background:var(--primary);color:#fff}
.btn-primary:hover{background:var(--primary-dark)}
.btn-ghost{background:transparent;color:#cbd5e1;border:1px solid rgba(255,255,255,.18)}
.btn-ghost:hover{background:rgba(255,255,255,.08)}
.btn-light{background:#fff;color:#1f2430;border:1px solid var(--line)}
.btn-light:hover{background:#f1f4f9}
.btn-danger{background:#fef2f2;color:var(--danger);border:1px solid #fecaca}
.btn-sm{padding:4px 10px;font-size:12px;border-radius:8px}
.tabs{display:flex;gap:6px;max-width:1080px;margin:18px auto 0;padding:0 20px}
.tab{flex:1;border:0;border-radius:12px 12px 0 0;padding:11px 16px;font-size:14px;font-weight:600;background:transparent;color:var(--muted);cursor:pointer}
.tab.active{background:var(--card);color:var(--primary);box-shadow:var(--shadow)}
main{max-width:1080px;margin:0 auto;padding:0 20px 40px}
.tab-panel{display:none}
.tab-panel.active{display:block}
.card{background:var(--card);border:1px solid var(--line);border-radius:var(--radius);box-shadow:var(--shadow);padding:18px 20px;margin-top:16px}
.card-title{display:flex;align-items:center;gap:8px;margin:0 0 14px;font-size:15px;font-weight:700}
.card-title .dot{width:8px;height:8px;border-radius:50%;background:var(--primary)}
.grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(230px,1fr));gap:14px 16px}
.field{display:flex;flex-direction:column;gap:6px}
.field-label{font-size:12px;color:var(--muted);font-weight:600}
.help{position:relative;display:inline-block;width:16px;height:16px;line-height:16px;text-align:center;border-radius:50%;background:#e2e8f0;color:#64748b;font-size:11px;font-weight:700;cursor:help;margin-left:6px;vertical-align:-2px;user-select:none}
.help:hover::after{content:attr(data-help);position:absolute;top:calc(100% + 7px);left:0;width:260px;max-width:80vw;padding:9px 11px;border-radius:10px;background:#111827;color:#fff;font-size:12px;font-weight:400;line-height:1.55;text-align:left;white-space:normal;z-index:80;box-shadow:0 8px 24px rgba(0,0,0,.28);pointer-events:none}
.help:hover::before{content:"";position:absolute;top:100%;left:7px;border:5px solid transparent;border-bottom-color:#111827;z-index:81;pointer-events:none}
.section-desc{font-size:12px;color:var(--muted);margin:-8px 0 14px;line-height:1.5}
.field input[type=number],.field input[type=text],.field select{width:100%;padding:8px 10px;border:1px solid var(--line);border-radius:10px;font-size:13px;background:#fff;color:var(--text)}
.field input:focus,.field select:focus,.textarea:focus{outline:none;border-color:var(--primary);box-shadow:0 0 0 3px rgba(59,110,246,.15)}
.checkbox-row{flex-direction:row;align-items:center;justify-content:space-between;border:1px solid var(--line);border-radius:10px;padding:8px 12px}
.checkbox-row input{width:18px;height:18px;accent-color:var(--primary)}
.textarea{width:100%;min-height:220px;padding:12px;border:1px solid var(--line);border-radius:10px;font-family:"SFMono-Regular",Consolas,"Liberation Mono",monospace;font-size:12.5px;line-height:1.5;resize:vertical;color:var(--text);background:#fff}
.toolbar{display:flex;align-items:center;gap:10px;margin:14px 0 10px;flex-wrap:wrap}
table{width:100%;border-collapse:collapse;background:#fff}
th,td{padding:10px 12px;text-align:left;border-bottom:1px solid var(--line);font-size:13px}
th{color:var(--muted);font-weight:600;font-size:12px;background:#fafbfd}
tr:hover td{background:#f8faff}
.badge{display:inline-block;padding:2px 8px;border-radius:999px;font-size:12px;font-weight:600}
.badge-ok{background:#ecfdf3;color:var(--ok)}
.badge-bad{background:#fef2f2;color:var(--danger)}
.badge-warn{background:#fff7ed;color:var(--warn)}
.badge-muted{background:#f1f3f7;color:var(--muted)}
.badge-rect{border-radius:6px}
.badge-click{border:0;cursor:pointer;font-family:inherit;line-height:inherit}
.badge-click:hover{filter:brightness(.96)}
.badge-primary{background:#e8f0ff;color:#2f5ad8}
.empty{padding:28px;text-align:center;color:var(--muted)}
.toast{position:fixed;right:20px;bottom:20px;z-index:50;max-width:360px;padding:12px 16px;border-radius:12px;background:#111827;color:#fff;font-size:13px;box-shadow:0 8px 24px rgba(0,0,0,.25);opacity:0;transform:translateY(8px);transition:all .2s}
.toast.show{opacity:1;transform:translateY(0)}
.toast.error{background:var(--danger)}
.toast.ok{background:var(--ok)}
.hint{font-size:12px;color:var(--muted);margin-top:4px}
.cell-input{width:88px;padding:5px 8px;border:1px solid var(--line);border-radius:8px;font-size:13px;background:#fff;color:var(--text)}
.cell-select{padding:5px 8px;border:1px solid var(--line);border-radius:8px;font-size:13px;background:#fff;color:var(--text)}
.cell-input:focus,.cell-select:focus{outline:none;border-color:var(--primary);box-shadow:0 0 0 3px rgba(59,110,246,.15)}
.status-list{display:grid;grid-template-columns:repeat(auto-fill,minmax(320px,1fr));gap:14px;align-items:stretch}
.acct-card{display:flex;flex-direction:column;border:1px solid var(--line);border-radius:12px;background:#fff;padding:14px 16px;box-shadow:0 1px 2px rgba(16,24,40,.04)}
.acct-card-disabled{background:#fef2f2;border-color:#fecaca;box-shadow:0 1px 2px rgba(220,38,38,.06)}
.acct-card-disabled .acct-name{color:var(--danger)}
.acct-card-disabled .acct-card-grid,.acct-card-disabled .acct-card-quota{background:#fff7f7;border-color:#fecaca}
.acct-card-top{display:flex;align-items:center;justify-content:space-between;gap:10px;flex-wrap:nowrap}
.acct-name{font-size:14px;font-weight:700;color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;min-width:0}
.acct-card-meta{display:flex;align-items:center;gap:8px;flex:none;white-space:nowrap}
.acct-card-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:10px 12px;margin-top:12px;padding:10px 12px;background:#fafbfd;border:1px solid var(--line);border-radius:10px}
.acct-card-grid .cell-input{width:100%;min-width:0}
.acct-card-grid .cell-select{width:100%;min-width:0}
.acct-cell{display:flex;flex-direction:column;gap:5px;min-width:0}
.acct-cell-label{font-size:11px;color:var(--muted);font-weight:600}
.acct-cell-value{font-size:13px;font-weight:600;line-height:28px}
.acct-card-quota{flex:1;margin-top:12px;padding:12px 14px;border:1px solid var(--line);border-radius:10px;background:#fdfefe}
.acct-card-footer{display:flex;align-items:center;justify-content:space-between;gap:12px;margin-top:12px;flex-wrap:wrap}
.acct-card-actions{display:flex;gap:6px;flex-wrap:wrap}

.rule-row{display:grid;grid-template-columns:1.7fr 1fr 1fr 1fr 1.2fr auto;gap:8px;align-items:end;padding:10px;border:1px solid var(--line);border-radius:10px;background:#fafbfd;margin-bottom:8px;min-width:760px}
.rule-row .field{margin:0}
.rule-row .field-label{font-size:11px}
.rule-head{display:grid;grid-template-columns:1.7fr 1fr 1fr 1fr 1.2fr auto;gap:8px;padding:0 10px;margin-bottom:2px;min-width:760px;color:var(--muted);font-size:11px;font-weight:600}
.rule-head span{text-align:left}
.acct-row{border:1px solid var(--line);border-radius:10px;padding:12px 14px;margin-bottom:10px;background:#fafbfd}
.acct-head{display:flex;align-items:center;justify-content:space-between;gap:10px;margin-bottom:10px}
.acct-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(210px,1fr));gap:10px 12px}
.acct-grid .field{margin:0}
.acct-grid .field-label{font-size:11px}
.acct-num{display:flex;align-items:center;gap:6px}
.acct-num .cell-check{width:16px;height:16px;accent-color:var(--primary);flex:none}
.acct-num .cell-input:disabled{background:#f1f3f7;color:#9aa2b1}
.acct-advanced{margin-top:12px;border-top:1px dashed var(--line);padding-top:8px}
.acct-advanced summary{cursor:pointer;font-size:12px;color:var(--muted);font-weight:600}
.quota-bar{position:relative;height:8px;border-radius:999px;background:#eef1f6;overflow:hidden;margin:6px 0 4px}
.quota-bar>span{display:block;height:100%;border-radius:999px;background:linear-gradient(90deg,#22c55e,#3b6ef6)}
.quota-line{font-size:12px;color:var(--muted);line-height:1.55}
.quota-head{display:flex;align-items:center;justify-content:space-between;gap:8px;flex-wrap:wrap;margin-bottom:2px}
.plan-sub{display:inline-flex;align-items:center;gap:4px;font-size:12px;color:var(--muted);white-space:nowrap}
.plan-sub-time{color:var(--text);font-variant-numeric:tabular-nums}
.plan-sub-dur{color:var(--warn);font-weight:600}
.plan-sub.muted{color:var(--muted)}
.billing-row{display:flex;align-items:center;gap:6px;flex-wrap:wrap;margin-top:2px}
.bill-chip{display:inline-flex;align-items:center;gap:5px;padding:3px 9px;border-radius:8px;background:#f7f9fc;border:1px solid var(--line);color:var(--muted);font-size:12px;line-height:1.4;white-space:nowrap}
.bill-chip b{color:var(--text);font-weight:700}
.bill-dur{color:var(--warn);font-weight:600}
.billing-row .btn{margin-left:auto}
.plan-pill{display:inline-flex;align-items:center;padding:2px 10px;border-radius:999px;font-size:12px;font-weight:700;letter-spacing:.2px;line-height:1.5;border:1px solid transparent;white-space:nowrap}
.plan-pill-unknown{background:#f1f3f7;color:#6b7280;border-color:#e5e7eb}
.plan-pill-free{background:#f3f4f6;color:#64748b;border-color:#e2e8f0}
.plan-pill-go{background:#e6f7fa;color:#0e7490;border-color:#bae6f0}
.plan-pill-plus{background:#e9f9ef;color:#15803d;border-color:#bbf7d0}
.plan-pill-team{background:#e8f0fe;color:#1d4ed8;border-color:#c7d8fe}
.plan-pill-business{background:#e6f5f3;color:#0f766e;border-color:#b6e5df}
.plan-pill-enterprise{background:#eef0fe;color:#4338ca;border-color:#d3d7fe}
.plan-pill-edu{background:#f0edfe;color:#6d28d9;border-color:#ddd0fb}
.plan-pill-pro{background:linear-gradient(135deg,#f2ebff,#e8ddff);color:#6d28d9;border-color:#ddd0fb}
.plan-pill-pro100{background:linear-gradient(135deg,#f3e8ff,#e9dcfe);color:#6d28d9;border-color:#c4b5fd;box-shadow:0 1px 2px rgba(109,40,217,.12)}
.plan-pill-pro200{background:linear-gradient(135deg,#fdeaf5,#fad9ed);color:#be185d;border-color:#f9a8d4;box-shadow:0 1px 3px rgba(190,24,93,.14)}
.plan-pill-pro500{background:linear-gradient(135deg,#fff8e0,#fdeeb8);color:#92400e;border-color:#fcd34d;box-shadow:0 1px 4px rgba(217,119,6,.22)}
.mini-actions{display:flex;gap:4px;flex-wrap:wrap}
.prot-chip{display:inline-flex;align-items:center;gap:4px;padding:3px 9px;border-radius:8px;font-size:12px;font-weight:600;border:1px solid var(--line);background:#f1f3f7;color:var(--muted);cursor:default;transition:all .15s}
.prot-chip.on{cursor:pointer;background:#fff7ed;color:var(--warn);border-color:#fed7aa;box-shadow:0 0 0 3px rgba(249,115,22,.12)}
.prot-chip.on:hover{background:#ffedd5;border-color:#fdba74}
.modal-overlay{position:fixed;inset:0;background:rgba(17,24,39,.45);display:flex;align-items:center;justify-content:center;z-index:120}
.modal{background:#fff;border-radius:14px;padding:20px 22px;min-width:320px;max-width:90vw;box-shadow:0 16px 48px rgba(0,0,0,.25)}
.modal-title{font-size:15px;font-weight:700;margin-bottom:10px}
.modal-text{font-size:13px;color:var(--muted);margin-bottom:18px;line-height:1.6}
.modal-actions{display:flex;justify-content:flex-end;gap:8px}
.toggle-on{background:#ecfdf3;color:var(--ok);border:1px solid #bbf7d0}
.toggle-off{background:#fef2f2;color:var(--danger);border:1px solid #fecaca}
.quota-window{margin-top:6px}
.quota-window+.quota-window{border-top:1px dashed var(--line);padding-top:6px}
.switch{position:relative;display:inline-block;width:40px;height:22px;vertical-align:middle;flex:none}
.switch input{opacity:0;width:0;height:0}
.switch .slider{position:absolute;cursor:pointer;top:0;left:0;right:0;bottom:0;background:#d1d5db;transition:.2s;border-radius:999px}
.switch .slider:before{content:"";position:absolute;height:16px;width:16px;left:3px;bottom:3px;background:#fff;transition:.2s;border-radius:50%;box-shadow:0 1px 2px rgba(0,0,0,.2)}
.switch input:checked+.slider{background:var(--ok)}
.switch input:checked+.slider:before{transform:translateX(18px)}
.switch input:disabled+.slider{opacity:.5}
.history-stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:10px;margin:14px 0}
.stat-card{background:#fafbfd;border:1px solid var(--line);border-radius:12px;padding:12px 14px}
.stat-card .stat-num{font-size:22px;font-weight:800;line-height:1.1}
.stat-card .stat-label{font-size:12px;color:var(--muted);margin-top:4px}
.hist-bar-row{display:flex;align-items:center;gap:10px;margin:6px 0}
.hist-bar-label{width:220px;font-size:12px;color:var(--text);flex:none;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.hist-bar{flex:1;height:16px;border-radius:8px;background:#eef1f6;overflow:hidden}
.hist-bar>span{display:block;height:100%;background:linear-gradient(90deg,#3b6ef6,#22c55e)}
.hist-bar-num{width:70px;font-size:12px;color:var(--muted);text-align:right;flex:none}
.pool-chips{display:flex;flex-wrap:wrap;gap:4px}
.pool-chip{display:inline-flex;align-items:center;gap:4px;padding:2px 8px;border-radius:6px;font-size:11px;font-weight:600;background:#f1f3f7;color:#374151}
.pool-chip.pool-top{background:#e8f0ff;color:#2f5ad8}
.pool-chip .sc{color:var(--muted);font-weight:400}
.res-badge{display:inline-block;padding:3px 9px;border-radius:6px;font-size:12px;font-weight:700}
.res-assigned{background:#ecfdf3;color:var(--ok)}
.res-affinity{background:#e0f2fe;color:#0369a1}
.res-rejected{background:#fef2f2;color:var(--danger)}
.res-fallback{background:#fff7ed;color:var(--warn)}
.eval-group{background:#fff;border:1px solid var(--line);border-radius:12px;margin:12px 0;overflow:hidden}
.eval-group-head{display:flex;align-items:center;justify-content:space-between;gap:10px;padding:10px 14px;background:#f7f9fc;border-bottom:1px solid var(--line)}
.eval-group-title{font-size:13px;font-weight:700;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.eval-group-count{flex:none;font-size:12px;color:var(--muted);background:#eef1f6;border-radius:999px;padding:2px 9px;font-weight:600}
.eval-entry{padding:10px 14px;border-bottom:1px solid #f0f2f7}
.eval-entry:last-child{border-bottom:0}
.eval-entry-head{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.eval-entry-time{font-size:12px;color:var(--muted)}
.eval-entry-body{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:6px 14px;margin-top:8px}
.eval-kv{font-size:12px;line-height:1.5}
.eval-kv b{color:var(--muted);font-weight:600;margin-right:4px}
.eval-window{display:flex;gap:6px;align-items:center;margin-top:8px}
.eval-slot{position:relative;width:34px;height:22px;border-radius:6px;border:1px solid var(--line);background:#f1f3f7;cursor:default}
.eval-slot.ok{background:#16a34a;border-color:#16a34a}
.eval-slot.bad{background:#dc2626;border-color:#dc2626}
.eval-slot.running{background:#facc15;border-color:#eab308}
.eval-slot.muted{background:#d7dbe3;border-color:#cfd4dd}
.eval-slot.has-entry{cursor:help}
.eval-tooltip{position:fixed;z-index:9999;max-width:520px;background:#1f2430;color:#eef1f6;border-radius:10px;padding:10px 12px;font-size:12px;line-height:1.55;box-shadow:0 8px 24px rgba(16,24,40,.35);pointer-events:none;white-space:pre-wrap;word-break:break-word}
.eval-tooltip b{color:#fff}
.eval-tooltip .tt-muted{color:#aab2c0}
.eval-tooltip .tt-div{margin:6px 0;border:0;border-top:1px solid #3a4252}
.eval-legend{grid-column:1/-1;font-size:12px;color:var(--muted);background:#f7f9fc;border:1px dashed var(--line);border-radius:10px;padding:8px 12px;line-height:1.7}
.eval-legend b{color:var(--text);font-weight:600}
</style>
</head>
<body>
<header class="topbar">
  <div class="brand">
    <div class="logo">🛡</div>
    <div>
      <h1>Codex Guard</h1>
      <p class="sub">Codex 账号调度控制台</p>
    </div>
  </div>
  <div class="auth-actions">
    <div class="refresh-select">
      <label for="autoRefresh">自动刷新</label>
      <select id="autoRefresh" onchange="setAutoRefresh()">
        <option value="0">不刷新</option>
        <option value="5">5 秒</option>
        <option value="10">10 秒</option>
        <option value="20">20 秒</option>
        <option value="30">30 秒</option>
      </select>
    </div>
    <div id="loginbar" class="loginbar" style="display:none">
      <input id="key" type="password" placeholder="CPA 管理密钥">
      <button class="btn btn-primary" onclick="login()">登录</button>
    </div>
    <button id="logoutBtn" class="btn btn-ghost" style="display:none" onclick="logout()">退出登录</button>
    <span id="loginmsg" class="loginmsg"></span>
  </div>
</header>
<nav class="tabs">
  <button class="tab active" data-tab="status" onclick="switchTab('status')">账号状态</button>
  <button class="tab" data-tab="settings" onclick="switchTab('settings')">参数设置</button>
  <button class="tab" data-tab="eval" onclick="switchTab('eval')">降智巡检</button>
  <button class="tab" data-tab="history" onclick="switchTab('history')">调度历史</button>
  <button class="tab" data-tab="raw" onclick="switchTab('raw')">原始配置</button>
</nav>
<main>
  <section id="tab-status" class="tab-panel active">
    <div class="card">
      <h3 class="card-title"><span class="dot"></span>账号运行状态</h3>
      <div class="toolbar">
        <button id="refreshQuotaBtn" class="btn btn-primary" onclick="refreshQuotas()">刷新额度</button>
        <button id="evalRunBtn" class="btn btn-light" onclick="triggerEvalRun()">降智测试</button>
        <span class="hint">点击后依次对所有 codex 账号执行降智检测（糖果题 + 指纹），完成后卡片上会更新“正常 / 降智”标识。</span>
      </div>
      <div style="overflow-x:auto">
      <div id="status" class="status-list"></div>
      </div>
      <div id="statusEmpty" class="empty" style="display:none">暂无账号数据</div>
    </div>
  </section>

  <section id="tab-settings" class="tab-panel">
    <div id="settingsForm"></div>
    <div class="card">
      <h3 class="card-title"><span class="dot"></span>账号（Accounts）</h3>
      <p class="section-desc">为单个账号配置覆盖项。未勾选或选择“继承”的字段会使用全局默认值；账号 ID 必须与认证文件里的账号 ID 一致。</p>
      <div id="accountsEditor"></div>
      <div class="toolbar">
        <button class="btn btn-light" onclick="addAccountRow()">＋ 添加账号</button>
        <span class="hint">ID 是唯一匹配键，例如 codex-xxx.json。角色、权重、并行、启用等均可按账号单独覆盖。</span>
      </div>
    </div>
    <div class="card">
      <h3 class="card-title"><span class="dot"></span>过载保护（Overload Rules）</h3>
      <p class="section-desc">消费型过载保护：在时间窗口内，某账号的令牌消耗或额度下降超过阈值时，自动停用该账号一段时间，期间不再被调度。命中任意规则即按最长的停用时长执行。</p>
      <div style="overflow-x:auto">
        <div id="overloadRules"></div>
      </div>
      <div class="toolbar">
        <button class="btn btn-light" onclick="addOverloadRule()">＋ 添加规则</button>
        <span class="hint">指标：额度百分比＝窗口内剩余额度下降幅度；令牌＝窗口内消耗的 token 总量。</span>
      </div>
    </div>
    <div class="toolbar">
      <button class="btn btn-primary" onclick="saveSettingsForm()">保存参数</button>
      <button class="btn btn-light" onclick="loadSettings()">重新读取</button>
      <span class="hint">保存后立即生效，并同步刷新账号状态。</span>
    </div>
  </section>

  <section id="tab-eval" class="tab-panel">
    <div class="card">
      <h3 class="card-title"><span class="dot"></span>巡检配置</h3>
      <div id="evalSettingsForm"></div>
      <div class="toolbar">
        <button class="btn btn-primary" onclick="saveEvalForm()">保存配置</button>
        <button class="btn btn-light" onclick="loadSettings()">重新读取</button>
        <button id="evalRunBtn2" class="btn btn-light" onclick="triggerEvalRun('evalRunBtn2')">立即巡检</button>
        <span class="hint">保存后立即生效；点击“立即巡检”依次检测全部 codex 账号。</span>
      </div>
    </div>
    <div class="card">
      <h3 class="card-title"><span class="dot"></span>巡检历史</h3>
      <p class="section-desc">按账号分组记录每次降智检测（糖果题 + lm-detector 指纹）的结果与证据。</p>
      <div class="toolbar">
        <button class="btn btn-primary" onclick="loadEvalHistory()">刷新</button>
        <button class="btn btn-light" onclick="exportEvalHistoryCsv()">导出 CSV</button>
        <button class="btn btn-light" onclick="clearEvalHistory()">清空</button>
      </div>
      <div id="evalHistoryStats" class="history-stats"></div>
      <div id="evalHistory"></div>
      <div id="evalHistoryEmpty" class="empty" style="display:none">暂无巡检记录</div>
    </div>
  </section>

  <section id="tab-history" class="tab-panel">
    <div class="card">
      <h3 class="card-title"><span class="dot"></span>调度分配历史</h3>
      <p class="section-desc">记录每次调度决策的完整上下文（会话/请求/候选明细）。可在参数设置中调整最大保留条数，超出后自动删除最旧记录。</p>
      <div class="toolbar">
        <button class="btn btn-primary" onclick="loadHistory()">刷新</button>
        <button class="btn btn-light" onclick="exportHistoryCsv()">导出 CSV</button>
        <button class="btn btn-light" onclick="clearHistory()">清空</button>
        <label class="switch" title="自动刷新（每 5 秒）"><input type="checkbox" id="historyAuto" onchange="toggleHistoryAuto()"><span class="slider"></span></label>
        <span class="hint">自动刷新</span>
      </div>
      <div id="historyStats" class="history-stats"></div>
      <div id="historyChart"></div>
      <div style="overflow-x:auto">
      <table id="history"><thead><tr><th>时间</th><th>结果</th><th>来源</th><th>模型</th><th>选中账号</th><th>分数</th><th>候选 / 池</th><th>原因</th><th>候选明细</th></tr></thead><tbody></tbody></table>
      </div>
      <div id="historyEmpty" class="empty" style="display:none">暂无调度记录</div>
    </div>
  </section>

  <section id="tab-raw" class="tab-panel">
    <div class="card">
      <h3 class="card-title"><span class="dot"></span>原始配置文件</h3>
      <div class="toolbar">
        <button id="jsonTabBtn" class="btn btn-primary" onclick="switchRawFormat('json')">JSON</button>
        <button id="yamlTabBtn" class="btn btn-light" onclick="switchRawFormat('yaml')">YAML</button>
        <button class="btn btn-light" onclick="loadRaw()">刷新</button>
        <button class="btn btn-primary" onclick="saveRaw()">保存配置</button>
        <span class="hint">JSON 与 YAML 均可直接编辑保存。</span>
      </div>
      <textarea id="rawArea" class="textarea" style="min-height:480px"></textarea>
    </div>
  </section>
</main>
<div id="toast" class="toast"></div>
<script>
var BASE = "/v0/management/codex-guard";
var KEY = localStorage.getItem("codex-guard-mgmt-key") || "";
var CFG = null;
var rawFormat = "json";

var SECTIONS = {
  "基本设置": {icon:"⚙", desc:"调度算法与接管范围，决定账号如何被排序与选择。"},
  "冷却": {icon:"⏱", desc:"账号出错后的临时停用时长与升级策略。会触发（递增）冷却的错误：真实额度/速率耗尽 429、服务端过载（overloaded，含响应体 service_unavailable、server_is_overloaded 等）；401/403 认证失败在未开启“自动禁用”时按“认证失败”单独冷却。瞬时准入 429（turn_rate_limit_exceeded / at capacity）只做短暂停用，不递增。502/503/504/408 与带重试提示的 500 是安全重试，不计入冷却与熔断。"},
  "认证失败": {icon:"🔑", desc:"401/403 认证失效（含 invalid_api_key、invalid_grant、unauthorized、forbidden、access denied 等）时的处理方式：自动禁用或临时冷却。"},
  "熔断器": {icon:"⛔", desc:"连续失败后在一段时间内停止调度该账号。计入熔断的错误：500（无 OpenAI 重试提示）以及无法识别的其他错误；502/503/504/408 与带重试提示的 500 为安全重试，不计入熔断；429/529/401/403 走冷却或禁用流程，不计入熔断。"},
  "递增冷却": {icon:"📈", desc:"短期内反复冷却时，逐步加重惩罚时长。"},
  "额度刷新": {icon:"🔄", desc:"后台轮询各账号额度，用于调度排序。"},
  "到期优先": {icon:"⏳", desc:"快过期的订阅优先消耗，避免额度作废。"},
  "会话亲和": {icon:"🧲", desc:"同一会话尽量复用同一账号。"},
  "并发与回退": {icon:"🧵", desc:"并发上限与无账号可用时的兜底行为。"},
  "调度历史": {icon:"🗂", desc:"调度记录的持久化与保留策略。"},
  "重试": {icon:"🔁", desc:"针对安全错误的重试预算与退避策略。注意：本插件不直接发起 HTTP 重放，真正重放由 CPA 的执行循环完成；这里的参数用于统一插件账务与界面展示的重试策略，并供退避时长计算使用。仅网络传输错误与 OpenAI 明确提示可重试的服务端错误会重试；过载（overload/529）永不重试。"},
  "巡检配置": {icon:"🧠", desc:"定期对所有 codex 账号依次执行降智检测（糖果题 + lm-detector 指纹），在账号卡片标记“正常 / 降智”。测试会按设定跳过异常账号（冷却/熔断/超负荷等）与高并发压力账号，避免影响正常调度。"}
};

var FIELDS = [
  {section:"基本设置", key:"strategy", label:"调度策略", type:"select", options:[["quota_pressure","额度压力"],["quota","额度"],["expiry","到期"]], help:"账号排序算法。额度压力＝剩余额度百分比 ÷ 到额度重置的剩余小时数，优先使用“压力”最小的账号（额度多且剩余时间长者优先）；额度＝仅按剩余额度百分比从高到低排序；到期＝按额度重置时间从近到远排序，优先使用快重置的账号。"},
  {section:"基本设置", key:"provider", label:"提供商", type:"text", help:"插件接管的提供商标识，默认 codex。只有该 provider 的账号会被本插件调度；其它 provider 的请求交回 CPA 内置调度处理。"},
  {section:"冷却", key:"cooldown_default_seconds", label:"默认冷却（秒）", type:"number", help:"触发限流（429 速率/额度耗尽）或服务端过载（529 overloaded）且响应没有 Retry-After 时使用的默认冷却时长，期间该账号不再被调度。529 的实际冷却 = Retry-After（缺省用此值）＋“固定附加冷却”。"},
  {section:"冷却", key:"cooldown_fixed_bonus_seconds", label:"固定附加冷却（秒）", type:"number", help:"服务端过载（529 overloaded）在基础冷却之上额外叠加的固定时长，让过载账号停用得更久。限流 429 不叠加该值。"},
  {section:"冷却", key:"max_cooldown_seconds", label:"最大冷却（秒）", type:"number", help:"冷却时长上限。任何冷却（含递增翻倍）都不会超过该值，防止冷却被无限放大。"},
  {section:"认证失败", key:"auth_failure_cooldown_seconds", label:"冷却时长（秒）", type:"number", help:"401/403 认证失败且未开启“失败后自动禁用”时的冷却时长，用于临时屏蔽失效或越权凭证。"},
  {section:"认证失败", key:"auto_disable_on_auth_failure", label:"失败后自动禁用", type:"bool", help:"开启后，401/403 认证失败会直接禁用该账号，直到后续 token 刷新成功才恢复；关闭则只做临时冷却。"},
  {section:"熔断器", key:"circuit_failure_threshold", label:"失败阈值（次）", type:"number", help:"连续失败达到该次数后打开熔断器。计入熔断的错误：500（无 OpenAI 重试提示）与无法识别的其他错误；502/503/504/408 与带重试提示的 500 为安全重试、不计入熔断；429/529/401/403 不计入熔断。"},
  {section:"熔断器", key:"circuit_open_duration_seconds", label:"开启时长（秒）", type:"number", help:"熔断器打开后的保持时长，期间该账号不再被调度，超时后自动恢复。"},
  {section:"递增冷却", key:"escalation_window_seconds", label:"统计窗口（秒）", type:"number", help:"递增冷却的统计窗口。窗口内冷却次数达到“触发阈值”时，下一次冷却时长会按倍率放大。"},
  {section:"递增冷却", key:"escalation_threshold", label:"触发阈值（次）", type:"number", help:"统计窗口内冷却次数达到该值后，触发冷却时长放大。"},
  {section:"递增冷却", key:"escalation_multiplier", label:"冷却倍率", type:"number", step:"0.1", help:"达到触发阈值后，冷却时长按此倍数放大（如 2 表示翻倍）。结果仍受“最大冷却”限制。"},
  {section:"额度刷新", key:"quota_refresh_enabled", label:"启用额度轮询", type:"bool", help:"开启后台定时拉取各账号最新额度，用于调度排序；关闭后仅靠请求时带回来的被动额度信息。"},
  {section:"额度刷新", key:"quota_refresh_interval_seconds", label:"轮询间隔（秒）", type:"number", help:"后台额度刷新的间隔（秒）。越短越实时，但对外发起的额度查询请求越频繁。"},
  {section:"额度刷新", key:"quota_hard_floor_percent", label:"硬下限百分比", type:"number", step:"0.5", help:"剩余额度低于该百分比时，账号视为已耗尽，不再被调度。"},
  {section:"到期优先", key:"expiry_priority_enabled", label:"启用到期优先", type:"bool", help:"开启后，快到期的订阅会获得更高排序权重，优先消耗以避免额度过期作废。依赖账号配置里的 expires_at 时间。"},
  {section:"到期优先", key:"expiry_priority_horizon_hours", label:"前瞻窗口（小时）", type:"number", step:"0.5", help:"到期优先的前瞻窗口（小时）。在此窗口内快到期的账号会获得额外排序权重，越临近到期权重越高。"},
  {section:"会话亲和", key:"session_affinity_enabled", label:"启用会话亲和", type:"bool", help:"开启后，同一会话尽量复用同一个账号，减少上下文切换。"},
  {section:"会话亲和", key:"session_affinity_ttl_seconds", label:"亲和 TTL（秒）", type:"number", help:"会话亲和绑定的有效时长，超时后重新选择账号。"},
  {section:"并发与回退", key:"max_parallel_default", label:"默认最大并行", type:"number", help:"每个账号默认允许的最大并发请求数，可在“账号（Accounts）”列表中单独覆盖。"},
  {section:"并发与回退", key:"fallback_reject", label:"无可用账号时拒绝", type:"bool", help:"开启后，所有账号都不可用时直接拒绝请求；关闭则交回 CPA 内置调度兜底。"},
  {section:"并发与回退", key:"in_flight_ttl_seconds", label:"在途租约 TTL（秒）", type:"number", help:"并发占用记录的有效时长，用于自动清理因异常未能释放的占用。"},
  {section:"调度历史", key:"history_max_entries", label:"最大保留条数", type:"number", help:"调度历史最多保留多少条，超出后自动删除最旧记录。0 或留空使用默认值 500。"},
  {section:"重试", key:"retry_max_attempts", label:"额外重试次数", type:"number", help:"对安全错误（网络传输错误、408/502/503/504、带 OpenAI 重试提示的 500、瞬时准入 429）最多额外重放的次数。过载（overload/529）与客户端 4xx 永不重试。0 表示不额外重试。实际重放由 CPA 执行循环完成。"},
  {section:"重试", key:"retry_backoff_base_seconds", label:"首次退避（秒）", type:"number", help:"第一次重试前的等待秒数（无 Retry-After 提示时的回退值）。后续每次重试的等待时间按 2 倍递增。"},
  {section:"重试", key:"retry_backoff_max_seconds", label:"最大退避（秒）", type:"number", help:"重试退避等待的上限。指数递增到该值后不再继续翻倍。"},
  {section:"重试", key:"retry_backoff_jitter", label:"退避抖动", type:"bool", help:"开启后在退避等待中加入随机抖动（上下浮动），避免大量请求在同一时刻集中重试造成拥塞。"},
  {section:"巡检配置", key:"eval_enabled", label:"启用降智测试", type:"bool", help:"开启后，后台按设定间隔对所有 codex 账号依次执行降智检测，并把结果写入账号卡片。关闭后停止自动检测，但“立即测试”仍可用。"},
  {section:"巡检配置", key:"eval_interval_minutes", label:"巡检间隔（分钟）", type:"number", help:"每间隔多少分钟对全部账号依次跑一遍检测。0 表示关闭定时巡检（仅手动触发）。间隔不宜过短，以免占用账号并发额度。"},
  {section:"巡检配置", key:"eval_concurrency", label:"巡检并发数", type:"number", help:"降智巡检同时检测的账号数量上限。并发越高巡检越快，但会给 CPA 带来更大瞬时压力，默认 3。"},
  {section:"巡检配置", key:"eval_model", label:"检测模型", type:"select", options:[["","自动（使用账号配置模型）"]], help:"执行检测时使用的模型。留空（自动）则优先使用账号自身配置的模型（host.auth.models），再回退到 gpt-5；也可以从下拉列表中选择具体模型。"},
  {section:"巡检配置", key:"eval_effort", label:"推理强度", type:"select", options:[["low","低"],["medium","中"],["high","高"],["none","无"]], help:"检测请求使用的推理强度（reasoning effort）。糖果题与指纹计数通常用低强度即可，越低越省额度。"},
  {section:"巡检配置", key:"eval_skip_abnormal", label:"跳过异常账号", type:"bool", help:"开启后，处于禁用、冷却、瞬时停用、熔断、超负荷等异常状态的账号不参与本轮检测，等恢复正常后再测。"},
  {section:"巡检配置", key:"eval_skip_high_pressure", label:"跳过高压账号", type:"bool", help:"开启后，当前并行数或统计窗口内的并行峰值超过“高压并行阈值”的账号不参与本轮检测，避免测试挤占正在忙碌的账号。"},
  {section:"巡检配置", key:"eval_high_pressure_parallel", label:"高压并行阈值", type:"number", help:"判断账号是否“高压”的并行数阈值。当前并行或窗口内峰值大于该值时跳过检测。默认 2，即 10 分钟内有 2 个以上并行调用就不测。"},
  {section:"巡检配置", key:"eval_high_pressure_window_seconds", label:"高压统计窗口（秒）", type:"number", help:"计算并行峰值所用的时间窗口。默认 600 秒（10 分钟）。"},
  {section:"巡检配置", key:"eval_history_max_entries", label:"历史最大保留条数", type:"number", help:"巡检历史最多保留多少条记录，超出后自动删除最旧记录。0 或留空使用默认值 500。"}
];

function authHeaders(extra){
  var h = {};
  for (var k in (extra||{})) h[k] = extra[k];
  if (KEY) h["Authorization"] = "Bearer " + KEY;
  return h;
}

function showLogin(msg){
  document.getElementById("loginbar").style.display = "flex";
  document.getElementById("logoutBtn").style.display = "none";
  if (msg) document.getElementById("loginmsg").textContent = msg;
}
function hideLogin(){
  document.getElementById("loginbar").style.display = "none";
  document.getElementById("logoutBtn").style.display = "inline-flex";
  document.getElementById("loginmsg").textContent = "";
}

async function api(path, opts){
  opts = opts || {};
  opts.headers = authHeaders(opts.headers);
  var r = await fetch(BASE + path, opts);
  if (r.status === 401 || r.status === 403) {
    var e = {};
    try { e = await r.json(); } catch(_) {}
    var msg = e.error || ("需要登录 (" + r.status + ")");
    KEY = "";
    localStorage.removeItem("codex-guard-mgmt-key");
    showLogin(msg);
    throw new Error(msg);
  }
  return r;
}

async function login(){
  var k = document.getElementById("key").value.trim();
  if (!k) { showLogin("请输入管理密钥"); return; }
  var r = await fetch(BASE + "/status", {headers:{"Authorization":"Bearer " + k}});
  if (r.ok) {
    KEY = k;
    localStorage.setItem("codex-guard-mgmt-key", k);
    hideLogin();
    loadAll();
  } else {
    var e = {};
    try { e = await r.json(); } catch(_) {}
    KEY = "";
    localStorage.removeItem("codex-guard-mgmt-key");
    showLogin("登录失败: " + (e.error || r.status));
  }
}

function logout(){
  KEY = "";
  localStorage.removeItem("codex-guard-mgmt-key");
  document.getElementById("key").value = "";
  showLogin();
}

function toast(msg, isError){
  var t = document.getElementById("toast");
  t.textContent = msg;
  t.className = "toast show " + (isError ? "error" : "ok");
  clearTimeout(t._timer);
  t._timer = setTimeout(function(){ t.className = "toast"; }, 2600);
}

function switchTab(name){
  document.querySelectorAll(".tab").forEach(function(b){ b.classList.toggle("active", b.getAttribute("data-tab") === name); });
  document.querySelectorAll(".tab-panel").forEach(function(p){ p.classList.toggle("active", p.id === "tab-" + name); });
  if (name === "raw") loadRaw();
  if (name === "eval") loadEvalHistory();
  if (name === "history") loadHistory();
  if (name === "status") loadStatus();
}

var pageAutoTimer = null;
var AUTO_REFRESH_KEY = "codex-guard-autorefresh";

function currentTab(){
  var active = document.querySelector(".tab.active");
  return active ? active.getAttribute("data-tab") : "status";
}

function restartAutoRefresh(){
  if (pageAutoTimer) { clearInterval(pageAutoTimer); pageAutoTimer = null; }
  var sel = document.getElementById("autoRefresh");
  var v = parseInt(sel ? sel.value : "0", 10) || 0;
  if (v > 0) { pageAutoTimer = setInterval(autoRefreshTick, v * 1000); }
}

function setAutoRefresh(){
  var sel = document.getElementById("autoRefresh");
  var v = parseInt(sel ? sel.value : "0", 10) || 0;
  localStorage.setItem(AUTO_REFRESH_KEY, String(v));
  restartAutoRefresh();
  if (v > 0) toast("自动刷新已开启：" + v + " 秒");
}

function initAutoRefresh(){
  var sel = document.getElementById("autoRefresh");
  var saved = parseInt(localStorage.getItem(AUTO_REFRESH_KEY) || "0", 10) || 0;
  var valid = [0, 5, 10, 20, 30];
  if (valid.indexOf(saved) < 0) saved = 0;
  sel.value = String(saved);
  restartAutoRefresh();
}

function autoRefreshTick(){
  if (!KEY) return;
  var tab = currentTab();
  if (tab === "status") loadStatus();
  else if (tab === "eval") loadEvalHistory();
  else if (tab === "history") loadHistory();
}

function esc(s){ return String(s).replace(/'/g, "\\'"); }
function escHtml(s){ return String(s).replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;").replace(/"/g,"&quot;"); }
function fmtRole(r){ return r === "backup" ? "备用" : (r === "primary" ? "主" : r); }
function badge(text, cls){ return text ? '<span class="badge ' + cls + '">' + text + '</span>' : ''; }
function rectBadge(text, cls){ return text ? '<span class="badge badge-rect ' + cls + '">' + text + '</span>' : ''; }

function tierBadge(a){
  var tier = a.tier || a.role || "primary";
  if (tier === "degraded") return "";
  var map = {
    primary:  ["主", "badge-primary"],
    backup:   ["备用", "badge-muted"]
  };
  var m = map[tier] || map.primary;
  return '<span class="badge badge-rect ' + m[1] + '">' + m[0] + '</span>';
}

function evalBadge(a, id){
  var st = a.eval_status || "unknown";
  var label = a.eval_label || (st === "normal" ? "正常" : st === "degraded" ? "降智" : "未知");
  var cls = st === "normal" ? "badge-ok" : st === "degraded" ? "badge-bad" : "badge-muted";
  var tips = [];
  if (a.eval_last_run_at) tips.push("检测时间 " + fmtTimeLocal(a.eval_last_run_at));
  if (a.eval_candy) tips.push("糖果题 " + candyLabel(a.eval_candy));
  if (a.eval_fingerprint) tips.push("指纹 " + fingerprintLabel(a.eval_fingerprint));
  if (a.eval_prediction || a.eval_prediction_name) tips.push("指纹检测到 " + (a.eval_prediction_name || a.eval_prediction) + (a.eval_family ? "（" + a.eval_family + "）" : "") + (a.eval_probability ? "，置信度 " + a.eval_probability : ""));
  if (a.eval_candy_error) tips.push("糖果失败原因 " + a.eval_candy_error);
  if (a.eval_fingerprint_error) tips.push("指纹失败原因 " + a.eval_fingerprint_error);
  if (a.eval_fingerprint_samples_error) tips.push("指纹样本错误 " + a.eval_fingerprint_samples_error);
  if (a.eval_skip_reason) tips.push("跳过原因 " + a.eval_skip_reason);
  tips.push("点击重新检测该账号");
  var title = tips.length ? ' title="' + escHtml(tips.join(" · ")) + '"' : '';
  return '<button type="button" class="badge badge-rect badge-click ' + cls + '"' + title + ' onclick="triggerAccountEval(\'' + id + '\')">' + escHtml(label) + '</button>';
}
function fmtTimeLocal(iso){
  if (!iso) return "—";
  var d = new Date(iso);
  if (isNaN(d.getTime())) return iso;
  return d.toLocaleString();
}

function durUntil(iso){
  if (!iso) return "";
  var d = new Date(iso);
  if (isNaN(d.getTime())) return "";
  var diff = d.getTime() - Date.now();
  var neg = diff < 0;
  var abs = Math.abs(diff);
  var days = Math.floor(abs / 86400000);
  var hours = Math.floor((abs % 86400000) / 3600000);
  var mins = Math.floor((abs % 3600000) / 60000);
  var parts = [];
  if (days > 0) parts.push(days + "天");
  if (hours > 0) parts.push(hours + "小时");
  if (parts.length === 0) parts.push(mins + "分钟");
  var s = parts.join("");
  return neg ? ("已过期 " + s) : ("剩余 " + s);
}

function accountName(a){
  return a.email ? a.email : (a.auth_id || "");
}

function quotaBar(pct){
  var p = Math.max(0, Math.min(100, pct || 0));
  return '<div class="quota-bar"><span style="width:' + p.toFixed(1) + '%"></span></div>';
}

var PLAN_META = {
  plus:       "Plus",
  pro:        "Pro",
  pro100:     "Pro100",
  pro200:     "Pro200",
  pro500:     "Pro500",
  free:       "Free",
  go:         "Go",
  team:       "Team",
  business:   "Business",
  enterprise: "Enterprise",
  edu:        "Edu",
  unknown:    "未知"
};

function planTierKey(a){
  var raw = String(a.plan_type || "").toLowerCase().trim();
  function has(s){ return raw.indexOf(s) >= 0; }
  if (has("pro500") || has("pro_500")) return "pro500";
  if (has("pro200") || has("pro_200")) return "pro200";
  if (has("pro100") || has("pro_100") || has("prolite") || has("pro_lite")) return "pro100";
  if (raw === "pro") return "pro";
  if (raw === "plus") return "plus";
  if (raw === "go") return "go";
  if (raw === "free") return "free";
  if (has("business")) return "business";
  if (raw === "team") return "team";
  if (has("enterprise") || has("ent26") || has("cbp")) return "enterprise";
  if (has("edu")) return "edu";
  var l = String(a.plan_label || "").toLowerCase();
  if (l.indexOf("pro500") >= 0) return "pro500";
  if (l.indexOf("pro200") >= 0) return "pro200";
  if (l.indexOf("pro100") >= 0 || l.indexOf("prolite") >= 0 || l.indexOf("pro lite") >= 0) return "pro100";
  if (l === "pro") return "pro";
  if (l === "plus") return "plus";
  if (l === "go") return "go";
  if (l === "free") return "free";
  if (l.indexOf("business") >= 0) return "business";
  if (l === "team") return "team";
  if (l.indexOf("enterprise") >= 0 || l.indexOf("cbp") >= 0) return "enterprise";
  if (l.indexOf("edu") >= 0) return "edu";
  return "unknown";
}

function planBadge(a){
  var tier = planTierKey(a);
  var meta = PLAN_META[tier] || PLAN_META.unknown;
  var label = a.plan_label || a.plan_type || meta.label;
  return '<span class="plan-pill plan-pill-' + tier + '">' + escHtml(label) + '</span>';
}

function quotaCell(a){
  var head = '<div class="quota-head">' + planBadge(a) + subscriptionInline(a) + '</div>';
  if (!a.quota_observed_at) return head + '<span class="badge badge-rect badge-muted">未采集</span>';
  var html = head;
  var wins = a.quota_windows || [];
  if (wins.length) {
    wins.forEach(function(w){
      var used = (w.used_percent != null) ? w.used_percent : 0;
      var rem = (w.remaining_percent != null) ? w.remaining_percent : (100 - used);
      html += '<div class="quota-window">';
      html += '<div class="quota-line"><b>' + escHtml(w.name || w.window_label || "窗口") + '</b>' + (w.window_label ? ' <span class="hint">' + escHtml(w.window_label) + '</span>' : '') + '</div>';
      html += quotaBar(rem);
      html += '<div class="quota-line">剩余 ' + rem.toFixed(1) + '% · 已用 ' + used.toFixed(1) + '%</div>';
      if (w.reset_at) html += '<div class="quota-line">额度重置 ' + fmtTimeLocal(w.reset_at) + (durUntil(w.reset_at) ? ' · <b>' + durUntil(w.reset_at) + '</b>' : '') + '</div>';
      html += '</div>';
    });
  } else {
    var used = (a.used_percent != null) ? a.used_percent : (100 - (a.remaining_percent || 0));
    var rem = (a.remaining_percent != null) ? a.remaining_percent : (100 - used);
    html += quotaBar(rem);
    html += '<div class="quota-line">已用 ' + used.toFixed(1) + '% · 剩余 ' + rem.toFixed(1) + '%</div>';
    if (a.reset_at) html += '<div class="quota-line">额度重置 ' + fmtTimeLocal(a.reset_at) + (durUntil(a.reset_at) ? ' · <b>' + durUntil(a.reset_at) + '</b>' : '') + '</div>';
  }
  if (a.limit_reached) {
    html += '<div class="quota-line" style="color:var(--danger)">已达上限' + (a.limit_reached_type ? '（' + escHtml(a.limit_reached_type) + '）' : '') + '</div>';
  } else if (a.quota_allowed === false) {
    html += '<div class="quota-line" style="color:var(--danger)">未放行</div>';
  }
  return html;
}

function subscriptionInline(a){
  var until = a.subscription_active_until || a.subscription_expires_at;
  if (!until) return '<span class="plan-sub muted">订阅到期未设置</span>';
  var s = '订阅续期 <span class="plan-sub-time">' + fmtTimeLocal(until) + '</span>';
  if (durUntil(until)) s += ' · <span class="plan-sub-dur">' + durUntil(until) + '</span>';
  return '<span class="plan-sub">' + s + '</span>';
}

function creditInt(v){
  if (v === undefined || v === null || v === "") return "";
  var n = Number(v);
  if (isNaN(n)) return String(v);
  return String(Math.trunc(n));
}

function billingLine(a, id){
  var chips = [];
  var creditKnown = (a.credit_balance !== undefined && a.credit_balance !== null && a.credit_balance !== "");
  if (creditKnown) {
    var credit = creditInt(a.credit_balance);
    chips.push('<span class="bill-chip">Credit 余额 <b>' + escHtml(credit) + '</b>' + (a.credit_unlimited ? ' · 无限' : '') + '</span>');
  }
  var credits = a.reset_credits || [];
  if (credits.length) {
    credits.forEach(function(c, i){
      var label = '主动重置 #' + (i + 1);
      if (c.status) label += ' · ' + escHtml(c.status);
      if (c.expires_at) {
        label += ' · <span class="bill-dur">' + durUntil(c.expires_at) + '</span>';
      }
      chips.push('<span class="bill-chip">' + label + '</span>');
    });
  } else if (a.reset_credits_available != null) {
    var rc = a.reset_credits_available;
    var extra = (a.reset_credits_applicable != null && a.reset_credits_applicable !== rc) ? ' · 适用 ' + a.reset_credits_applicable : '';
    chips.push('<span class="bill-chip">主动重置 <b>' + rc + '</b>' + extra + '</span>');
  }
  var canRedeem = credits.some(function(c){ return c.status === "available"; }) || (a.reset_credits_available != null && a.reset_credits_available > 0);
  var btn = canRedeem ? '<button class="btn btn-sm btn-primary" onclick="confirmRedeem(\'' + id + '\')">主动重置</button>' : '';
  if (!chips.length) return btn ? '<div class="billing-row">' + btn + '</div>' : '';
  return '<div class="billing-row">' + chips.join('') + btn + '</div>';
}

function confirmRedeem(id){
  confirmModal("是否触发该账号的官方额度重置？", function(){ accountAction(id, "redeem_reset_credit"); });
}

function protectionCell(a, id){
  var actionMap = {cooldown:"reset_cooldown", circuit:"reset_circuit", overload:"reset_overload"};
  var prot = a.protections || [];
  var html = '<div class="mini-actions">';
  prot.forEach(function(p){
    var active = !!p.active;
    var cls = "prot-chip" + (active ? " on" : "");
    var action = actionMap[p.kind] || "";
    var click = (active && action) ? ' onclick="confirmReset(\'' + id + '\',\'' + action + '\',\'' + esc(p.label || "") + '\')"' : '';
    var data = active ? ' data-prot="' + escHtml(JSON.stringify(p)) + '"' : '';
    html += '<span class="' + cls + '"' + data + click + '>' + escHtml(p.label || "") + '</span>';
  });
  return html + '</div>';
}

function confirmReset(id, action, label){
  confirmModal("是否重置" + label + "状态？", function(){ accountAction(id, action); });
}

function confirmModal(message, onYes){
  var overlay = document.createElement("div");
  overlay.className = "modal-overlay";
  overlay.innerHTML = '<div class="modal">'
    + '<div class="modal-title">确认重置</div>'
    + '<div class="modal-text">' + escHtml(message) + '</div>'
    + '<div class="modal-actions">'
    + '<button class="btn btn-light" id="cgModalNo">取消</button> '
    + '<button class="btn btn-primary" id="cgModalYes">确认</button>'
    + '</div></div>';
  document.body.appendChild(overlay);
  overlay.querySelector("#cgModalNo").onclick = function(){ overlay.remove(); };
  overlay.querySelector("#cgModalYes").onclick = function(){ overlay.remove(); if (onYes) onYes(); };
  overlay.addEventListener("click", function(ev){ if (ev.target === overlay) overlay.remove(); });
}

function cpaCell(a, id){
  var disabled = !!(a.host_disabled || a.host_status === "disabled");
  var stateText = disabled ? "禁用" : (a.unavailable ? "不可用" : "启用");
  var stateCls = disabled ? "badge-bad" : (a.unavailable ? "badge-warn" : "badge-ok");
  var title = a.status_message || stateText;
  var sw = '<label class="switch" title="' + escHtml(title) + '"><input type="checkbox" ' + (disabled ? '' : 'checked') + ' onchange="toggleCpa(\'' + id + '\',this.checked)"><span class="slider"></span></label>';
  return '<div class="mini-actions">' + sw + ' ' + rectBadge(stateText, stateCls) + '</div>';
}

async function loadStatus(){
  var r = await api("/status");
  var d = await r.json();
  var wrap = document.getElementById("status");
  wrap.innerHTML = "";
  var accounts = d.accounts || [];
  document.getElementById("statusEmpty").style.display = accounts.length ? "none" : "block";
  accounts.forEach(function(a){
    var id = esc(a.auth_id);
    var tier = a.tier || a.role || "primary";
    var roleSelect = '<select class="cell-select" onchange="saveField(\'' + id + '\',\'role\',this.value)">'
      + '<option value="primary"' + (tier === "primary" ? " selected" : "") + '>主</option>'
      + '<option value="backup"' + (tier === "backup" ? " selected" : "") + '>备用</option>'
      + '<option value="degraded"' + (tier === "degraded" ? " selected" : "") + '>降智</option>'
      + '</select>';
    var weightInput = '<input class="cell-input" type="number" step="0.1" min="0" value="' + a.weight + '" onchange="saveField(\'' + id + '\',\'weight\',this.value)">';
    var card = document.createElement("div");
    card.className = "acct-card" + (a.disabled ? " acct-card-disabled" : "");
    var name = accountName(a);
    card.innerHTML =
      '<div class="acct-card-top">'
      + '<span class="acct-name" title="' + escHtml(name) + '">' + escHtml(name) + '</span>'
      + '<div class="acct-card-meta">' + tierBadge(a) + ' ' + cpaCell(a, id) + '</div>'
      + '</div>'
      + '<div class="acct-card-grid">'
      + '<div class="acct-cell"><span class="acct-cell-label">定位</span>' + roleSelect + '</div>'
      + '<div class="acct-cell"><span class="acct-cell-label">权重</span>' + weightInput + '</div>'
      + '<div class="acct-cell"><span class="acct-cell-label">并行</span><span class="acct-cell-value">' + a.in_flight + ' / ' + a.max_parallel + '</span></div>'
      + '</div>'
      + '<div class="acct-card-quota">' + quotaCell(a) + billingLine(a, id) + '</div>'
      + '<div class="acct-card-footer">'
      + '<div class="acct-cell"><span class="acct-cell-label">保护状态</span><div>' + protectionCell(a, id) + '</div></div>'
      + '<div class="acct-cell"><span class="acct-cell-label">降智检测</span><div class="mini-actions">' + evalBadge(a, id) + '</div></div>'
      + '</div>';
    card.querySelectorAll(".prot-chip.on[data-prot]").forEach(function(el){
      var p = {};
      try { p = JSON.parse(el.getAttribute("data-prot") || "{}"); } catch(_) {}
      el.addEventListener("mouseenter", function(ev){ showProtTooltip(ev, p); });
      el.addEventListener("mousemove", moveEvalTooltip);
      el.addEventListener("mouseleave", hideEvalTooltip);
    });
    wrap.appendChild(card);
  });
}

async function saveField(id, field, value){
  var body = {id: id, action: "update_account"};
  if (field === "role") body.role = value;
  else if (field === "weight") body.weight = parseFloat(value);
  else if (field === "enabled") body.enabled = value;
  try {
    var r = await api("/account", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify(body)});
    var d = await r.json();
    if (r.ok) { toast("已保存"); if (d && d.config) CFG = d.config; await loadStatus(); }
    else { toast("保存失败: " + JSON.stringify(d), true); }
  } catch(e) { toast(e.message, true); }
}

async function toggleCpa(id, enable){
  try {
    var r = await api("/account", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify({id:id, action:"set_cpa", enabled:enable})});
    var d = await r.json();
    if (r.ok) { toast(enable ? "CPA 已启用" : "CPA 已禁用"); await loadStatus(); }
    else { toast("操作失败: " + JSON.stringify(d), true); }
  } catch(e) { toast(e.message, true); }
}

async function accountAction(id, action){
  try {
    var r = await api("/account", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify({id:id, action:action})});
    var d = await r.json();
    if (r.ok) { toast(d.message || "操作成功"); await loadStatus(); }
    else { toast("操作失败: " + JSON.stringify(d), true); }
  } catch(e) { toast(e.message, true); }
}

async function triggerAccountEval(id){
  try {
    var r = await api("/account", {method:"POST", headers:{"Content-Type":"application/json"}, body: JSON.stringify({id:id, action:"eval_test"})});
    var d = await r.json();
    if (r.ok) {
      toast(d.running ? "已开始检测该账号" : (d.message || "检测已在进行中"));
      setTimeout(loadStatus, 2500);
      if (currentTab() === "eval") setTimeout(loadEvalHistory, 3000);
    } else {
      toast("检测失败: " + JSON.stringify(d), true);
    }
  } catch(e) { toast(e.message, true); }
}

async function triggerEvalRun(btnId){
  var btn = document.getElementById(btnId || "evalRunBtn");
  if (!btn) return;
  btn.disabled = true;
  var old = btn.textContent;
  btn.textContent = "检测中…";
  try {
    var r = await api("/eval/run", {method:"POST", headers:{"Content-Type":"application/json"}, body:"{}"});
    var d = await r.json();
    if (r.ok) {
      toast(d.running ? "已开始降智检测，完成后自动更新" : (d.message || "检测已在进行中"));
      setTimeout(loadStatus, 2500);
      if (currentTab() === "eval") setTimeout(loadEvalHistory, 3000);
    } else {
      toast("检测失败: " + JSON.stringify(d), true);
    }
  } catch(e) {
    toast(e.message, true);
  } finally {
    btn.textContent = old;
    btn.disabled = false;
  }
}

async function refreshQuotas(){
  var btn = document.getElementById("refreshQuotaBtn");
  if (btn.disabled) return;
  btn.disabled = true;
  var old = btn.textContent;
  btn.textContent = "刷新中…";
  try {
    var r = await api("/refresh", {method:"POST", headers:{"Content-Type":"application/json"}, body:"{}"});
    var d = await r.json();
    if (r.ok) {
      if (d.refreshing) {
        toast("已触发额度刷新，稍后自动更新");
        setTimeout(loadStatus, 2500);
      } else {
        toast(d.message || "额度刷新已在进行中");
        setTimeout(loadStatus, 1500);
      }
    } else {
      toast("刷新失败: " + JSON.stringify(d), true);
    }
  } catch(e) {
    toast(e.message, true);
  } finally {
    btn.textContent = old;
    btn.disabled = false;
  }
}

var historyAutoTimer = null;

function shortAuth(id){
  if (!id) return id;
  var m = String(id).match(/[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}/);
  return m ? m[0] : id;
}

function resBadge(result){
  var map = {assigned:["已分配","res-assigned"], affinity:["亲和命中","res-affinity"], rejected:["已拒绝","res-rejected"], fallback:["兜底","res-fallback"]};
  var m = map[result] || [result || "未知","res-fallback"];
  return '<span class="res-badge ' + m[1] + '">' + m[0] + '</span>';
}

function sourceBadge(source){
  if (source === "degradation_test") {
    return '<span class="res-badge res-affinity" title="源于降智测试的指定调用">降智测试</span>';
  }
  return '<span class="res-badge res-assigned" title="正常业务调度">正常</span>';
}

async function loadHistory(){
  try {
    var r = await api("/history?limit=200");
    var d = await r.json();
    renderHistory(d.entries || [], d.total || 0);
  } catch(e) { toast(e.message, true); }
}

function renderHistory(entries, total){
  var stats = document.getElementById("historyStats");
  var chart = document.getElementById("historyChart");
  var tb = document.querySelector("#history tbody");
  var empty = document.getElementById("historyEmpty");
  tb.innerHTML = "";

  var counts = {assigned:0, affinity:0, rejected:0, fallback:0};
  var degradationTestCount = 0;
  var byAcct = {};
  entries.forEach(function(e){
    counts[e.result] = (counts[e.result] || 0) + 1;
    if (e.source === "degradation_test") degradationTestCount++;
    if (e.selected) byAcct[e.selected] = (byAcct[e.selected] || 0) + 1;
  });

  var maxKeep = (CFG && CFG.history_max_entries) ? CFG.history_max_entries : 500;
  stats.innerHTML = '<div class="stat-card"><div class="stat-num">' + total + '</div><div class="stat-label">历史总量</div></div>'
    + '<div class="stat-card"><div class="stat-num">' + entries.length + '</div><div class="stat-label">当前视图</div></div>'
    + '<div class="stat-card"><div class="stat-num">' + maxKeep + '</div><div class="stat-label">最大保留</div></div>'
    + '<div class="stat-card"><div class="stat-num">' + (counts.assigned||0) + '</div><div class="stat-label">已分配</div></div>'
    + '<div class="stat-card"><div class="stat-num">' + (counts.affinity||0) + '</div><div class="stat-label">亲和命中</div></div>'
    + '<div class="stat-card"><div class="stat-num">' + (counts.rejected||0) + '</div><div class="stat-label">已拒绝</div></div>'
    + '<div class="stat-card"><div class="stat-num">' + (counts.fallback||0) + '</div><div class="stat-label">兜底</div></div>'
    + '<div class="stat-card"><div class="stat-num">' + degradationTestCount + '</div><div class="stat-label">降智测试调用</div></div>';

  var keys = Object.keys(byAcct).sort(function(a,b){ return byAcct[b] - byAcct[a]; });
  var chartHtml = "";
  if (keys.length) {
    var max = byAcct[keys[0]];
    var top = keys.slice(0, 10);
    top.forEach(function(k){
      var pct = max > 0 ? Math.round(byAcct[k] / max * 100) : 0;
      chartHtml += '<div class="hist-bar-row"><div class="hist-bar-label" title="' + escHtml(k) + '">' + escHtml(shortAuth(k)) + '</div><div class="hist-bar"><span style="width:' + pct + '%"></span></div><div class="hist-bar-num">' + byAcct[k] + ' 次</div></div>';
    });
    if (keys.length > 10) {
      var rest = 0;
      for (var i=10;i<keys.length;i++) rest += byAcct[keys[i]];
      chartHtml += '<div class="hist-bar-row"><div class="hist-bar-label">其它 ' + (keys.length-10) + ' 个账号</div><div class="hist-bar"><span style="width:' + (max>0?Math.round(rest/max*100):0) + '%"></span></div><div class="hist-bar-num">' + rest + ' 次</div></div>';
    }
  } else {
    chartHtml = '<p class="hint">暂无按账号的分配分布。</p>';
  }
  chart.innerHTML = chartHtml;

  empty.style.display = entries.length ? "none" : "block";

  entries.forEach(function(e){
    var tr = document.createElement("tr");
    var poolHtml = "";
    var pool = e.pool || [];
    if (pool.length) {
      poolHtml = '<div class="pool-chips">';
      pool.forEach(function(p){
        var top = p.auth_id === e.selected;
        poolHtml += '<span class="pool-chip' + (top?' pool-top':'') + '" title="' + escHtml(p.auth_id) + '">' + escHtml(shortAuth(p.auth_id)) + ' <span class="sc">' + (p.score != null ? p.score.toFixed(2) : "—") + '</span>' + (p.role==='backup'?' <span class="sc">备用</span>':'') + '</span>';
      });
      poolHtml += '</div>';
    } else {
      poolHtml = '<span class="hint">—</span>';
    }
    var cells = [
      fmtTimeLocal(e.timestamp),
      resBadge(e.result),
      sourceBadge(e.source),
      escHtml(e.model || "—"),
      e.selected ? escHtml(shortAuth(e.selected)) : '<span class="hint">—</span>',
      (e.selected_score != null) ? e.selected_score.toFixed(2) : "—",
      (e.candidates || 0) + " / " + (e.pool_size != null ? e.pool_size : 0),
      escHtml(e.reason || "—"),
      poolHtml
    ];
    tr.innerHTML = cells.map(function(c){ return "<td>" + c + "</td>"; }).join("");
    tb.appendChild(tr);
  });
}

async function clearHistory(){
  try {
    var r = await api("/history/clear", {method:"POST", headers:{"Content-Type":"application/json"}, body:"{}"});
    var d = await r.json();
    if (r.ok) { toast("调度历史已清空"); await loadHistory(); }
    else { toast("清空失败: " + JSON.stringify(d), true); }
  } catch(e) { toast(e.message, true); }
}

async function exportHistoryCsv(){
  try {
    var r = await api("/history/export");
    if (!r.ok) {
      var e = {};
      try { e = await r.json(); } catch(_) {}
      toast("导出失败: " + (e.error || r.status), true);
      return;
    }
    var blob = await r.blob();
    var url = URL.createObjectURL(blob);
    var a = document.createElement("a");
    a.href = url;
    a.download = "codex-guard-history.csv";
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(function(){ URL.revokeObjectURL(url); }, 1000);
    toast("CSV 已导出");
  } catch(e) { toast(e.message, true); }
}

function toggleHistoryAuto(){
  if (document.getElementById("historyAuto").checked) {
    loadHistory();
    historyAutoTimer = setInterval(loadHistory, 5000);
  } else if (historyAutoTimer) {
    clearInterval(historyAutoTimer);
    historyAutoTimer = null;
  }
}

function evalStatusBadge(st){
  st = st || "unknown";
  if (st === "normal") return rectBadge("正常", "badge-ok");
  if (st === "degraded") return rectBadge("降智", "badge-bad");
  return rectBadge("未知", "badge-muted");
}

function candyLabel(s){
  var m = {correct:"答对", wrong:"答错", failed:"失败"};
  return m[s] || s || "—";
}

function fingerprintLabel(s){
  var m = {model:"像模型", not_model:"不像模型", partial:"部分有效", failed:"失败", skipped:"跳过"};
  return m[s] || s || "—";
}

function effortLabel(s){
  var m = {low:"低", medium:"中", high:"高", none:"无"};
  return m[(s||"").toLowerCase()] || s || "—";
}

function fingerprintPredictionText(e){
  if (e.fingerprint === "not_model") return "未识别为已知模型";
  var name = e.prediction_name || e.prediction || "";
  var family = e.family ? "（" + e.family + "）" : "";
  var prob = e.probability ? "，置信度 " + e.probability : "";
  return (name || "—") + family + prob;
}

async function loadEvalHistory(){
  try {
    var r = await api("/eval/history");
    var d = await r.json();
    renderEvalHistory(d.entries || [], d.total || 0, d.accounts || []);
  } catch(e) { toast(e.message, true); }
}

function evalHasError(e){
  if (!e) return false;
  if (e.candy_error || e.fingerprint_error || e.fingerprint_samples_error) return true;
  if (e.candy === "failed" || e.fingerprint === "failed") return true;
  var samples = e.fingerprint_samples || [];
  for (var i=0;i<samples.length;i++){ if (samples[i] && samples[i].error) return true; }
  return false;
}

function evalSlotClass(e){
  if (!e) return "muted";
  if (e.status === "normal") return "ok";
  if (e.status === "degraded" || evalHasError(e)) return "bad";
  return "muted";
}

function evalSlotLabel(e){
  if (!e) return "未测试";
  if (e.status === "normal") return "正常";
  if (e.status === "degraded" || evalHasError(e)) return "错误";
  return "未知";
}

function evalTooltip(){
  if (!window._evalTooltip) {
    var el = document.createElement("div");
    el.className = "eval-tooltip";
    el.style.display = "none";
    document.body.appendChild(el);
    window._evalTooltip = el;
  }
  return window._evalTooltip;
}

function moveEvalTooltip(ev){
  var t = evalTooltip();
  var pad = 14;
  var x = ev.clientX + pad;
  var y = ev.clientY + pad;
  var r = t.getBoundingClientRect();
  if (x + r.width > window.innerWidth - 8) x = ev.clientX - r.width - pad;
  if (y + r.height > window.innerHeight - 8) y = ev.clientY - r.height - pad;
  t.style.left = Math.max(4, x) + "px";
  t.style.top = Math.max(4, y) + "px";
}

function showEvalTooltip(ev, e){
  var t = evalTooltip();
  t.innerHTML = evalTooltipHtml(e);
  t.style.display = "block";
  moveEvalTooltip(ev);
}

function hideEvalTooltip(){
  var t = evalTooltip();
  if (t) t.style.display = "none";
}

function evalTooltipHtml(e){
  var lines = [];
  lines.push("<b>" + escHtml(fmtTimeLocal(e.timestamp)) + "</b> · " + evalStatusBadge(e.status));
  if (e.model || e.effort) lines.push("调用模型：" + escHtml(e.model || "—") + (e.effort ? " · 思考强度：" + escHtml(effortLabel(e.effort)) : ""));
  lines.push('<hr class="tt-div">');
  lines.push("<b>糖果题：</b>" + escHtml(candyLabel(e.candy)));
  if (e.candy_answer) lines.push("<span class='tt-muted'>原始回答：</span>" + escHtml(e.candy_answer));
  if (e.candy_error) lines.push("<span class='tt-muted'>调用错误：</span>" + escHtml(e.candy_error));
  lines.push('<hr class="tt-div">');
  lines.push("<b>指纹：</b>" + escHtml(fingerprintLabel(e.fingerprint)));
  if (e.prediction || e.prediction_name) lines.push("<span class='tt-muted'>指纹检测到：</span>" + escHtml(fingerprintPredictionText(e)));
  if (e.fingerprint_error) lines.push("<span class='tt-muted'>指纹错误：</span>" + escHtml(e.fingerprint_error));
  if (e.fingerprint_samples_error) lines.push("<span class='tt-muted'>样本错误：</span>" + escHtml(e.fingerprint_samples_error));
  var samples = e.fingerprint_samples || [];
  samples.forEach(function(s, i){
    lines.push("— 样本 #" + (i + 1) + (s.expected ? "（期望 " + s.expected + " 个）" : "") + (s.valid ? " · 有效 " + s.parsed + " 个" : " · 无效"));
    if (s.answer) lines.push("  回答：" + escHtml(s.answer));
    if (s.error) lines.push("  错误：" + escHtml(s.error));
  });
  if (e.skip_reason) lines.push("<span class='tt-muted'>跳过原因：</span>" + escHtml(e.skip_reason));
  if (e.detector_revision) lines.push('<hr class="tt-div"><span class="tt-muted">检测器版本：</span>' + escHtml(e.detector_revision));
  return lines.join("\n");
}

function runningTooltipHtml(a){
  var lines = [];
  lines.push("<b>测试中</b>");
  if (a.eval_stage) lines.push("<span class='tt-muted'>当前阶段：</span>" + escHtml(a.eval_stage));
  if (a.eval_started_at) lines.push("<span class='tt-muted'>开始时间：</span>" + escHtml(fmtTimeLocal(a.eval_started_at)));
  lines.push("<span class='tt-muted'>完成后自动刷新该账号的最近检测记录。</span>");
  return lines.join("\n");
}

function showRunningTooltip(ev, a){
  var t = evalTooltip();
  t.innerHTML = runningTooltipHtml(a || {});
  t.style.display = "block";
  moveEvalTooltip(ev);
}

function protTooltipHtml(p){
  var lines = [];
  lines.push("<b>" + escHtml(p.label || "保护") + "</b>" + (p.active ? "" : " · 未触发"));
  lines.push('<hr class="tt-div">');
  if (p.reason) lines.push("<span class='tt-muted'>触发原因：</span>" + escHtml(p.reason));
  if (p.started_at) lines.push("<span class='tt-muted'>触发时间：</span>" + escHtml(fmtTimeLocal(p.started_at)));
  if (p.duration) lines.push("<span class='tt-muted'>保护时长：</span>" + escHtml(p.duration));
  if (p.until) {
    var left = durUntil(p.until);
    lines.push("<span class='tt-muted'>预计结束：</span>" + escHtml(fmtTimeLocal(p.until)) + (left ? "（" + escHtml(left) + "）" : ""));
  }
  return lines.join("\n");
}

function showProtTooltip(ev, p){
  var t = evalTooltip();
  t.innerHTML = protTooltipHtml(p || {});
  t.style.display = "block";
  moveEvalTooltip(ev);
}

function renderEvalHistory(entries, total, accounts){
  var stats = document.getElementById("evalHistoryStats");
  var wrap = document.getElementById("evalHistory");
  var empty = document.getElementById("evalHistoryEmpty");
  wrap.innerHTML = "";

  var normal = 0, degraded = 0, error = 0;
  (accounts || []).forEach(function(a){
    var st = a && a.eval_status ? a.eval_status : "unknown";
    if (st === "normal") normal++;
    else if (st === "degraded") degraded++;
  });
  entries.forEach(function(e){
    if (evalHasError(e)) error++;
  });

  var byAcct = {};
  entries.forEach(function(e){
    var k = e.auth_id || "（未知账号）";
    if (!byAcct[k]) byAcct[k] = [];
    byAcct[k].push(e);
  });

  var meta = {};
  var order = [];
  (accounts || []).forEach(function(a){
    if (!a || !a.auth_id) return;
    meta[a.auth_id] = a;
    order.push(a.auth_id);
  });
  Object.keys(byAcct).forEach(function(k){
    if (order.indexOf(k) < 0) order.push(k);
  });

  var running = 0;
  (accounts || []).forEach(function(a){ if (a && a.eval_running) running++; });

  var maxKeep = (CFG && CFG.eval_history_max_entries) ? CFG.eval_history_max_entries : 500;
  stats.innerHTML = '<div class="stat-card"><div class="stat-num">' + total + '</div><div class="stat-label">历史总量</div></div>'
    + '<div class="stat-card"><div class="stat-num">' + order.length + '</div><div class="stat-label">账号数</div></div>'
    + '<div class="stat-card"><div class="stat-num">' + normal + '</div><div class="stat-label">正常</div></div>'
    + '<div class="stat-card"><div class="stat-num">' + degraded + '</div><div class="stat-label">降智</div></div>'
    + '<div class="stat-card"><div class="stat-num">' + error + '</div><div class="stat-label">调用失败</div></div>'
    + '<div class="stat-card"><div class="stat-num">' + running + '</div><div class="stat-label">测试中</div></div>'
    + '<div class="stat-card"><div class="stat-num">' + maxKeep + '</div><div class="stat-label">最大保留</div></div>'
    + '<div class="eval-legend">'
    + '滑窗为最近 5 次检测（左旧右新）：<b style="color:#16a34a">绿</b>=正常 · <b style="color:#dc2626">红</b>=错误（降智/调用失败） · <b style="color:#facc15">黄</b>=测试中 · <b style="color:#9aa1ad">灰</b>=未测试/未知。'
    + '悬停格子查看该次检测的原始回答或调用错误信息；悬停黄色格子查看当前测试阶段。'
    + '</div>';

  empty.style.display = order.length ? "none" : "block";

  order.forEach(function(id){
    var list = byAcct[id] || [];
    var last5 = list.slice(-5);
    var a = meta[id] || {};
    var name = a.email || shortAuth(id);
    var group = document.createElement("div");
    group.className = "eval-group";
    var head = '<div class="eval-group-head">'
      + '<span class="eval-group-title" title="' + escHtml(id) + '">' + escHtml(name) + '</span>'
      + '<span class="eval-group-count">' + list.length + ' 次检测</span>'
      + '</div>';
    var win = document.createElement("div");
    win.className = "eval-window";
    for (var i=0;i<5;i++){
      var e = last5[i] || null;
      var slot = document.createElement("span");
      slot.className = "eval-slot " + evalSlotClass(e) + (e ? " has-entry" : "");
      slot.title = evalSlotLabel(e);
      if (e) {
        slot._entry = e;
        slot.addEventListener("mouseenter", function(ev){ showEvalTooltip(ev, this._entry); });
        slot.addEventListener("mousemove", moveEvalTooltip);
        slot.addEventListener("mouseleave", hideEvalTooltip);
      }
      win.appendChild(slot);
    }
    if (a.eval_running) {
      var runningSlot = document.createElement("span");
      runningSlot.className = "eval-slot running has-entry";
      runningSlot.title = "测试中";
      runningSlot._running = a;
      runningSlot.addEventListener("mouseenter", function(ev){ showRunningTooltip(ev, this._running); });
      runningSlot.addEventListener("mousemove", moveEvalTooltip);
      runningSlot.addEventListener("mouseleave", hideEvalTooltip);
      win.appendChild(runningSlot);
    }
    var body = document.createElement("div");
    body.style.padding = "8px 14px 12px";
    body.appendChild(win);
    group.innerHTML = head;
    group.appendChild(body);
    wrap.appendChild(group);
  });
}

async function exportEvalHistoryCsv(){
  try {
    var r = await api("/eval/history/export");
    if (!r.ok) {
      var e = {};
      try { e = await r.json(); } catch(_) {}
      toast("导出失败: " + (e.error || r.status), true);
      return;
    }
    var blob = await r.blob();
    var url = URL.createObjectURL(blob);
    var a = document.createElement("a");
    a.href = url;
    a.download = "codex-guard-eval-history.csv";
    document.body.appendChild(a);
    a.click();
    a.remove();
    setTimeout(function(){ URL.revokeObjectURL(url); }, 1000);
    toast("CSV 已导出");
  } catch(e) { toast(e.message, true); }
}

async function clearEvalHistory(){
  if (!confirm("确定清空全部巡检历史吗？此操作不可撤销。")) return;
  try {
    var r = await api("/eval/history/clear", {method:"POST", headers:{"Content-Type":"application/json"}, body:"{}"});
    var d = await r.json();
    if (r.ok) { toast("巡检历史已清空"); await loadEvalHistory(); }
    else { toast("清空失败: " + JSON.stringify(d), true); }
  } catch(e) { toast(e.message, true); }
}

function buildSettingsForm(){
  renderFieldSections("settingsForm", function(f){ return f.section !== "巡检配置"; });
}

function buildEvalForm(){
  renderFieldSections("evalSettingsForm", function(f){ return f.section === "巡检配置"; });
}

function renderFieldSections(containerId, include){
  var wrap = document.getElementById(containerId);
  var bySection = {};
  FIELDS.forEach(function(f){
    if (!include(f)) return;
    if (!bySection[f.section]) bySection[f.section] = [];
    bySection[f.section].push(f);
  });
  var html = "";
  Object.keys(bySection).forEach(function(sec){
    var meta = SECTIONS[sec] || {};
    html += '<div class="card"><h3 class="card-title"><span class="dot"></span>' + (meta.icon ? meta.icon + ' ' : '') + sec + '</h3>';
    if (meta.desc) html += '<p class="section-desc">' + meta.desc + '</p>';
    html += '<div class="grid">';
    bySection[sec].forEach(function(f){
      var help = f.help ? '<span class="help" data-help="' + escHtml(f.help) + '" onclick="event.preventDefault();">?</span>' : '';
      html += '<label class="field';
      if (f.type === "bool") html += ' checkbox-row';
      html += '"><span class="field-label">' + escHtml(f.label) + help + '</span>';
      if (f.type === "bool") {
        html += '<input type="checkbox" data-key="' + f.key + '" data-type="bool">';
      } else if (f.type === "select") {
        html += '<select data-key="' + f.key + '" data-type="text">';
        f.options.forEach(function(o){ html += '<option value="' + o[0] + '">' + o[1] + '</option>'; });
        html += '</select>';
      } else if (f.type === "text") {
        html += '<input type="text" data-key="' + f.key + '" data-type="text">';
      } else {
        html += '<input type="number" step="' + (f.step || "1") + '" data-key="' + f.key + '" data-type="number">';
      }
      html += '</label>';
    });
    html += '</div></div>';
  });
  wrap.innerHTML = html;
}

function fillSettingsForm(cfg){
  document.querySelectorAll("#settingsForm [data-key], #evalSettingsForm [data-key]").forEach(function(el){
    var key = el.getAttribute("data-key");
    var type = el.getAttribute("data-type");
    var v = cfg[key];
    if (type === "bool") { el.checked = !!v; }
    else if (el.tagName === "SELECT") {
      el.value = v;
      if (el.value !== v && v != null) {
        var o = document.createElement("option");
        o.value = v; o.textContent = v;
        el.appendChild(o); el.value = v;
      }
    }
    else if (type === "text") { el.value = (v == null) ? "" : v; }
    else { el.value = (v == null || v === "") ? 0 : v; }
  });
  accountsList = cfg.accounts || [];
  renderAccounts();
  overloadRules = cfg.overload_rules || [];
  renderOverloadRules();
}

async function loadEvalModels(){
  var sel = document.querySelector('#evalSettingsForm [data-key="eval_model"]');
  if (!sel) return;
  var current = (CFG && CFG.eval_model != null) ? String(CFG.eval_model) : sel.value;
  var models = [];
  try {
    var r = await api("/models");
    if (r.ok) {
      var d = await r.json();
      if (Array.isArray(d.models)) models = d.models;
    }
  } catch(e) {}
  sel.innerHTML = "";
  var auto = document.createElement("option");
  auto.value = ""; auto.textContent = "自动（使用账号配置模型）";
  sel.appendChild(auto);
  models.forEach(function(m){
    var o = document.createElement("option");
    o.value = m; o.textContent = m;
    sel.appendChild(o);
  });
  if (current && !models.includes(current)) {
    var o = document.createElement("option");
    o.value = current; o.textContent = current;
    sel.appendChild(o);
  }
  sel.value = current;
}

function readSettingsForm(){
  var cfg = {};
  document.querySelectorAll("#settingsForm [data-key], #evalSettingsForm [data-key]").forEach(function(el){
    var key = el.getAttribute("data-key");
    var type = el.getAttribute("data-type");
    if (type === "bool") { cfg[key] = el.checked; }
    else if (type === "text") { cfg[key] = el.value; }
    else { var n = parseFloat(el.value); cfg[key] = isNaN(n) ? 0 : n; }
  });
  return cfg;
}

var overloadRules = [];
var overloadRuleSeq = 0;

function overloadRuleRowHtml(rule, idx){
  var metric = (rule && rule.metric) || "quota_percent";
  var ws = (rule && rule.window_seconds != null) ? rule.window_seconds : 3600;
  var limit = (rule && rule.limit != null) ? rule.limit : 10;
  var stop = (rule && rule.stop_seconds != null) ? rule.stop_seconds : 600;
  var rid = (rule && rule.id) ? rule.id : ("rule" + (idx + 1));
  var opts = ["quota_percent","token"].map(function(m){
    var label = m === "token" ? "令牌（token）" : "额度百分比（quota_percent）";
    return '<option value="' + m + '"' + (m === metric ? ' selected' : '') + '>' + label + '</option>';
  }).join("");
  return '<div class="rule-row" data-idx="' + idx + '">'
    + '<label class="field"><span class="field-label">指标<span class="help" data-help="监控哪类消耗：额度百分比＝窗口内剩余额度下降幅度超过阈值；令牌＝窗口内累计消耗的 token 总量超过阈值。" onclick="event.preventDefault();">?</span></span><select class="cell-select" data-f="metric">' + opts + '</select></label>'
    + '<label class="field"><span class="field-label">窗口（秒）<span class="help" data-help="统计时间窗口长度。只有该窗口内发生的事件参与判断。" onclick="event.preventDefault();">?</span></span><input class="cell-input" type="number" step="1" data-f="window_seconds" value="' + ws + '"></label>'
    + '<label class="field"><span class="field-label">阈值<span class="help" data-help="触发阈值。额度百分比＝下降的百分点；令牌＝token 总数。超过该值即触发。" onclick="event.preventDefault();">?</span></span><input class="cell-input" type="number" step="any" data-f="limit" value="' + limit + '"></label>'
    + '<label class="field"><span class="field-label">停用（秒）<span class="help" data-help="触发后停用该账号的时长，期间不参与调度。" onclick="event.preventDefault();">?</span></span><input class="cell-input" type="number" step="1" data-f="stop_seconds" value="' + stop + '"></label>'
    + '<label class="field"><span class="field-label">ID<span class="help" data-help="规则标识，仅用于阅读与日志区分。" onclick="event.preventDefault();">?</span></span><input class="cell-input" type="text" data-f="id" value="' + escHtml(rid) + '"></label>'
    + '<button class="btn btn-sm btn-danger" type="button" onclick="removeOverloadRule(' + idx + ')">删除</button>'
    + '</div>';
}

function renderOverloadRules(){
  var wrap = document.getElementById("overloadRules");
  var head = '<div class="rule-head"><span>指标</span><span>窗口（秒）</span><span>阈值</span><span>停用（秒）</span><span>ID</span><span></span></div>';
  overloadRuleSeq = 0;
  var list = overloadRules || [];
  if (!list.length) {
    wrap.innerHTML = head + '<div class="empty">暂无规则，点击“添加规则”创建第一条。</div>';
    return;
  }
  var html = head;
  list.forEach(function(r){ html += overloadRuleRowHtml(r, overloadRuleSeq++); });
  wrap.innerHTML = html;
}

function readOverloadRules(){
  var out = [];
  document.querySelectorAll("#overloadRules .rule-row").forEach(function(row){
    var q = function(f){ return row.querySelector('[data-f="' + f + '"]'); };
    var ws = parseFloat(q("window_seconds").value);
    var limit = parseFloat(q("limit").value);
    var stop = parseInt(q("stop_seconds").value, 10);
    var rid = q("id").value.trim() || ("rule" + (out.length + 1));
    out.push({
      id: rid,
      metric: q("metric").value,
      window_seconds: isNaN(ws) ? 0 : ws,
      limit: isNaN(limit) ? 0 : limit,
      stop_seconds: isNaN(stop) ? 0 : stop
    });
  });
  return out;
}

function syncOverloadRules(){
  overloadRules = readOverloadRules();
}

function addOverloadRule(){
  syncOverloadRules();
  overloadRules.push({id:"rule" + (overloadRules.length + 1), metric:"quota_percent", window_seconds:3600, limit:10, stop_seconds:600});
  renderOverloadRules();
}

function removeOverloadRule(idx){
  syncOverloadRules();
  overloadRules.splice(idx, 1);
  renderOverloadRules();
}

var accountsList = [];
var accountRowSeq = 0;

function accountRowHtml(ac, idx){
  ac = ac || {};
  var id = ac.id || "";
  var enabled = ac.enabled == null ? "" : (ac.enabled ? "true" : "false");
  var role = ac.role || "";
  var weightSet = ac.weight != null;
  var weight = weightSet ? ac.weight : 1;
  var mpSet = ac.max_parallel != null;
  var mp = mpSet ? ac.max_parallel : 5;
  var wsSet = ac.window_seconds != null;
  var ws = wsSet ? ac.window_seconds : 18000;
  var exp = ac.expires_at || "";
  var rules = ac.overload_rules ? JSON.stringify(ac.overload_rules, null, 2) : "";

  var enOpts = [["","继承默认（启用）"],["true","启用"],["false","禁用"]].map(function(o){
    return '<option value="' + o[0] + '"' + (enabled === o[0] ? ' selected' : '') + '>' + o[1] + '</option>';
  }).join("");
  var roleOpts = [["","继承（主 primary）"],["primary","主（primary）"],["backup","备用（backup）"],["degraded","降智（degraded）"]].map(function(o){
    return '<option value="' + o[0] + '"' + (role === o[0] ? ' selected' : '') + '>' + o[1] + '</option>';
  }).join("");

  var html = '<div class="acct-row" data-aidx="' + idx + '">';
  html += '<div class="acct-head"><b>账号 #' + (idx + 1) + '</b><button class="btn btn-sm btn-danger" type="button" onclick="removeAccountRow(' + idx + ')">删除</button></div>';
  html += '<div class="acct-grid">';
  html += '<label class="field"><span class="field-label">账号 ID<span class="help" data-help="必须与认证文件中的账号 ID（auth_id）一致，是唯一匹配键。" onclick="event.preventDefault();">?</span></span><input class="cell-input" type="text" data-a="id" value="' + escHtml(id) + '"></label>';
  html += '<label class="field"><span class="field-label">启用<span class="help" data-help="继承默认＝使用全局默认（启用）；也可为单个账号单独启用或禁用。" onclick="event.preventDefault();">?</span></span><select class="cell-select" data-a="enabled">' + enOpts + '</select></label>';
  html += '<label class="field"><span class="field-label">角色<span class="help" data-help="账号定位/状态：主账号优先调度，备用账号在主账号不可用时顶上，降智账号不参与调度、只做智能检测（可自动恢复）。" onclick="event.preventDefault();">?</span></span><select class="cell-select" data-a="role">' + roleOpts + '</select></label>';
  html += '<label class="field"><span class="field-label">权重<span class="help" data-help="调度权重，越大越优先。勾选后自定义，未勾选继承全局默认 1。" onclick="event.preventDefault();">?</span></span><div class="acct-num"><input type="checkbox" class="cell-check" data-a="weight_set"' + (weightSet ? ' checked' : '') + ' onchange="toggleAcctNum(this)"><input class="cell-input" type="number" step="any" data-a="weight" value="' + weight + '"' + (weightSet ? '' : ' disabled') + '></div></label>';
  html += '<label class="field"><span class="field-label">最大并行<span class="help" data-help="该账号允许的最大并发请求数。勾选后自定义，未勾选继承全局默认。" onclick="event.preventDefault();">?</span></span><div class="acct-num"><input type="checkbox" class="cell-check" data-a="max_parallel_set"' + (mpSet ? ' checked' : '') + ' onchange="toggleAcctNum(this)"><input class="cell-input" type="number" step="1" data-a="max_parallel" value="' + mp + '"' + (mpSet ? '' : ' disabled') + '></div></label>';
  html += '<label class="field"><span class="field-label">额度窗口（秒）<span class="help" data-help="该账号额度重置周期的长度（如 5 小时=18000、周=604800）。勾选后自定义，用于额度端点未返回重置时间时兜底。" onclick="event.preventDefault();">?</span></span><div class="acct-num"><input type="checkbox" class="cell-check" data-a="window_seconds_set"' + (wsSet ? ' checked' : '') + ' onchange="toggleAcctNum(this)"><input class="cell-input" type="number" step="1" data-a="window_seconds" value="' + ws + '"' + (wsSet ? '' : ' disabled') + '></div></label>';
  html += '<label class="field"><span class="field-label">到期时间<span class="help" data-help="可选 RFC3339 时间，如 2026-12-31T23:59:59Z，留空表示不设到期。" onclick="event.preventDefault();">?</span></span><input class="cell-input" type="text" data-a="expires_at" placeholder="2026-12-31T23:59:59Z" value="' + escHtml(exp) + '"></label>';
  html += '</div>';
  html += '<details class="acct-advanced"><summary>该账号的过载规则（可选，留空继承全局）</summary>';
  html += '<textarea class="textarea" style="min-height:120px" data-a="overload_rules" placeholder=\'[{"id":"rule1","metric":"quota_percent","window_seconds":3600,"limit":10,"stop_seconds":600}]\'>' + escHtml(rules) + '</textarea>';
  html += '</details>';
  html += '</div>';
  return html;
}

function renderAccounts(){
  var wrap = document.getElementById("accountsEditor");
  accountRowSeq = 0;
  var list = accountsList || [];
  if (!list.length) { wrap.innerHTML = '<div class="empty">暂无账号，点击“添加账号”创建第一条。</div>'; return; }
  var html = "";
  list.forEach(function(ac){ html += accountRowHtml(ac, accountRowSeq++); });
  wrap.innerHTML = html;
}

function toggleAcctNum(el){
  var wrap = el.parentElement;
  var input = wrap.querySelector('input[type=number]');
  input.disabled = !el.checked;
}

function readAccountsRows(skipInvalid){
  var out = [];
  var rows = document.querySelectorAll("#accountsEditor .acct-row");
  rows.forEach(function(row){
    var g = function(f){ return row.querySelector('[data-a="' + f + '"]'); };
    var id = g("id").value.trim();
    if (!id) {
      if (skipInvalid) { out.push({id:""}); return; }
      throw new Error("第 " + (out.length + 1) + " 个账号缺少 ID");
    }
    var ac = {id:id};
    var en = g("enabled").value;
    if (en === "true") ac.enabled = true;
    else if (en === "false") ac.enabled = false;
    var role = g("role").value;
    if (role) ac.role = role;
    if (g("weight_set").checked) { var w = parseFloat(g("weight").value); ac.weight = isNaN(w) ? 0 : w; }
    if (g("max_parallel_set").checked) { var m = parseInt(g("max_parallel").value, 10); ac.max_parallel = isNaN(m) ? 0 : m; }
    if (g("window_seconds_set").checked) { var s = parseInt(g("window_seconds").value, 10); ac.window_seconds = isNaN(s) ? 0 : s; }
    var exp = g("expires_at").value.trim();
    if (exp) ac.expires_at = exp;
    var rulesTxt = g("overload_rules").value.trim();
    if (rulesTxt) {
      try {
        var v = JSON.parse(rulesTxt);
        if (!Array.isArray(v)) throw new Error("应为数组");
        ac.overload_rules = v;
      } catch(e) { throw new Error("账号 " + id + " 的过载规则不是合法 JSON: " + e.message); }
    }
    out.push(ac);
  });
  return out;
}

function readAccounts(){
  return readAccountsRows(false);
}

function syncAccounts(){
  accountsList = readAccountsRows(true);
}

function addAccountRow(){
  syncAccounts();
  accountsList.push({id:""});
  renderAccounts();
}

function removeAccountRow(idx){
  syncAccounts();
  accountsList.splice(idx, 1);
  renderAccounts();
}

async function loadSettings(){
  var r = await api("/settings");
  var d = await r.json();
  CFG = d;
  fillSettingsForm(CFG);
  await loadEvalModels();
}

async function saveSettingsForm(){
  await persistSettingsForm();
}

async function saveEvalForm(){
  await persistSettingsForm();
}

async function persistSettingsForm(){
  try {
    var cfg = readSettingsForm();
    cfg.accounts = readAccounts();
    cfg.overload_rules = readOverloadRules();
    var r = await api("/settings", {method:"PUT", headers:{"Content-Type":"application/json"}, body: JSON.stringify(cfg)});
    var d = await r.json();
    if (r.ok) {
      CFG = d;
      toast("参数已保存并生效");
      await loadStatus();
    } else {
      toast("保存失败: " + JSON.stringify(d), true);
    }
  } catch(e) { toast(e.message, true); }
}

function switchRawFormat(fmt){
  rawFormat = fmt;
  document.getElementById("jsonTabBtn").className = "btn " + (fmt === "json" ? "btn-primary" : "btn-light");
  document.getElementById("yamlTabBtn").className = "btn " + (fmt === "yaml" ? "btn-primary" : "btn-light");
  loadRaw();
}

async function loadRaw(){
  try {
    if (rawFormat === "yaml") {
      var r = await api("/settings?format=yaml");
      document.getElementById("rawArea").value = await r.text();
    } else {
      if (!CFG) { await loadSettings(); }
      document.getElementById("rawArea").value = JSON.stringify(CFG, null, 2);
    }
  } catch(e) { toast(e.message, true); }
}

async function saveRaw(){
  try {
    var body = document.getElementById("rawArea").value;
    var r = await api("/settings", {method:"PUT", headers:{"Content-Type": rawFormat === "yaml" ? "text/yaml" : "application/json"}, body: body});
    var d = await r.json();
    if (r.ok) {
      CFG = d;
      toast("配置已保存并生效");
      await loadStatus();
      fillSettingsForm(CFG);
    } else {
      toast("保存失败: " + JSON.stringify(d), true);
    }
  } catch(e) { toast(e.message, true); }
}

async function loadAll(){
  try {
    buildSettingsForm();
    buildEvalForm();
    await loadStatus();
    await loadSettings();
    hideLogin();
  } catch(e) {
    showLogin(e.message);
  }
}

initAutoRefresh();
if (KEY) { loadAll(); } else { showLogin(); }
</script>
</html>
`
