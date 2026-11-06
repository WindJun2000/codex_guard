package main

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

// ---------------------------------------------------------------------------
// usage.handle — primary error-handling entry point. UsageRecord carries the
// selected credential (AuthID) plus the failure body, so policy decisions
// (cooldown / escalation / circuit-break / disable) are attributed correctly.
// ---------------------------------------------------------------------------

type usageService struct{}

func (usageService) HandleUsage(ctx context.Context, record pluginapi.UsageRecord) {
	if record.Provider != "" && record.Provider != activeConfig().Provider {
		return
	}
	authID := record.AuthID
	if authID == "" {
		authID = record.AuthIndex
	}
	if authID == "" {
		return
	}

	if !record.Failed {
		recordSuccess(authID)
		recordTokenUsage(authID, float64(record.Detail.TotalTokens), time.Now())
		maybeApplyOverload(authID, time.Now())
		return
	}

	c := classifyError(record.Failure.StatusCode, record.Failure.Body)
	applyFailurePolicy(authID, record.Failure.StatusCode, c)
}

// applyFailurePolicy maps a classified error to a policy action. It is the single
// source for failure handling (usage.handle only — request.complete must not
// re-apply it to avoid double-counting).
func applyFailurePolicy(authID string, statusCode int, c classified) {
	cfg := activeConfig()
	switch c.Kind {
	case ErrorRateLimit:
		secs := c.RetryAfterSecs
		if secs <= 0 {
			secs = cfg.CooldownDefaultSeconds
		}
		applyCooldown(authID, secs)
	case ErrorOverloaded:
		secs := c.RetryAfterSecs
		if secs <= 0 {
			secs = cfg.CooldownDefaultSeconds
		}
		applyCooldown(authID, secs+cfg.CooldownFixedBonusSeconds)
	case ErrorAuth:
		if cfg.AutoDisableOnAuthFailure {
			disableAccount(authID)
		} else {
			applyCooldown(authID, cfg.AuthFailureCooldownSeconds)
		}
	case ErrorServer, ErrorUnknown:
		recordFailure(authID)
	}
	logf("warn", "usage.handle auth=%s kind=%s status=%d reason=%s", authID, kindName(c.Kind), statusCode, c.Reason)
}

func handleUsageHandle(raw []byte) ([]byte, error) {
	var record pluginapi.UsageRecord
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &record); err != nil {
			return nil, err
		}
	}
	usageService{}.HandleUsage(context.Background(), record)
	return okEnvelope(struct{}{}), nil
}

// ---------------------------------------------------------------------------
// request.complete — terminal per-request events. Releases the in-flight slot
// for every outcome and clears state on success. Failure policy is NOT applied
// here (usage.handle already owns it).
// ---------------------------------------------------------------------------

type lifecycleService struct{}

func (lifecycleService) HandleRequestComplete(ctx context.Context, completion pluginapi.RequestCompletion) error {
	if completion.RequestID == "" {
		return nil
	}
	selectionID := metadataString(completion.Metadata, "selection_id")
	authID := metadataString(completion.Metadata, "selected_auth_id")
	if authID == "" {
		authID = metadataString(completion.Metadata, "auth_id")
	}
	if authID != "" {
		releaseSlot(authID, selectionID)
	}
	switch completion.Outcome {
	case pluginapi.RequestCompletionSucceeded:
		if authID != "" {
			recordSuccess(authID)
		}
	case pluginapi.RequestCompletionRejected, pluginapi.RequestCompletionCanceled:
		// Nothing beyond slot release: the request never reached upstream.
	}
	return nil
}

func handleRequestComplete(raw []byte) ([]byte, error) {
	var completion pluginapi.RequestCompletion
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &completion); err != nil {
			return nil, err
		}
	}
	_ = lifecycleService{}.HandleRequestComplete(context.Background(), completion)
	return okEnvelope(struct{}{}), nil
}

// ---------------------------------------------------------------------------
// stream chunk — read-only observer. Extracts upstream rate-limit headers so
// quota state stays fresh as a side effect of real traffic.
// ---------------------------------------------------------------------------

type streamObserver struct{}

func (streamObserver) InterceptStreamChunk(ctx context.Context, req pluginapi.StreamChunkInterceptRequest) (pluginapi.StreamChunkInterceptResponse, error) {
	authID := metadataString(req.Metadata, "selected_auth_id")
	if authID == "" {
		authID = metadataString(req.Metadata, "auth_id")
	}
	if authID != "" {
		if q, ok := quotaFromHeaders(req.ResponseHeaders); ok {
			updateQuota(authID, q)
		}
	}
	return pluginapi.StreamChunkInterceptResponse{}, nil
}

func handleStreamChunk(raw []byte) ([]byte, error) {
	var req pluginapi.StreamChunkInterceptRequest
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &req); err != nil {
			return okEnvelope(pluginapi.StreamChunkInterceptResponse{}), nil
		}
	}
	resp, _ := streamObserver{}.InterceptStreamChunk(context.Background(), req)
	return okEnvelope(resp), nil
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func kindName(k ErrorKind) string {
	switch k {
	case ErrorRateLimit:
		return "rate_limit"
	case ErrorOverloaded:
		return "overloaded"
	case ErrorAuth:
		return "auth"
	case ErrorServer:
		return "server"
	default:
		return "unknown"
	}
}

func quotaFromHeaders(h http.Header) (QuotaSnapshot, bool) {
	if h == nil {
		return QuotaSnapshot{}, false
	}
	q := QuotaSnapshot{}
	found := false
	for k, vals := range h {
		key := strings.ToLower(k)
		switch {
		case key == "x-codex-remaining-percent" || key == "x-codex-quota-remaining" || key == "x-ratelimit-remaining":
			if len(vals) > 0 {
				if pct, err := strconv.ParseFloat(strings.TrimSpace(vals[0]), 64); err == nil {
					q.RemainingPercent = pct
					found = true
				}
			}
		case key == "x-codex-reset-at" || key == "x-ratelimit-reset" || key == "x-codex-quota-resets-at":
			if len(vals) > 0 {
				if t, err := time.Parse(time.RFC3339, strings.TrimSpace(vals[0])); err == nil {
					q.ResetAt = t
					found = true
				}
			}
		}
	}
	return q, found
}

func metadataString(metadata map[string]any, key string) string {
	if metadata == nil {
		return ""
	}
	v, ok := metadata[key]
	if !ok {
		return ""
	}
	switch s := v.(type) {
	case string:
		return strings.TrimSpace(s)
	default:
		return ""
	}
}

func redact(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
