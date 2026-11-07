package main

import (
	"encoding/json"
	"math/rand"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// ErrorKind classifies an upstream failure into a policy action.
type ErrorKind int

const (
	// ErrorUnknown is an unrecognized failure (counts toward circuit breaker).
	ErrorUnknown ErrorKind = iota
	// ErrorRateLimit is 429 — quota/rate exhausted, cool down by retry-after.
	ErrorRateLimit
	// ErrorOverloaded is 529 — upstream overload, cool down by retry-after.
	ErrorOverloaded
	// ErrorAuth is 401/403 — credential invalid or forbidden, disable account.
	ErrorAuth
	// ErrorServer is 500 without a retry hint — transient upstream, circuit-breaker count.
	ErrorServer
	// ErrorTransport is a pre-HTTP network failure (DNS/TLS/dial/reset/EOF) that
	// never reached a usable response and is safe to retry.
	ErrorTransport
	// ErrorRetryableServer is an idempotent upstream 408/502/503/504 (or 500 with
	// OpenAI's explicit retry hint) — safe to retry and must not trip the breaker.
	ErrorRetryableServer
	// ErrorClientFault is a request-scoped 4xx: retrying the same input cannot
	// succeed, so it must neither retry nor discipline the account.
	ErrorClientFault
	// ErrorSuccess is a non-error (should not reach classification).
	ErrorSuccess
)

// classified is the result of parsing an upstream error.
type classified struct {
	Kind           ErrorKind
	RetryAfterSecs int
	Reason         string
	Retryable      bool
	Overload       bool
}

// upstreamErrorEnvelope carries the structured fields an upstream body may expose at
// either the root, under "error", or under "body.error".
type upstreamErrorEnvelope struct {
	Type            string
	Code            string
	Message         string
	ResetsInSeconds int64
	ResetsAt        int64
	ResetSeconds    int64
}

// retryAfterRe matches retry-after / retry-after-ms fields in error bodies.
var (
	retryAfterRe   = regexp.MustCompile(`(?i)"retry-after"\s*:\s*"?(\d+)"?`)
	retryAfterMsRe = regexp.MustCompile(`(?i)"retry-after-ms"\s*:\s*"?(\d+)"?`)
)

// classifyError turns an HTTP status + error string into a policy decision.
// Order matters:
//  1. stream truncation (generation already submitted) is terminal — never replay.
//  2. overload is detected from the body and wins over any status code, because
//     OpenAI/Codex often surface server_is_overloaded under 502/503 instead of 529.
//  3. pre-HTTP transport failures carry no status and are safe to retry.
//  4. status + body refinement handle the rest.
func classifyError(statusCode int, errStr string) classified {
	if isStreamTruncation(errStr) {
		return classified{Kind: ErrorClientFault, Reason: "stream truncated (generation already submitted)"}
	}
	if overload, reason := overloadFromBody(errStr); overload {
		c := classified{Kind: ErrorOverloaded, Reason: reason, Retryable: false, Overload: true}
		if secs := extractRetryAfter(errStr, time.Now()); secs > 0 {
			c.RetryAfterSecs = secs
		} else {
			c.RetryAfterSecs = activeConfig().CooldownDefaultSeconds
		}
		return c
	}
	if statusCode == 0 && isTransportError(errStr) {
		return classified{Kind: ErrorTransport, Reason: "network transport error", Retryable: true}
	}

	kind, reason := classifyByStatus(statusCode, errStr)
	env := parseErrorEnvelope(errStr)

	// Structured type/code wins over the plain status code, except that a
	// 401/403 must never be downgraded to a quota cooldown by a body field.
	refined := false
	if k, r, ok := classifyByInfo(env, statusCode); ok && shouldRefineBody(kind, k, statusCode) {
		kind, reason = k, r
		refined = true
	}

	// A textual body can still refine the status code (e.g. 500 + insufficient_quota).
	if !refined {
		if k, r := classifyByBody(errStr); k != ErrorUnknown && shouldRefineBody(kind, k, statusCode) {
			kind, reason = k, r
		}
	}

	c := classified{Kind: kind, Reason: reason, Retryable: kindRetryable(kind, errStr)}

	if isWebsocketConnectionLimit(errStr) {
		// CPA's contract is to open a fresh WebSocket immediately.
		c.RetryAfterSecs = 0
	} else if secs := extractRetryAfter(errStr, time.Now()); secs > 0 {
		c.RetryAfterSecs = secs
	} else if kind == ErrorOverloaded || kind == ErrorRateLimit {
		c.RetryAfterSecs = activeConfig().CooldownDefaultSeconds
	} else if c.Retryable {
		// Retryable transport/server errors without an upstream Retry-After
		// fall back to the configured backoff base (attempt 0).
		c.RetryAfterSecs = retryBackoffSeconds(0)
	}

	return c
}

func classifyByStatus(statusCode int, errStr string) (ErrorKind, string) {
	switch statusCode {
	case 401, 403:
		return ErrorAuth, "authentication/authorization rejected"
	case 408:
		return ErrorRetryableServer, "request timeout (retryable)"
	case 429:
		return ErrorRateLimit, "rate limit / quota exhausted"
	case 529:
		return ErrorOverloaded, "upstream overloaded"
	case 500:
		if isOpenAIRetryHint(errStr) {
			return ErrorRetryableServer, "server error with retry hint"
		}
		return ErrorServer, "upstream server error"
	case 502, 503, 504:
		return ErrorRetryableServer, "transient gateway/server error (retryable)"
	case 400, 404, 409, 413, 422:
		return ErrorClientFault, "client request error"
	default:
		return ErrorUnknown, "unrecognized failure"
	}
}

// classifyByInfo maps structured error.type / error.code fields to a policy
// decision. It returns ok=false when the body carries no recognized field.
func classifyByInfo(env upstreamErrorEnvelope, statusCode int) (ErrorKind, string, bool) {
	code := strings.ToLower(strings.TrimSpace(env.Code))
	typ := strings.ToLower(strings.TrimSpace(env.Type))

	switch {
	case code == "websocket_connection_limit_reached" || typ == "websocket_connection_limit_reached":
		return ErrorRetryableServer, "websocket connection limit reached (retry immediately)", true
	case code == "model_cooldown":
		return ErrorRateLimit, "model cooling down", true
	case code == "slow_down":
		return ErrorRateLimit, "slow_down transient rate limit", true
	case code == "usage_limit_reached" || typ == "usage_limit_reached":
		return ErrorRateLimit, "usage limit reached (quota exhausted)", true
	case code == "insufficient_quota" || typ == "insufficient_quota":
		return ErrorRateLimit, "insufficient quota (billing/limit)", true
	case code == "credit_balance_exhausted":
		return ErrorRateLimit, "credit balance exhausted", true
	case code == "organization_spend_limit_exceeded":
		return ErrorRateLimit, "organization spend limit exceeded", true
	case code == "project_spend_limit_exceeded":
		return ErrorRateLimit, "project spend limit exceeded", true
	case code == "organization_usage_limit_exceeded":
		return ErrorRateLimit, "organization usage limit exceeded", true
	case code == "context_length_exceeded" || code == "context_too_large":
		return ErrorClientFault, "context length exceeded", true
	case code == "thinking_signature_invalid":
		return ErrorClientFault, "thinking signature invalid", true
	case code == "model_not_found" || typ == "not_found_error":
		return ErrorClientFault, "model not found", true
	case code == "invalid_api_key" || typ == "authentication_error":
		return ErrorAuth, "authentication rejected", true
	case typ == "permission_error" || code == "permission_denied":
		return ErrorAuth, "authorization rejected", true
	}
	return ErrorUnknown, "", false
}

// shouldRefineBody reports whether a body-derived candidate may override the
// status-derived kind. Auth statuses are pinned so a 403 carrying
// insufficient_quota is never reinterpreted as a quota cooldown.
func shouldRefineBody(base ErrorKind, candidate ErrorKind, statusCode int) bool {
	if base == ErrorAuth && (statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden) {
		return false
	}
	return true
}

// classifyByBody inspects the error string for provider-specific codes.
func classifyByBody(body string) (ErrorKind, string) {
	lower := strings.ToLower(body)
	switch {
	case strings.Contains(lower, "overloaded") ||
		strings.Contains(lower, "server_is_overloaded") ||
		strings.Contains(lower, "service_unavailable"):
		return ErrorOverloaded, "upstream overloaded"
	case strings.Contains(lower, "rate_limit") ||
		strings.Contains(lower, "rate limit") ||
		strings.Contains(lower, "too many requests") ||
		strings.Contains(lower, "insufficient_quota") ||
		strings.Contains(lower, "usage_limit_reached"):
		return ErrorRateLimit, "rate limit / quota exhausted"
	case strings.Contains(lower, "invalid_api_key") ||
		strings.Contains(lower, "invalid_grant") ||
		strings.Contains(lower, "unauthorized") ||
		strings.Contains(lower, "authentication_error") ||
		strings.Contains(lower, "invalid_request_error") && strings.Contains(lower, "key"):
		return ErrorAuth, "authentication rejected"
	case strings.Contains(lower, "permission") ||
		strings.Contains(lower, "forbidden") ||
		strings.Contains(lower, "access denied"):
		return ErrorAuth, "authorization rejected"
	}
	return ErrorUnknown, ""
}

// kindRetryable reports whether a classification may be retried by CPA.
func kindRetryable(kind ErrorKind, body string) bool {
	switch kind {
	case ErrorTransport, ErrorRetryableServer:
		return true
	case ErrorRateLimit:
		return isTransientRateLimitBody(body)
	default:
		return false
	}
}

// isStreamTruncation reports locally synthesized failures for accepted streams
// that ended without a terminal frame. The generation was already submitted
// upstream, so replaying it would double-bill and duplicate output.
func isStreamTruncation(body string) bool {
	lower := strings.ToLower(body)
	return strings.Contains(lower, "stream disconnected before completion") ||
		strings.Contains(lower, "stream closed before response.completed") ||
		strings.Contains(lower, "incomplete empty response (0 tokens)")
}

// overloadFromBody detects upstream overload from the body, independent of the
// HTTP status code. OpenAI/Codex surface server_is_overloaded under 503 (and,
// via CPA's terminal-status fallback, under 502) rather than a dedicated 529.
func overloadFromBody(body string) (bool, string) {
	lower := strings.ToLower(body)
	switch {
	case strings.Contains(lower, "server_is_overloaded"),
		strings.Contains(lower, "service_unavailable"),
		strings.Contains(lower, "overloaded"):
		return true, "upstream overloaded"
	}
	return false, ""
}

// isTransportError reports pre-HTTP dial/TLS/DNS/reset failures by message. These
// carry no HTTP status code because no usable response was received, so the
// request almost certainly was not accepted or billed.
func isTransportError(body string) bool {
	lower := strings.ToLower(strings.TrimSpace(body))
	if lower == "" {
		return false
	}
	switch {
	case strings.Contains(lower, "tls handshake"),
		strings.Contains(lower, "connection refused"),
		strings.Contains(lower, "connection reset"),
		strings.Contains(lower, "connection aborted"),
		strings.Contains(lower, "i/o timeout"),
		strings.Contains(lower, "connection timed out"),
		strings.Contains(lower, "connect: connection timed out"),
		strings.Contains(lower, "no such host"),
		strings.Contains(lower, "network is unreachable"),
		strings.Contains(lower, "no route to host"),
		strings.Contains(lower, "broken pipe"),
		strings.Contains(lower, "unexpected eof"),
		strings.Contains(lower, "use of closed network connection"),
		strings.Contains(lower, "http2: client connection force closed"),
		strings.Contains(lower, "http2: client conn is closed"),
		strings.Contains(lower, "a connection attempt failed"),
		strings.Contains(lower, "wsarecv"),
		strings.Contains(lower, "wsasend"):
		return true
	}
	return false
}

// isTransientRateLimitBody reports a per-model admission 429 (e.g. Codex
// turn_rate_limit_exceeded) whose quota is NOT exhausted and which can be retried
// after Retry-After.
func isTransientRateLimitBody(body string) bool {
	env := parseErrorEnvelope(body)
	code := strings.ToLower(strings.TrimSpace(env.Code))
	typ := strings.ToLower(strings.TrimSpace(env.Type))
	switch code {
	case "slow_down", "turn_rate_limit_exceeded", "model_at_capacity", "model_is_at_capacity":
		return true
	case "usage_limit_reached", "model_cooldown", "credit_balance_exhausted",
		"organization_spend_limit_exceeded", "project_spend_limit_exceeded",
		"organization_usage_limit_exceeded", "insufficient_quota":
		return false
	}
	if typ == "usage_limit_reached" {
		return false
	}
	lower := strings.ToLower(body)
	return strings.Contains(lower, "turn_rate_limit_exceeded") ||
		strings.Contains(lower, "at capacity") ||
		strings.Contains(lower, "selected model is at capacity")
}

// isOpenAIRetryHint reports OpenAI's explicit safe-to-retry server error, which
// pairs the server_error type with a "you can retry your request" message.
func isOpenAIRetryHint(body string) bool {
	lower := strings.ToLower(body)
	return strings.Contains(lower, "server_error") && strings.Contains(lower, "you can retry your request")
}

// retryBackoffSeconds returns the recommended delay for a retryable failure when
// the upstream response carries no Retry-After hint. attempt is zero-based (the
// first replay is attempt 0), so the delay doubles each replay up to the
// configured cap: base, base*2, base*4, ... capped at max.
//
// The plugin itself has no HTTP executor, so this value is a policy hint that
// mirrors the retry budget exposed in the settings UI. CPA's own executeHome
// loop performs the actual replay; this keeps the plugin's accounting and the
// user-visible retry budget consistent.
func retryBackoffSeconds(attempt int) int {
	if attempt < 0 {
		attempt = 0
	}
	cfg := activeConfig()
	base := cfg.RetryBackoffBaseSeconds
	if base <= 0 {
		base = 1
	}
	max := cfg.RetryBackoffMaxSeconds
	if max < base {
		max = base
	}
	secs := base
	for i := 0; i < attempt; i++ {
		if secs >= max {
			break
		}
		secs *= 2
		if secs > max {
			secs = max
		}
	}
	if cfg.RetryBackoffJitter && secs > 1 {
		// Uniform jitter in [secs/2, secs] keeps the upper bound stable while
		// decorrelating bursts of simultaneous retries.
		secs = secs/2 + rand.Intn(secs/2+1)
		if secs < 1 {
			secs = 1
		}
	}
	return secs
}

// extractRetryAfter pulls a retry-after (seconds) hint out of an error body.
// Precedence: retry-after-ms, retry-after, then Codex reset fields
// (resets_at/resets_in_seconds/reset_seconds).
func extractRetryAfter(body string, now time.Time) int {
	// Try retry-after-ms first (higher precision), then retry-after.
	if m := retryAfterMsRe.FindStringSubmatch(body); len(m) == 2 {
		if ms, err := strconv.Atoi(m[1]); err == nil && ms > 0 {
			return (ms + 999) / 1000
		}
	}
	if m := retryAfterRe.FindStringSubmatch(body); len(m) == 2 {
		if s, err := strconv.Atoi(m[1]); err == nil && s > 0 {
			return s
		}
	}
	return codexResetSeconds(body, now)
}

// codexResetSeconds extracts Codex quota/cooldown reset timing. resets_at is an
// absolute Unix timestamp and wins over resets_in_seconds; an expired absolute
// timestamp falls back to resets_in_seconds, then to reset_seconds.
func codexResetSeconds(body string, now time.Time) int {
	env := parseErrorEnvelope(body)
	if env.ResetsAt > 0 {
		resetAt := time.Unix(env.ResetsAt, 0)
		if resetAt.After(now) {
			d := resetAt.Sub(now)
			return int((d + time.Second - 1) / time.Second)
		}
	}
	if env.ResetsInSeconds > 0 {
		return int(env.ResetsInSeconds)
	}
	if env.ResetSeconds > 0 {
		return int(env.ResetSeconds)
	}
	return 0
}

// isWebsocketConnectionLimit reports Codex's 60-minute WebSocket lifecycle limit,
// which CPA retries immediately with zero delay.
func isWebsocketConnectionLimit(body string) bool {
	env := parseErrorEnvelope(body)
	if strings.EqualFold(strings.TrimSpace(env.Code), "websocket_connection_limit_reached") ||
		strings.EqualFold(strings.TrimSpace(env.Type), "websocket_connection_limit_reached") {
		return true
	}
	return strings.Contains(strings.ToLower(body), "websocket_connection_limit_reached")
}

// parseErrorEnvelope pulls structured error fields from the root object, the
// nested "error" object, or the "body.error" object.
func parseErrorEnvelope(raw string) upstreamErrorEnvelope {
	var env upstreamErrorEnvelope
	var root map[string]any
	if err := json.Unmarshal([]byte(raw), &root); err != nil || root == nil {
		return env
	}

	env.Type = pickString(root, []string{"error", "type"}, []string{"body", "error", "type"}, []string{"type"}, []string{"body", "type"})
	env.Code = pickString(root, []string{"error", "code"}, []string{"body", "error", "code"}, []string{"code"}, []string{"body", "code"})
	env.Message = pickString(root, []string{"error", "message"}, []string{"body", "error", "message"}, []string{"message"}, []string{"body", "message"})
	env.ResetsInSeconds = pickInt64(root, []string{"error", "resets_in_seconds"}, []string{"body", "error", "resets_in_seconds"}, []string{"resets_in_seconds"}, []string{"body", "resets_in_seconds"})
	env.ResetsAt = pickInt64(root, []string{"error", "resets_at"}, []string{"body", "error", "resets_at"}, []string{"resets_at"}, []string{"body", "resets_at"})
	env.ResetSeconds = pickInt64(root, []string{"error", "reset_seconds"}, []string{"body", "error", "reset_seconds"}, []string{"reset_seconds"}, []string{"body", "reset_seconds"})
	return env
}

func pickString(root map[string]any, paths ...[]string) string {
	for _, path := range paths {
		if v, ok := drill(root, path).(string); ok && strings.TrimSpace(v) != "" {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

func pickInt64(root map[string]any, paths ...[]string) int64 {
	for _, path := range paths {
		v := drill(root, path)
		switch t := v.(type) {
		case float64:
			if t > 0 {
				return int64(t)
			}
		case json.Number:
			if n, err := t.Int64(); err == nil && n > 0 {
				return n
			}
		case string:
			if n, err := strconv.ParseInt(strings.TrimSpace(t), 10, 64); err == nil && n > 0 {
				return n
			}
		}
	}
	return 0
}

func drill(root map[string]any, path []string) any {
	if len(path) == 0 {
		return root
	}
	var cur any = root
	for _, key := range path {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur, ok = m[key]
		if !ok {
			return nil
		}
	}
	return cur
}
