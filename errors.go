package main

import (
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
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
	// ErrorServer is 500/502/503/504 — transient upstream, circuit-breaker count.
	ErrorServer
	// ErrorSuccess is a non-error (should not reach classification).
	ErrorSuccess
)

// classified is the result of parsing an upstream error.
type classified struct {
	Kind           ErrorKind
	RetryAfterSecs int
	Reason         string
}

// retryAfterRe matches retry-after / retry-after-ms fields in error bodies.
var (
	retryAfterRe   = regexp.MustCompile(`(?i)"retry-after"\s*:\s*"?(\d+)"?`)
	retryAfterMsRe = regexp.MustCompile(`(?i)"retry-after-ms"\s*:\s*"?(\d+)"?`)
)

// classifyError turns an HTTP status + error string into a policy decision.
func classifyError(statusCode int, errStr string) classified {
	kind, reason := classifyByStatus(statusCode, errStr)

	// A textual body can refine the status code (e.g. Anthropic returns 529
	// but sometimes the body says server_is_overloaded on a 503).
	if k, r := classifyByBody(errStr); k != ErrorUnknown {
		kind, reason = k, r
	}

	c := classified{Kind: kind, Reason: reason}

	// Extract retry-after guidance (seconds) when present.
	if secs := extractRetryAfter(errStr); secs > 0 {
		c.RetryAfterSecs = secs
	} else if kind == ErrorOverloaded || kind == ErrorRateLimit {
		// Fall back to a conservative default.
		c.RetryAfterSecs = activeConfig().CooldownDefaultSeconds
	}

	return c
}

func classifyByStatus(statusCode int, errStr string) (ErrorKind, string) {
	switch statusCode {
	case 401, 403:
		return ErrorAuth, "authentication/authorization rejected"
	case 429:
		return ErrorRateLimit, "rate limit / quota exhausted"
	case 529:
		return ErrorOverloaded, "upstream overloaded"
	case 500, 502, 503, 504:
		return ErrorServer, "upstream server error"
	default:
		return ErrorUnknown, "unrecognized failure"
	}
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

// extractRetryAfter pulls a retry-after (seconds) hint out of an error body.
func extractRetryAfter(body string) int {
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
	return 0
}

// parseErrorPayload attempts to pull a structured message out of a JSON body.
func parseErrorPayload(raw string) (code, message string) {
	var obj map[string]any
	if err := json.Unmarshal([]byte(raw), &obj); err != nil {
		return "", raw
	}
	if c, ok := obj["code"].(string); ok {
		code = c
	}
	if m, ok := obj["message"].(string); ok {
		message = m
	} else if t, ok := obj["type"].(string); ok {
		message = t
	}
	return code, message
}
