package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

// Active quota polling. The scheduler's passive evidence (response headers +
// candidate metadata) is only refreshed when traffic happens to hit an account.
// This refresher closes that gap by polling OpenAI's usage endpoint for every
// Codex account on a timer, using host.auth.list / host.auth.get to resolve
// credentials and host.http.do for the outbound calls (the host owns the
// transport, so the plugin never sees or stores raw secrets beyond the in-scope
// credential JSON returned by the host itself).

const (
	chatGPTQuotaEndpoint = "https://chatgpt.com/backend-api/wham/usage"
	codexTokenEndpoint   = "https://auth.openai.com/oauth/token"
	codexClientID        = "app_EMoamEEZ73f0CkXaXp7hrann"
	codexUserAgent       = "codex_cli_rs/0.76.0 (Debian 13.0.0; x86_64) WindowsTerminal"
)

// rosterEntry is one host.auth.list record.
type rosterEntry struct {
	ID        string
	AuthIndex string
	Provider  string
}

// codexCredentials is the normalized credential view extracted from host JSON.
type codexCredentials struct {
	AccessToken      string
	RefreshToken     string
	IDToken          string
	ChatGPTAccountID string
	ExpiresAt        time.Time
	LastRefresh      time.Time
}

// quotaRefresher owns the background poll loop.
type quotaRefresher struct {
	mu      sync.Mutex
	started bool
	stop    chan struct{}
	wg      sync.WaitGroup
}

var globalRefresher quotaRefresher

// maybeStartRefresher starts the poller once. It is called from configure() so
// the loop begins after the first plugin.register/reconfigure delivers config.
func maybeStartRefresher() {
	globalRefresher.mu.Lock()
	defer globalRefresher.mu.Unlock()
	if globalRefresher.started {
		return
	}
	globalRefresher.started = true
	globalRefresher.stop = make(chan struct{})
	globalRefresher.wg.Add(1)
	go globalRefresher.run()
}

// stopRefresher halts the poller (called from plugin shutdown).
func (r *quotaRefresher) stopRefresher() {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.started || r.stop == nil {
		return
	}
	select {
	case <-r.stop:
	default:
		close(r.stop)
	}
}

func (r *quotaRefresher) run() {
	defer r.wg.Done()
	for {
		interval := time.Duration(activeConfig().QuotaRefreshIntervalSeconds) * time.Second
		if interval <= 0 {
			interval = 300 * time.Second
		}
		select {
		case <-r.stop:
			return
		case <-time.After(interval):
		}
		if !activeConfig().QuotaRefreshEnabled {
			continue
		}
		r.refreshAll()
	}
}

func (r *quotaRefresher) refreshAll() {
	entries, err := listHostAuths()
	if err != nil {
		logf("warn", "quota.refresh host.auth.list failed: %v", err)
		return
	}
	now := time.Now()
	for _, e := range entries {
		if !strings.EqualFold(strings.TrimSpace(e.Provider), "codex") {
			continue
		}
		if err := r.refreshAccount(e, now); err != nil {
			logf("warn", "quota.refresh auth=%s failed: %v", accountKey(e), err)
		}
	}
}

// accountKey maps a roster entry to the same key the scheduler uses
// (candidate.ID). Falls back to AuthIndex when the host omits ID.
func accountKey(e rosterEntry) string {
	if strings.TrimSpace(e.ID) != "" {
		return e.ID
	}
	return e.AuthIndex
}

func (r *quotaRefresher) refreshAccount(e rosterEntry, now time.Time) error {
	authIndex := e.AuthIndex
	if authIndex == "" {
		authIndex = e.ID
	}
	if authIndex == "" {
		return errors.New("roster entry has no auth index")
	}

	var getResp pluginapi.HostAuthGetResponse
	if err := callHostJSON(pluginabi.MethodHostAuthGet, pluginapi.HostAuthGetRequest{AuthIndex: authIndex}, &getResp); err != nil {
		return err
	}
	creds, err := extractCodexCredentials(getResp.JSON)
	if err != nil {
		return err
	}

	// Refresh an expired access token so the quota call succeeds and CPA keeps
	// using a live credential.
	if accessTokenExpired(creds, now) {
		if strings.TrimSpace(creds.RefreshToken) == "" {
			return errors.New("access token expired and refresh token missing")
		}
		refreshed, err := refreshCodexAccessToken(creds.RefreshToken, now)
		if err != nil {
			return fmt.Errorf("token refresh: %w", err)
		}
		if refreshed.RefreshToken == "" {
			refreshed.RefreshToken = creds.RefreshToken
		}
		if refreshed.ChatGPTAccountID == "" {
			refreshed.ChatGPTAccountID = creds.ChatGPTAccountID
		}
		if refreshed.IDToken == "" {
			refreshed.IDToken = creds.IDToken
		}
		if refreshed.AccessToken == "" {
			return errors.New("refresh response did not include access_token")
		}
		if updated, err := updateCodexAuthJSON(getResp.JSON, refreshed, now); err == nil {
			if name := strings.TrimSpace(getResp.Name); name != "" {
				_ = callHostJSON(pluginabi.MethodHostAuthSave, pluginapi.HostAuthSaveRequest{Name: name, JSON: updated}, nil)
			}
		}
		// A successful refresh proves the credential is valid again.
		if key := accountKey(e); key != "" {
			reEnableAccount(key)
		}
		creds = refreshed
	}

	q, err := fetchQuota(creds, now)
	if err != nil {
		return err
	}
	if key := accountKey(e); key != "" {
		updateQuota(key, q)
	}
	return nil
}

// listHostAuths returns the Codex roster via host.auth.list.
func listHostAuths() ([]rosterEntry, error) {
	var response struct {
		Files []struct {
			ID        string `json:"id"`
			AuthIndex string `json:"auth_index"`
			Provider  string `json:"provider"`
		} `json:"files"`
	}
	if err := callHostJSON(pluginabi.MethodHostAuthList, map[string]any{}, &response); err != nil {
		return nil, err
	}
	entries := make([]rosterEntry, 0, len(response.Files))
	for _, f := range response.Files {
		entries = append(entries, rosterEntry{ID: f.ID, AuthIndex: f.AuthIndex, Provider: f.Provider})
	}
	return entries, nil
}

// extractCodexCredentials pulls access/refresh tokens and the account id out of
// an arbitrary host credential JSON (flat or nested under tokens/metadata).
func extractCodexCredentials(raw json.RawMessage) (codexCredentials, error) {
	var doc any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return codexCredentials{}, err
	}
	accessToken, ok := lookupStringDeep(doc, "access_token")
	if !ok || accessToken == "" {
		return codexCredentials{}, errors.New("missing access_token")
	}
	refreshToken, _ := lookupStringDeep(doc, "refresh_token")
	idToken, _ := lookupStringDeep(doc, "id_token")
	expiresAt, _ := lookupTimeDeep(doc, "expired", "expire", "expires_at", "expiresAt")
	lastRefresh, _ := lookupTimeDeep(doc, "last_refresh")

	accountID, _ := lookupStringDeep(doc, "account_id")
	if accountID == "" {
		accountID, _ = lookupStringDeep(doc, "chatgpt_account_id")
	}
	if accountID == "" && idToken != "" {
		if extracted, err := extractChatGPTAccountID(idToken); err == nil {
			accountID = extracted
		}
	}

	return codexCredentials{
		AccessToken:      accessToken,
		RefreshToken:     refreshToken,
		IDToken:          idToken,
		ChatGPTAccountID: accountID,
		ExpiresAt:        expiresAt,
		LastRefresh:      lastRefresh,
	}, nil
}

const accessTokenFallbackTTL = time.Hour

func accessTokenExpired(c codexCredentials, now time.Time) bool {
	if !c.ExpiresAt.IsZero() {
		return !now.Add(2 * time.Minute).Before(c.ExpiresAt)
	}
	if !c.LastRefresh.IsZero() {
		return now.Sub(c.LastRefresh) >= accessTokenFallbackTTL
	}
	return true
}

// refreshCodexAccessToken exchanges a refresh_token for a fresh access token.
func refreshCodexAccessToken(refreshToken string, now time.Time) (codexCredentials, error) {
	form := url.Values{
		"client_id":     {codexClientID},
		"grant_type":    {"refresh_token"},
		"refresh_token": {refreshToken},
		"scope":         {"openid profile email"},
	}
	req := pluginapi.HTTPRequest{
		Method: http.MethodPost,
		URL:    codexTokenEndpoint,
		Headers: http.Header{
			"Accept":       []string{"application/json"},
			"Content-Type": []string{"application/x-www-form-urlencoded"},
		},
		Body: []byte(form.Encode()),
	}
	var resp pluginapi.HTTPResponse
	if err := callHostJSON(pluginabi.MethodHostHTTPDo, req, &resp); err != nil {
		return codexCredentials{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return codexCredentials{}, fmt.Errorf("token refresh status %d", resp.StatusCode)
	}
	var tr struct {
		AccessToken  string `json:"access_token"`
		RefreshToken string `json:"refresh_token"`
		IDToken      string `json:"id_token"`
		ExpiresIn    int    `json:"expires_in"`
		AccountID    string `json:"account_id"`
	}
	if err := json.Unmarshal(resp.Body, &tr); err != nil {
		return codexCredentials{}, fmt.Errorf("parse token refresh: %w", err)
	}
	c := codexCredentials{
		AccessToken:      strings.TrimSpace(tr.AccessToken),
		RefreshToken:     strings.TrimSpace(tr.RefreshToken),
		IDToken:          strings.TrimSpace(tr.IDToken),
		ChatGPTAccountID: strings.TrimSpace(tr.AccountID),
	}
	if c.ChatGPTAccountID == "" && c.IDToken != "" {
		if id, err := extractChatGPTAccountID(c.IDToken); err == nil {
			c.ChatGPTAccountID = id
		}
	}
	if tr.ExpiresIn > 0 {
		c.ExpiresAt = now.Add(time.Duration(tr.ExpiresIn) * time.Second)
	}
	return c, nil
}

// updateCodexAuthJSON writes refreshed tokens back into the host credential JSON.
func updateCodexAuthJSON(raw json.RawMessage, c codexCredentials, now time.Time) (json.RawMessage, error) {
	var doc map[string]any
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &doc); err != nil {
			return nil, err
		}
	}
	if doc == nil {
		doc = make(map[string]any)
	}
	doc["access_token"] = c.AccessToken
	doc["refresh_token"] = c.RefreshToken
	doc["id_token"] = c.IDToken
	doc["account_id"] = c.ChatGPTAccountID
	doc["type"] = "codex"
	if !c.ExpiresAt.IsZero() {
		doc["expired"] = c.ExpiresAt.Format(time.RFC3339)
	}
	if !now.IsZero() {
		doc["last_refresh"] = now.Format(time.RFC3339)
	}
	return json.Marshal(doc)
}

// fetchQuota reads the account's remaining quota from the usage endpoint.
func fetchQuota(c codexCredentials, now time.Time) (QuotaSnapshot, error) {
	req := pluginapi.HTTPRequest{
		Method: http.MethodGet,
		URL:    chatGPTQuotaEndpoint,
		Headers: http.Header{
			"Authorization":      []string{"Bearer " + c.AccessToken},
			"Chatgpt-Account-Id": []string{c.ChatGPTAccountID},
			"Content-Type":       []string{"application/json"},
			"User-Agent":         []string{codexUserAgent},
		},
	}
	var resp pluginapi.HTTPResponse
	if err := callHostJSON(pluginabi.MethodHostHTTPDo, req, &resp); err != nil {
		return QuotaSnapshot{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return QuotaSnapshot{}, fmt.Errorf("quota status %d", resp.StatusCode)
	}
	return parseUsagePayload(resp.Body, now)
}

// parseUsagePayload maps the wham/usage response into a QuotaSnapshot.
// Window/reset discovery is tier-agnostic: it prefers the account's own reset
// signal (absolute reset_at, then reset_after_seconds) and only keeps the
// window length as a fallback hint. It never invents a reset time, so a
// non-conforming endpoint degrades to percent-only ordering instead of
// guessing a 5-hour Codex Plus window for every account.
func parseUsagePayload(body []byte, now time.Time) (QuotaSnapshot, error) {
	var doc map[string]any
	if err := json.Unmarshal(body, &doc); err != nil {
		return QuotaSnapshot{}, err
	}
	q := QuotaSnapshot{}

	rateLimit, _ := doc["rate_limit"].(map[string]any)
	primary, _ := rateLimit["primary_window"].(map[string]any)
	if primary == nil {
		primary, _ = doc["primary_window"].(map[string]any)
	}
	if primary != nil {
		if used, ok := numberFromAny(primary["used_percent"]); ok {
			q.RemainingPercent = 100 - used
		}
		q.ResetAt = quotaResetAt(primary, now)
		if secs, ok := numberFromAny(primary["window_seconds"]); ok && secs > 0 {
			q.WindowSeconds = int64(secs)
		} else if secs, ok := numberFromAny(primary["window"]); ok && secs > 0 {
			q.WindowSeconds = int64(secs)
		}
	}
	return q, nil
}

// quotaResetAt resolves the primary-window reset instant. Absolute reset_at is
// preferred (RFC3339 string or Unix seconds); reset_after_seconds is the
// relative fallback. Zero time means the endpoint did not disclose a reset.
func quotaResetAt(primary map[string]any, now time.Time) time.Time {
	for _, key := range []string{"reset_at", "resets_at", "reset_time"} {
		v, ok := primary[key]
		if !ok {
			continue
		}
		if s, ok := v.(string); ok && s != "" {
			if t, ok := parseAuthTime(s); ok {
				return t
			}
			continue
		}
		if secs, ok := numberFromAny(v); ok && secs > 0 {
			return time.Unix(int64(secs), 0)
		}
	}
	if secs, ok := numberFromAny(primary["reset_after_seconds"]); ok && secs > 0 {
		return now.Add(time.Duration(secs) * time.Second)
	}
	return time.Time{}
}

// ---------------------------------------------------------------------------
// small JSON helpers (self-contained; no external deps)
// ---------------------------------------------------------------------------

func lookupStringDeep(v any, key string) (string, bool) {
	switch typed := v.(type) {
	case map[string]any:
		if s, ok := stringFromMap(typed, key); ok {
			return s, true
		}
		for _, child := range typed {
			if s, ok := lookupStringDeep(child, key); ok {
				return s, true
			}
		}
	case []any:
		for _, child := range typed {
			if s, ok := lookupStringDeep(child, key); ok {
				return s, true
			}
		}
	}
	return "", false
}

func stringFromMap(m map[string]any, key string) (string, bool) {
	v, ok := m[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

func lookupTimeDeep(v any, keys ...string) (time.Time, bool) {
	for _, key := range keys {
		if raw, ok := lookupStringDeep(v, key); ok {
			if parsed, ok := parseAuthTime(raw); ok {
				return parsed, true
			}
		}
	}
	return time.Time{}, false
}

func parseAuthTime(raw string) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed, true
		}
	}
	return time.Time{}, false
}

func extractChatGPTAccountID(idToken string) (string, error) {
	claims, err := decodeJWTClaims(idToken)
	if err != nil {
		return "", err
	}
	if v, ok := stringFromMap(claims, "https://api.openai.com/auth.chatgpt_account_id"); ok && v != "" {
		return v, nil
	}
	if authClaim, ok := claims["https://api.openai.com/auth"].(map[string]any); ok {
		if v, ok := stringFromMap(authClaim, "chatgpt_account_id"); ok && v != "" {
			return v, nil
		}
	}
	if v, ok := lookupStringDeep(claims, "chatgpt_account_id"); ok && v != "" {
		return v, nil
	}
	return "", errors.New("missing chatgpt_account_id")
}

func decodeJWTClaims(token string) (map[string]any, error) {
	parts := strings.Split(token, ".")
	if len(parts) < 2 {
		return nil, errors.New("invalid id_token")
	}
	payload, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, err
	}
	var claims map[string]any
	if err := json.Unmarshal(payload, &claims); err != nil {
		return nil, err
	}
	return claims, nil
}
