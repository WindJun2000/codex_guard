package main

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"path/filepath"
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
	chatGPTQuotaEndpoint        = "https://chatgpt.com/backend-api/wham/usage"
	resetCreditsEndpoint        = "https://chatgpt.com/backend-api/wham/rate-limit-reset-credits"
	resetCreditsConsumeEndpoint = "https://chatgpt.com/backend-api/wham/rate-limit-reset-credits/consume"
	codexTokenEndpoint          = "https://auth.openai.com/oauth/token"
	codexClientID               = "app_EMoamEEZ73f0CkXaXp7hrann"
	codexUserAgent              = "codex_cli_rs/0.76.0 (Debian 13.0.0; x86_64) WindowsTerminal"
)

// rosterEntry is one host.auth.list record.
type rosterEntry struct {
	ID            string
	AuthIndex     string
	Email         string
	Name          string
	Provider      string
	Disabled      bool
	Status        string
	StatusMessage string
	Unavailable   bool
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
	mu        sync.Mutex
	refreshMu sync.Mutex
	started   bool
	stop      chan struct{}
	wg        sync.WaitGroup
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
		r.runRefresh()
	}
}

// runRefresh performs a full refresh synchronously, skipping when another
// refresh (manual or scheduled) is already in progress.
func (r *quotaRefresher) runRefresh() bool {
	if !r.refreshMu.TryLock() {
		return false
	}
	defer r.refreshMu.Unlock()
	r.refreshAll()
	return true
}

// triggerRefresh starts a manual full refresh in the background. It returns
// false when a refresh is already running so the caller can report that.
func (r *quotaRefresher) triggerRefresh() bool {
	if !r.refreshMu.TryLock() {
		return false
	}
	go func() {
		defer r.refreshMu.Unlock()
		r.refreshAll()
	}()
	return true
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
		key := accountKey(e)
		if e.Unavailable {
			continue
		}
		if st := stateFor(key); st != nil && st.Disabled {
			continue
		}
		if err := r.refreshAccount(e, now); err != nil {
			logf("warn", "quota.refresh auth=%s failed: %v", key, err)
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
	key := accountKey(e)
	creds, err := resolveCodexCreds(e, now)
	if err != nil {
		return err
	}

	q, err := fetchQuota(creds, now)
	if err != nil {
		return err
	}
	if planType, activeStart, activeUntil := extractIDTokenSubscription(creds.IDToken); planType != "" || !activeStart.IsZero() || !activeUntil.IsZero() {
		if planType != "" {
			q.SubscriptionPlanType = planType
			if q.PlanType == "" {
				q.PlanType = planType
			}
		}
		q.SubscriptionActiveStart = activeStart
		q.SubscriptionActiveUntil = activeUntil
	}
	if q.ResetCreditsAvailableCount != nil && *q.ResetCreditsAvailableCount > 0 {
		if credits, err := fetchResetCredits(creds); err == nil {
			q.ResetCredits = credits
		} else {
			logf("warn", "quota.refresh reset-credits auth=%s failed: %v", key, err)
		}
	}
	if key != "" {
		updateQuota(key, q)
	}
	return nil
}

// resolveCodexCreds loads one roster entry's credential JSON and refreshes an
// expired access token, persisting the refreshed credential back to the host.
func resolveCodexCreds(e rosterEntry, now time.Time) (codexCredentials, error) {
	authIndex := strings.TrimSpace(e.AuthIndex)
	if authIndex == "" {
		authIndex = strings.TrimSpace(e.ID)
	}
	if authIndex == "" {
		return codexCredentials{}, errors.New("roster entry has no auth index")
	}

	var getResp pluginapi.HostAuthGetResponse
	if err := callHostJSON(pluginabi.MethodHostAuthGet, pluginapi.HostAuthGetRequest{AuthIndex: authIndex}, &getResp); err != nil {
		return codexCredentials{}, err
	}
	creds, err := extractCodexCredentials(getResp.JSON)
	if err != nil {
		return codexCredentials{}, err
	}

	// Refresh an expired access token so the quota call succeeds and CPA keeps
	// using a live credential.
	if accessTokenExpired(creds, now) {
		if strings.TrimSpace(creds.RefreshToken) == "" {
			return codexCredentials{}, errors.New("access token expired and refresh token missing")
		}
		refreshed, err := refreshCodexAccessToken(creds.RefreshToken, now)
		if err != nil {
			return codexCredentials{}, fmt.Errorf("token refresh: %w", err)
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
			return codexCredentials{}, errors.New("refresh response did not include access_token")
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
	return creds, nil
}

// listHostAuths returns the Codex roster via host.auth.list.
func listHostAuths() ([]rosterEntry, error) {
	var response struct {
		Files []pluginapi.HostAuthFileEntry `json:"files"`
	}
	if err := callHostJSON(pluginabi.MethodHostAuthList, map[string]any{}, &response); err != nil {
		return nil, err
	}
	entries := make([]rosterEntry, 0, len(response.Files))
	for _, f := range response.Files {
		entries = append(entries, rosterEntry{
			ID:            f.ID,
			AuthIndex:     f.AuthIndex,
			Email:         f.Email,
			Name:          f.Name,
			Provider:      f.Provider,
			Disabled:      f.Disabled,
			Status:        f.Status,
			StatusMessage: f.StatusMessage,
			Unavailable:   f.Unavailable,
		})
	}
	return entries, nil
}

// setHostAuthDisabled persists the CPA-side disabled flag for one Codex auth
// file. It resolves the account's physical JSON via host.auth.get, flips the
// top-level "disabled" metadata field, and writes it back through host.auth.save
// so the host's own auth manager reflects the change immediately.
func setHostAuthDisabled(id string, disabled bool) error {
	id = strings.TrimSpace(id)
	if id == "" {
		return errors.New("auth id is required")
	}
	entries, err := listHostAuths()
	if err != nil {
		return err
	}
	var target rosterEntry
	found := false
	for _, e := range entries {
		if !strings.EqualFold(strings.TrimSpace(e.Provider), "codex") {
			continue
		}
		if accountKey(e) == id {
			target = e
			found = true
			break
		}
	}
	if !found {
		return fmt.Errorf("codex auth %q not found", id)
	}

	authIndex := strings.TrimSpace(target.AuthIndex)
	if authIndex == "" {
		authIndex = target.ID
	}
	if authIndex == "" {
		return errors.New("roster entry has no auth index")
	}

	var getResp pluginapi.HostAuthGetResponse
	if err := callHostJSON(pluginabi.MethodHostAuthGet, pluginapi.HostAuthGetRequest{AuthIndex: authIndex}, &getResp); err != nil {
		return err
	}
	if len(getResp.JSON) == 0 {
		return errors.New("host returned empty auth json")
	}

	dec := json.NewDecoder(bytes.NewReader(getResp.JSON))
	dec.UseNumber()
	var doc map[string]any
	if err := dec.Decode(&doc); err != nil {
		return fmt.Errorf("decode auth json: %w", err)
	}
	doc["disabled"] = disabled
	updated, err := json.Marshal(doc)
	if err != nil {
		return fmt.Errorf("encode auth json: %w", err)
	}

	name := strings.TrimSpace(getResp.Name)
	if name == "" {
		name = target.ID
	}
	if name == "" {
		name = filepath.Base(strings.TrimSpace(getResp.Path))
	}
	if name == "" {
		return errors.New("cannot determine auth file name")
	}
	if err := callHostJSON(pluginabi.MethodHostAuthSave, pluginapi.HostAuthSaveRequest{Name: name, JSON: updated}, nil); err != nil {
		return err
	}
	logf("info", "auth %s cpa disabled=%v", id, disabled)
	return nil
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

	if plan, ok := doc["plan_type"].(string); ok {
		q.PlanType = strings.TrimSpace(plan)
	}
	if credits, ok := doc["credits"].(map[string]any); ok {
		if v, ok := stringFromMap(credits, "balance"); ok {
			q.CreditBalance = v
		} else if n, ok := numberFromAny(credits["balance"]); ok {
			q.CreditBalance = fmt.Sprintf("%v", n)
		}
		if v, ok := credits["has_credits"].(bool); ok {
			q.CreditHasCredits = v
		}
		if v, ok := credits["unlimited"].(bool); ok {
			q.CreditUnlimited = v
		}
		if v, ok := credits["overage_limit_reached"].(bool); ok {
			q.CreditOverageLimitReached = v
		}
	}
	if rrc, ok := doc["rate_limit_reset_credits"].(map[string]any); ok {
		if n, ok := numberFromAny(rrc["available_count"]); ok {
			v := int(n)
			q.ResetCreditsAvailableCount = &v
		}
		if n, ok := numberFromAny(rrc["applicable_available_count"]); ok {
			v := int(n)
			q.ResetCreditsApplicableCount = &v
		}
	}
	rateLimit, _ := doc["rate_limit"].(map[string]any)
	if rateLimit != nil {
		if allowed, ok := rateLimit["allowed"].(bool); ok {
			q.Allowed = allowed
		}
		if reached, ok := rateLimit["limit_reached"].(bool); ok {
			q.LimitReached = reached
		}
	}
	if typ, ok := doc["rate_limit_reached_type"].(string); ok {
		q.LimitReachedType = strings.TrimSpace(typ)
	}
	primary, _ := rateLimit["primary_window"].(map[string]any)
	if primary == nil {
		primary, _ = doc["primary_window"].(map[string]any)
	}
	if primary != nil {
		if used, ok := numberFromAny(primary["used_percent"]); ok {
			q.UsedPercent = used
			q.RemainingPercent = 100 - used
		}
		q.ResetAt = quotaResetAt(primary, now)
		q.WindowSeconds = quotaWindowSeconds(primary)
		q.Windows = append(q.Windows, quotaWindowFromMap(primary, now, "主额度"))
	}
	if secondary, _ := rateLimit["secondary_window"].(map[string]any); secondary != nil {
		q.Windows = append(q.Windows, quotaWindowFromMap(secondary, now, "次级额度"))
	}
	return q, nil
}

// fetchResetCredits reads the detailed proactive reset-credit list. The usage
// endpoint only reports counts, so this call is needed to surface each credit's
// expiry and status. Schema drift is tolerated: a missing credits array simply
// yields an empty slice.
func fetchResetCredits(c codexCredentials) ([]ResetCredit, error) {
	req := pluginapi.HTTPRequest{
		Method: http.MethodGet,
		URL:    resetCreditsEndpoint,
		Headers: http.Header{
			"Authorization":      []string{"Bearer " + c.AccessToken},
			"Chatgpt-Account-Id": []string{c.ChatGPTAccountID},
			"Content-Type":       []string{"application/json"},
			"User-Agent":         []string{codexUserAgent},
		},
	}
	var resp pluginapi.HTTPResponse
	if err := callHostJSON(pluginabi.MethodHostHTTPDo, req, &resp); err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("reset credits status %d", resp.StatusCode)
	}
	return parseResetCredits(resp.Body)
}

func parseResetCredits(body []byte) ([]ResetCredit, error) {
	var doc map[string]any
	if err := json.Unmarshal(body, &doc); err != nil {
		return nil, err
	}
	raw, _ := doc["credits"].([]any)
	if len(raw) == 0 {
		return nil, nil
	}
	credits := make([]ResetCredit, 0, len(raw))
	for _, item := range raw {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		rc := ResetCredit{}
		if v, ok := stringFromMap(m, "id"); ok {
			rc.ID = strings.TrimSpace(v)
		}
		if v, ok := stringFromMap(m, "status"); ok {
			rc.Status = strings.TrimSpace(v)
		}
		if v, ok := stringFromMap(m, "title"); ok {
			rc.Title = strings.TrimSpace(v)
		}
		if v, ok := stringFromMap(m, "reset_type"); ok {
			rc.ResetType = strings.TrimSpace(v)
		}
		if v, ok := lookupTimeDeep(m, "granted_at"); ok {
			rc.GrantedAt = v
		}
		if v, ok := lookupTimeDeep(m, "expires_at"); ok {
			rc.ExpiresAt = v
		}
		credits = append(credits, rc)
	}
	return credits, nil
}

// redeemResetCredit triggers OpenAI's official banked rate-limit reset for an
// account. It picks the soonest-expiring available credit and consumes it, then
// refreshes the account quota so the UI reflects the reset immediately.
func redeemResetCredit(id string) (string, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return "", errors.New("auth id is required")
	}
	entries, err := listHostAuths()
	if err != nil {
		return "", err
	}
	var target rosterEntry
	found := false
	for _, e := range entries {
		if !strings.EqualFold(strings.TrimSpace(e.Provider), "codex") {
			continue
		}
		if accountKey(e) == id {
			target = e
			found = true
			break
		}
	}
	if !found {
		return "", fmt.Errorf("codex auth %q not found", id)
	}

	now := time.Now()
	creds, err := resolveCodexCreds(target, now)
	if err != nil {
		return "", err
	}
	credits, err := fetchResetCredits(creds)
	if err != nil {
		return "", err
	}

	var chosen *ResetCredit
	for i := range credits {
		c := &credits[i]
		if !strings.EqualFold(c.Status, "available") {
			continue
		}
		if !c.ExpiresAt.IsZero() && c.ExpiresAt.Before(now) {
			continue
		}
		if c.ID == "" {
			continue
		}
		if chosen == nil || resetCreditExpiresBefore(c, chosen) {
			chosen = c
		}
	}
	if chosen == nil {
		return "没有可用的主动重置额度", nil
	}
	if err := consumeResetCredit(creds, chosen.ID, newRandomHex(16)); err != nil {
		return "", err
	}
	globalRefresher.refreshAccount(target, time.Now())
	return "已触发官方额度重置", nil
}

func resetCreditExpiresBefore(a, b *ResetCredit) bool {
	if a.ExpiresAt.IsZero() {
		return false
	}
	if b.ExpiresAt.IsZero() {
		return true
	}
	return a.ExpiresAt.Before(b.ExpiresAt)
}

func consumeResetCredit(c codexCredentials, creditID, redeemRequestID string) error {
	payload, err := json.Marshal(map[string]string{
		"credit_id":         creditID,
		"redeem_request_id": redeemRequestID,
	})
	if err != nil {
		return err
	}
	req := pluginapi.HTTPRequest{
		Method: http.MethodPost,
		URL:    resetCreditsConsumeEndpoint,
		Headers: http.Header{
			"Authorization":      []string{"Bearer " + c.AccessToken},
			"Chatgpt-Account-Id": []string{c.ChatGPTAccountID},
			"Content-Type":       []string{"application/json"},
			"User-Agent":         []string{codexUserAgent},
		},
		Body: payload,
	}
	var resp pluginapi.HTTPResponse
	if err := callHostJSON(pluginabi.MethodHostHTTPDo, req, &resp); err != nil {
		return err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("reset credit consume status %d: %s", resp.StatusCode, strings.TrimSpace(string(resp.Body)))
	}
	return nil
}

// newRandomHex returns a lowercase hex string used as a redeem request id.
func newRandomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// quotaWindowFromMap normalizes one rate-limit window into a QuotaWindow.
// The fallback name is used when the window length has no friendly label.
func quotaWindowFromMap(m map[string]any, now time.Time, fallbackName string) QuotaWindow {
	w := QuotaWindow{Name: fallbackName}
	if used, ok := numberFromAny(m["used_percent"]); ok {
		w.UsedPercent = used
		w.RemainingPercent = 100 - used
	}
	w.WindowSeconds = quotaWindowSeconds(m)
	w.ResetAt = quotaResetAt(m, now)
	if name := quotaWindowName(w.WindowSeconds); name != "" {
		w.Name = name
	}
	return w
}

// quotaWindowSeconds reads the window length using the endpoint's actual field
// name (limit_window_seconds) with legacy fallbacks.
func quotaWindowSeconds(m map[string]any) int64 {
	for _, k := range []string{"limit_window_seconds", "window_seconds", "window"} {
		if secs, ok := numberFromAny(m[k]); ok && secs > 0 {
			return int64(secs)
		}
	}
	return 0
}

// quotaWindowName maps common Codex window lengths to a short Chinese label.
func quotaWindowName(seconds int64) string {
	switch seconds {
	case 604800:
		return "周额度"
	case 2592000:
		return "月额度"
	case 18000:
		return "5小时"
	case 86400:
		return "日额度"
	case 3600:
		return "小时额度"
	case 900:
		return "15分钟"
	default:
		return ""
	}
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

// extractIDTokenSubscription decodes the id_token and returns the ChatGPT
// subscription plan type and active window carried in the OpenID auth claim.
func extractIDTokenSubscription(idToken string) (planType string, activeStart, activeUntil time.Time) {
	idToken = strings.TrimSpace(idToken)
	if idToken == "" {
		return "", time.Time{}, time.Time{}
	}
	claims, err := decodeJWTClaims(idToken)
	if err != nil {
		return "", time.Time{}, time.Time{}
	}
	if authClaim, ok := claims["https://api.openai.com/auth"].(map[string]any); ok {
		if v, ok := lookupStringDeep(authClaim, "chatgpt_plan_type"); ok {
			planType = strings.TrimSpace(v)
		}
		activeStart = claimTimeAny(authClaim, "chatgpt_subscription_active_start")
		activeUntil = claimTimeAny(authClaim, "chatgpt_subscription_active_until")
	}
	if planType == "" {
		if v, ok := lookupStringDeep(claims, "chatgpt_plan_type"); ok {
			planType = strings.TrimSpace(v)
		}
	}
	if activeStart.IsZero() {
		activeStart = claimTimeAny(claims, "chatgpt_subscription_active_start")
	}
	if activeUntil.IsZero() {
		activeUntil = claimTimeAny(claims, "chatgpt_subscription_active_until")
	}
	return planType, activeStart, activeUntil
}

// claimTimeAny reads a claim value that may be an RFC3339 string or a Unix
// timestamp (seconds) into a time.Time.
func claimTimeAny(m map[string]any, key string) time.Time {
	v, ok := m[key]
	if !ok {
		return time.Time{}
	}
	if s, ok := v.(string); ok {
		if t, ok := parseAuthTime(s); ok {
			return t
		}
		return time.Time{}
	}
	if secs, ok := numberFromAny(v); ok && secs > 0 {
		return time.Unix(int64(secs), 0)
	}
	return time.Time{}
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
