package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

// Degradation eval statuses.
const (
	EvalStatusNormal   = "normal"
	EvalStatusDegraded = "degraded"
	EvalStatusUnknown  = "unknown"
)

// accountEvalResult is the persistent per-account degradation verdict.
type accountEvalResult struct {
	AuthID                  string    `json:"auth_id"`
	Status                  string    `json:"status"` // normal | degraded | unknown
	Candy                   string    `json:"candy"`  // correct | wrong | failed
	Fingerprint             string    `json:"fingerprint"`
	Prediction              string    `json:"prediction,omitempty"`
	PredictionName          string    `json:"prediction_name,omitempty"`
	Probability             string    `json:"probability,omitempty"`
	Family                  string    `json:"family,omitempty"`
	CandyError              string    `json:"candy_error,omitempty"`
	FingerprintError        string    `json:"fingerprint_error,omitempty"`
	FingerprintSamplesError string    `json:"fingerprint_samples_error,omitempty"`
	LastRunAt               time.Time `json:"last_run_at"`
	SkipReason              string    `json:"skip_reason,omitempty"`
	DetectorRev             string    `json:"detector_revision,omitempty"`
	Tier                    string    `json:"tier,omitempty"`               // "" inherit role | primary | backup | degraded
	ConsecutiveNormal       int       `json:"consecutive_normal,omitempty"` // consecutive normal verdicts while in degraded tier
}

var (
	evalMu       sync.Mutex
	evalResults  = map[string]accountEvalResult{}
	evalRunMu    sync.Mutex
	evalOnce     sync.Once
	evalLoopStop chan struct{}
	evalLoaded   bool

	evalProgressMu sync.Mutex
	evalProgress   = map[string]evalProgressEntry{}
)

// evalProgressEntry tracks the live stage of one in-flight degradation eval so
// the management console can mark the account as "检测中".
type evalProgressEntry struct {
	Stage     string
	StartedAt time.Time
}

// setEvalProgress marks an account as being evaluated, keeping the original
// start time while the stage advances.
func setEvalProgress(id, stage string) {
	evalProgressMu.Lock()
	e := evalProgress[id]
	if e.StartedAt.IsZero() {
		e.StartedAt = time.Now()
	}
	e.Stage = stage
	evalProgress[id] = e
	evalProgressMu.Unlock()
}

func clearEvalProgress(id string) {
	evalProgressMu.Lock()
	delete(evalProgress, id)
	evalProgressMu.Unlock()
}

func evalProgressFor(id string) (evalProgressEntry, bool) {
	evalProgressMu.Lock()
	defer evalProgressMu.Unlock()
	e, ok := evalProgress[id]
	return e, ok
}

// evalFilePath returns the persistent eval-results file, next to plugin settings.
func evalFilePath() string {
	if p := strings.TrimSpace(os.Getenv("CODEX_GUARD_EVAL_FILE")); p != "" {
		return p
	}
	return filepath.Join(filepath.Dir(pluginStatePath()), "codex-guard.eval.json")
}

// loadEvalResults restores persisted verdicts once per process.
func loadEvalResults() {
	evalMu.Lock()
	defer evalMu.Unlock()
	if evalLoaded {
		return
	}
	evalLoaded = true
	raw, err := os.ReadFile(evalFilePath())
	if err != nil {
		return
	}
	var snapshots map[string]accountEvalResult
	if err := json.Unmarshal(raw, &snapshots); err != nil {
		logf("warn", "eval results file invalid: %v", err)
		return
	}
	for id, r := range snapshots {
		if id == "" {
			continue
		}
		r.AuthID = id
		evalResults[id] = r
	}
}

func persistEvalResults() {
	evalMu.Lock()
	snapshots := make(map[string]accountEvalResult, len(evalResults))
	for id, r := range evalResults {
		snapshots[id] = r
	}
	evalMu.Unlock()

	path := evalFilePath()
	data, err := json.MarshalIndent(snapshots, "", "  ")
	if err != nil {
		return
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return
	}
	if err := os.WriteFile(path, data, 0o644); err != nil {
		logf("warn", "persist eval results failed: %v", err)
	}
}

func evalResultFor(id string) (accountEvalResult, bool) {
	evalMu.Lock()
	defer evalMu.Unlock()
	r, ok := evalResults[id]
	return r, ok
}

// saveEvalResult stores a fresh verdict and persists it.
func saveEvalResult(id string, r accountEvalResult) {
	r.AuthID = id
	evalMu.Lock()
	evalResults[id] = r
	evalMu.Unlock()
	persistEvalResults()
}

// saveEvalSkip keeps the previous verdict but records why the account was skipped.
func saveEvalSkip(id, reason string) {
	evalMu.Lock()
	r, ok := evalResults[id]
	if !ok {
		r = accountEvalResult{AuthID: id, Status: EvalStatusUnknown}
	}
	r.LastRunAt = time.Now()
	r.SkipReason = reason
	r.ConsecutiveNormal = 0
	evalResults[id] = r
	evalMu.Unlock()
	persistEvalResults()
	recordEvalHistory(evalHistoryEntry{
		AuthID:     id,
		Status:     r.Status,
		SkipReason: reason,
	})
}

// Tier constants for the three-tier account classification.
const (
	TierPrimary  = "primary"
	TierBackup   = "backup"
	TierDegraded = "degraded"
)

// accountTier resolves the scheduling tier for one account: an explicit eval
// tier wins, otherwise the configured base role (primary | backup) is used.
func accountTier(cfg Config, id string) string {
	if er, ok := evalResultFor(id); ok && er.Tier != "" {
		return er.Tier
	}
	return effectiveRole(cfg, id)
}

// applyEvalTier advances the three-tier state machine from one fresh detection
// verdict. It mutates res so the new tier and consecutive-normal count are
// persisted together with the verdict by saveEvalResult.
func applyEvalTier(id string, freshVerdict string, res *accountEvalResult) {
	prev, ok := evalResultFor(id)
	tier := ""
	consec := 0
	if ok {
		tier = prev.Tier
		consec = prev.ConsecutiveNormal
	}
	if tier == "" {
		tier = effectiveRole(activeConfig(), id)
	}

	switch freshVerdict {
	case EvalStatusDegraded:
		tier = TierDegraded
		consec = 0
	case EvalStatusNormal:
		if tier == TierDegraded {
			consec++
			if consec >= 2 {
				tier = TierBackup
				consec = 0
			}
		} else {
			consec = 0
		}
	default: // EvalStatusUnknown
		consec = 0
	}

	base := effectiveRole(activeConfig(), id)
	if tier == base {
		res.Tier = ""
	} else {
		res.Tier = tier
	}
	res.ConsecutiveNormal = consec
}

// resetEvalTier clears any derived tier so the account inherits its configured
// role again. Used when the user explicitly overrides the account role.
func resetEvalTier(id string) {
	evalMu.Lock()
	r, ok := evalResults[id]
	if !ok {
		evalMu.Unlock()
		return
	}
	r.Tier = ""
	r.ConsecutiveNormal = 0
	evalResults[id] = r
	evalMu.Unlock()
	persistEvalResults()
}

// startEvalLoop launches the background evaluation loop once.
func startEvalLoop() {
	evalOnce.Do(func() {
		evalLoopStop = make(chan struct{})
		go evalLoop()
	})
}

func stopEvalLoop() {
	if evalLoopStop == nil {
		return
	}
	select {
	case <-evalLoopStop:
	default:
		close(evalLoopStop)
	}
}

func evalLoop() {
	for {
		cfg := activeConfig()
		interval := time.Duration(cfg.EvalIntervalMinutes) * time.Minute
		if interval <= 0 || !cfg.EvalEnabled {
			interval = time.Minute
		}
		select {
		case <-evalLoopStop:
			return
		case <-time.After(interval):
		}
		cfg = activeConfig()
		if !cfg.EvalEnabled || cfg.EvalIntervalMinutes <= 0 {
			continue
		}
		runEvalAll()
	}
}

// triggerEvalRun starts a manual full evaluation run. It returns false when one
// is already running.
func triggerEvalRun() bool {
	if !evalRunMu.TryLock() {
		return false
	}
	go func() {
		defer evalRunMu.Unlock()
		runEvalAll()
	}()
	return true
}

// triggerEvalRunFor starts a targeted single-account degradation eval. It
// returns false when another eval run is already active; the caller keeps the
// distinction between "not found" (error) and "busy" (false, nil).
func triggerEvalRunFor(id string) (bool, error) {
	id = strings.TrimSpace(id)
	if id == "" {
		return false, fmt.Errorf("auth id is required")
	}
	entries, err := listHostAuths()
	if err != nil {
		return false, err
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
		return false, fmt.Errorf("codex auth %q not found", id)
	}
	if !evalRunMu.TryLock() {
		return false, nil
	}
	go func(e rosterEntry) {
		defer evalRunMu.Unlock()
		key := accountKey(e)
		if e.Unavailable {
			saveEvalSkip(key, "不可用")
			return
		}
		if e.Disabled {
			saveEvalSkip(key, "CPA 禁用")
			return
		}
		if reason := evalSkipReason(key); reason != "" {
			saveEvalSkip(key, reason)
			return
		}
		res := runAccountEval(e, time.Now())
		saveEvalResult(key, res)
	}(target)
	return true, nil
}

// runEvalAll evaluates every Codex account with bounded parallelism.
func runEvalAll() {
	entries, err := listHostAuths()
	if err != nil {
		logf("warn", "eval host.auth.list failed: %v", err)
		return
	}
	concurrency := activeConfig().EvalConcurrency
	if concurrency < 1 {
		concurrency = 1
	}
	now := time.Now()
	sem := make(chan struct{}, concurrency)
	var wg sync.WaitGroup
	for _, e := range entries {
		if !strings.EqualFold(strings.TrimSpace(e.Provider), "codex") {
			continue
		}
		e := e
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			key := accountKey(e)
			if e.Unavailable {
				saveEvalSkip(key, "不可用")
				return
			}
			if e.Disabled {
				saveEvalSkip(key, "CPA 禁用")
				return
			}
			if reason := evalSkipReason(key); reason != "" {
				saveEvalSkip(key, reason)
				return
			}
			res := runAccountEval(e, now)
			saveEvalResult(key, res)
		}()
	}
	wg.Wait()
}

// evalHardProtectionReason reports the reason an account cannot run a
// degradation check because of a hard protection (disable / cooldown / transient
// pause / circuit / overload). It mirrors normal scheduling, which always blocks
// these states, and is used both for the pre-run skip and for the mid-run guard.
func evalHardProtectionReason(st *AccountState, now time.Time) string {
	if st.Disabled {
		return "插件禁用"
	}
	if !st.CooldownUntil.IsZero() && st.CooldownUntil.After(now) {
		return "冷却中"
	}
	if !st.TransientPauseUntil.IsZero() && st.TransientPauseUntil.After(now) {
		return "瞬时停用中"
	}
	if !st.CircuitOpenUntil.IsZero() && st.CircuitOpenUntil.After(now) {
		return "熔断中"
	}
	if !st.OverloadUntil.IsZero() && st.OverloadUntil.After(now) {
		return "超负荷中"
	}
	return ""
}

// evalHighPressureReason reports whether the account's dispatch concurrency
// exceeds the configured eval high-pressure threshold.
func evalHighPressureReason(key string, cfg Config) string {
	cur, peak := parallelPeakWithin(key, time.Duration(cfg.EvalHighPressureWindowSeconds)*time.Second)
	if cur > cfg.EvalHighPressureParallel || peak > cfg.EvalHighPressureParallel {
		return "高压运行中"
	}
	return ""
}

// evalSkipReason decides whether a degradation check should be skipped before
// it starts. Hard protections only apply when eval_skip_abnormal is enabled;
// high pressure only when eval_skip_high_pressure is enabled. This is the
// config-gated pre-run skip, used by the batch and targeted eval entry points.
func evalSkipReason(key string) string {
	cfg := activeConfig()
	if cfg.EvalSkipAbnormal {
		if st := stateFor(key); st != nil {
			if r := evalHardProtectionReason(st, time.Now()); r != "" {
				return r
			}
		}
	}
	if cfg.EvalSkipHighPressure {
		return evalHighPressureReason(key, cfg)
	}
	return ""
}

// evalMidRunProtection is polled between questions (candy → fingerprint, and
// before every fingerprint sample). Hard protections always apply, matching
// normal scheduling, so a mid-run overload/cooldown abandons the test even when
// eval_skip_abnormal allowed the run to start. High pressure stays gated by
// eval_skip_high_pressure.
func evalMidRunProtection(key string) string {
	cfg := activeConfig()
	if st := stateFor(key); st != nil {
		if r := evalHardProtectionReason(st, time.Now()); r != "" {
			return r
		}
	}
	if cfg.EvalSkipHighPressure {
		return evalHighPressureReason(key, cfg)
	}
	return ""
}

// evalStopReason formats the history record for an eval run abandoned by the
// protection guard, naming the stage that was about to run.
func evalStopReason(stage, reason string) string {
	if reason == "" {
		return ""
	}
	return fmt.Sprintf("%s前触发保护：%s", stage, reason)
}

func runAccountEval(e rosterEntry, now time.Time) accountEvalResult {
	key := accountKey(e)
	model := resolveEvalModel(e)
	cfg := activeConfig()
	tier := accountTier(cfg, key)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	setEvalProgress(key, "准备中")
	defer clearEvalProgress(key)

	preGuard := func() string { return evalSkipReason(key) }
	midGuard := func() string { return evalMidRunProtection(key) }
	stage := func(s string) { setEvalProgress(key, s) }

	// finish builds, classifies, records and returns the run result. A non-empty
	// stopReason marks an abandoned run (either before candy or between stages).
	finish := func(stopReason string, candy candyResult, fp fingerprintEvalResult) accountEvalResult {
		res := accountEvalResult{
			AuthID:                  key,
			Status:                  EvalStatusUnknown,
			Candy:                   candy.Status,
			Fingerprint:             fp.Status,
			CandyError:              candy.Error,
			FingerprintError:        fp.Error,
			FingerprintSamplesError: fingerprintSamplesError(fp.Samples),
			Prediction:              fp.Prediction,
			PredictionName:          fp.PredictionName,
			Probability:             evalProbabilityString(fp.Probability),
			Family:                  fp.Family,
			LastRunAt:               now,
			SkipReason:              stopReason,
			DetectorRev:             fp.DetectorRev,
		}
		fresh := classifyEval(candy, fp)
		if prev, ok := evalResultFor(key); ok {
			// Preserve the previous verdict when this run is inconclusive.
			res.Status = prev.Status
		} else {
			res.Status = EvalStatusUnknown
		}
		if fresh != EvalStatusUnknown {
			res.Status = fresh
		}
		applyEvalTier(key, fresh, &res)
		recordEvalHistory(evalHistoryEntry{
			AuthID:                  key,
			Status:                  res.Status,
			Candy:                   res.Candy,
			Fingerprint:             res.Fingerprint,
			Prediction:              res.Prediction,
			PredictionName:          res.PredictionName,
			Probability:             res.Probability,
			Family:                  res.Family,
			CandyAnswer:             candy.Answer,
			CandyError:              res.CandyError,
			FingerprintError:        res.FingerprintError,
			FingerprintSamplesError: res.FingerprintSamplesError,
			FingerprintSamples:      historyFingerprintSamples(fp.Samples),
			SkipReason:              stopReason,
			Model:                   model,
			Effort:                  cfg.EvalEffort,
			DetectorRev:             res.DetectorRev,
		})
		return res
	}

	stage("糖果题")
	if reason := preGuard(); reason != "" {
		return finish(evalStopReason("糖果题", reason), candyResult{}, fingerprintEvalResult{})
	}

	candy := evaluateCandy(ctx, key, "codex", model, cfg.EvalEffort)
	if !shouldRunFingerprint(tier, candy) {
		return finish("", candy, fingerprintEvalResult{Status: "skipped"})
	}

	stage("指纹题")
	if reason := midGuard(); reason != "" {
		return finish(evalStopReason("指纹题", reason), candy, fingerprintEvalResult{})
	}

	fp := evaluateFingerprint(ctx, key, "codex", model, midGuard, stage)
	if fp.Status == fingerprintStopped {
		return finish(fp.Error, candy, fp)
	}
	return finish("", candy, fp)
}

// historyFingerprintSamples maps evaluator samples into the persisted history
// shape, preserving raw answers and per-sample errors for hover inspection.
func historyFingerprintSamples(samples []fingerprintSampleEval) []evalHistoryFingerprintSample {
	out := make([]evalHistoryFingerprintSample, 0, len(samples))
	for _, s := range samples {
		out = append(out, evalHistoryFingerprintSample{
			Index:    s.Index,
			Expected: s.Expected,
			Attempts: s.Attempts,
			Parsed:   s.Parsed,
			Valid:    s.Valid,
			Answer:   s.Answer,
			Error:    s.Error,
		})
	}
	return out
}

// fingerprintSamplesError collapses the per-sample errors of one fingerprint
// run into a single readable line so failures remain auditable in history.
func fingerprintSamplesError(samples []fingerprintSampleEval) string {
	parts := make([]string, 0, len(samples))
	for _, s := range samples {
		if s.Error != "" {
			parts = append(parts, fmt.Sprintf("#%d: %s", s.Index+1, s.Error))
		}
	}
	return strings.Join(parts, "; ")
}

// classifyEval maps one run's candy + fingerprint evidence to a verdict.
// Unknown means "not conclusive; keep the previous verdict".
func classifyEval(candy candyResult, fp fingerprintEvalResult) string {
	if candy.Status == "wrong" {
		return EvalStatusDegraded
	}
	if candy.Status == "correct" && fp.Status == "skipped" {
		return EvalStatusNormal
	}
	if fp.Status == "not_model" {
		return EvalStatusDegraded
	}
	if fp.Status == "model" && !isOpenAIFamily(fp.Family) {
		return EvalStatusDegraded
	}
	if candy.Status == "correct" && fp.Status == "model" {
		return EvalStatusNormal
	}
	return EvalStatusUnknown
}

// shouldRunFingerprint decides whether the three fingerprint challenges are
// worth running after the candy answer. Primary/backup tiers only need
// fingerprint attribution when candy is wrong; degraded tiers only need it
// when candy is correct (to confirm recovery). A failed candy call never
// triggers fingerprint, since follow-up calls would most likely fail too.
func shouldRunFingerprint(tier string, candy candyResult) bool {
	switch candy.Status {
	case "correct":
		return tier == TierDegraded
	case "wrong":
		return tier != TierDegraded
	default: // failed / empty
		return false
	}
}

// evalProbabilityString formats a 0..1 detector probability as a human-readable percent.
func evalProbabilityString(p *float64) string {
	if p == nil {
		return ""
	}
	return fmt.Sprintf("%.1f%%", *p*100)
}

// isOpenAIFamily reports whether the detector's family prediction belongs to
// OpenAI's GPT line.
func isOpenAIFamily(family string) bool {
	family = strings.ToLower(strings.TrimSpace(family))
	return strings.Contains(family, "gpt") || strings.Contains(family, "openai")
}

// resolveEvalModel picks the model for one account: the configured eval_model
// first, then host.auth.models, then gpt-5.
func resolveEvalModel(e rosterEntry) string {
	cfg := activeConfig()
	if m := strings.TrimSpace(cfg.EvalModel); m != "" {
		return m
	}
	authIndex := strings.TrimSpace(e.AuthIndex)
	if authIndex == "" {
		authIndex = strings.TrimSpace(e.ID)
	}
	key := accountKey(e)
	if authIndex != "" {
		var resp pluginapi.HostAuthModelsResponse
		if err := callHostJSON(pluginabi.MethodHostAuthModels, pluginapi.HostAuthModelsRequest{AuthIndex: authIndex}, &resp); err == nil {
			for _, c := range resp.Credentials {
				matches := strings.TrimSpace(c.ID) == key || strings.TrimSpace(c.AuthIndex) == key || strings.TrimSpace(c.AuthIndex) == authIndex
				if matches && len(c.Models) > 0 {
					return c.Models[0]
				}
			}
		}
	}
	return "gpt-5"
}
