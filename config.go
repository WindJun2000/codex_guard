package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"gopkg.in/yaml.v3"
)

// OverloadRule is one "consume > limit within window → stop stop_seconds"
// protection rule. Metric selects the consumption source.
type OverloadRule struct {
	ID            string  `yaml:"id" json:"id"`
	Metric        string  `yaml:"metric" json:"metric"` // "token" | "quota_percent"
	WindowSeconds int64   `yaml:"window_seconds" json:"window_seconds"`
	Limit         float64 `yaml:"limit" json:"limit"`
	StopSeconds   int     `yaml:"stop_seconds" json:"stop_seconds"`
}

// AccountConfig carries per-account overrides. Pointer fields mean "unset",
// which inherits the global default.
type AccountConfig struct {
	ID            string         `yaml:"id" json:"id"`
	Enabled       *bool          `yaml:"enabled" json:"enabled"`
	Role          string         `yaml:"role" json:"role"` // "primary" | "backup" | "degraded"
	Weight        *float64       `yaml:"weight" json:"weight"`
	MaxParallel   *int           `yaml:"max_parallel" json:"max_parallel"`
	OverloadRules []OverloadRule `yaml:"overload_rules" json:"overload_rules"`
	ExpiresAt     string         `yaml:"expires_at" json:"expires_at"`
	WindowSeconds *int64         `yaml:"window_seconds" json:"window_seconds"`
}

// Config holds all plugin settings: global defaults plus per-account overrides.
type Config struct {
	Strategy string `yaml:"strategy" json:"strategy"` // quota_pressure | quota | expiry
	Provider string `yaml:"provider" json:"provider"`

	CooldownDefaultSeconds    int `yaml:"cooldown_default_seconds" json:"cooldown_default_seconds"`
	CooldownFixedBonusSeconds int `yaml:"cooldown_fixed_bonus_seconds" json:"cooldown_fixed_bonus_seconds"`
	MaxCooldownSeconds        int `yaml:"max_cooldown_seconds" json:"max_cooldown_seconds"`

	AuthFailureCooldownSeconds int  `yaml:"auth_failure_cooldown_seconds" json:"auth_failure_cooldown_seconds"`
	AutoDisableOnAuthFailure   bool `yaml:"auto_disable_on_auth_failure" json:"auto_disable_on_auth_failure"`

	CircuitFailureThreshold    int `yaml:"circuit_failure_threshold" json:"circuit_failure_threshold"`
	CircuitOpenDurationSeconds int `yaml:"circuit_open_duration_seconds" json:"circuit_open_duration_seconds"`

	EscalationWindowSeconds int     `yaml:"escalation_window_seconds" json:"escalation_window_seconds"`
	EscalationThreshold     int     `yaml:"escalation_threshold" json:"escalation_threshold"`
	EscalationMultiplier    float64 `yaml:"escalation_multiplier" json:"escalation_multiplier"`

	QuotaRefreshEnabled         bool    `yaml:"quota_refresh_enabled" json:"quota_refresh_enabled"`
	QuotaRefreshIntervalSeconds int     `yaml:"quota_refresh_interval_seconds" json:"quota_refresh_interval_seconds"`
	QuotaHardFloorPercent       float64 `yaml:"quota_hard_floor_percent" json:"quota_hard_floor_percent"`

	ExpiryPriorityEnabled      bool    `yaml:"expiry_priority_enabled" json:"expiry_priority_enabled"`
	ExpiryPriorityHorizonHours float64 `yaml:"expiry_priority_horizon_hours" json:"expiry_priority_horizon_hours"`

	SessionAffinityEnabled    bool `yaml:"session_affinity_enabled" json:"session_affinity_enabled"`
	SessionAffinityTTLSeconds int  `yaml:"session_affinity_ttl_seconds" json:"session_affinity_ttl_seconds"`

	MaxParallelDefault int  `yaml:"max_parallel_default" json:"max_parallel_default"`
	FallbackReject     bool `yaml:"fallback_reject" json:"fallback_reject"`
	InFlightTTLSeconds int  `yaml:"in_flight_ttl_seconds" json:"in_flight_ttl_seconds"`

	HistoryMaxEntries     int `yaml:"history_max_entries" json:"history_max_entries"`
	EvalHistoryMaxEntries int `yaml:"eval_history_max_entries" json:"eval_history_max_entries"`

	EvalEnabled                   bool   `yaml:"eval_enabled" json:"eval_enabled"`
	EvalIntervalMinutes           int    `yaml:"eval_interval_minutes" json:"eval_interval_minutes"`
	EvalModel                     string `yaml:"eval_model" json:"eval_model"`
	EvalEffort                    string `yaml:"eval_effort" json:"eval_effort"`
	EvalSkipAbnormal              bool   `yaml:"eval_skip_abnormal" json:"eval_skip_abnormal"`
	EvalSkipHighPressure          bool   `yaml:"eval_skip_high_pressure" json:"eval_skip_high_pressure"`
	EvalHighPressureParallel      int    `yaml:"eval_high_pressure_parallel" json:"eval_high_pressure_parallel"`
	EvalHighPressureWindowSeconds int    `yaml:"eval_high_pressure_window_seconds" json:"eval_high_pressure_window_seconds"`
	EvalConcurrency               int    `yaml:"eval_concurrency" json:"eval_concurrency"`

	RetryMaxAttempts        int  `yaml:"retry_max_attempts" json:"retry_max_attempts"`
	RetryBackoffBaseSeconds int  `yaml:"retry_backoff_base_seconds" json:"retry_backoff_base_seconds"`
	RetryBackoffMaxSeconds  int  `yaml:"retry_backoff_max_seconds" json:"retry_backoff_max_seconds"`
	RetryBackoffJitter      bool `yaml:"retry_backoff_jitter" json:"retry_backoff_jitter"`

	OverloadRules []OverloadRule  `yaml:"overload_rules" json:"overload_rules"`
	Accounts      []AccountConfig `yaml:"accounts" json:"accounts"`
}

func DefaultConfig() Config {
	return Config{
		Strategy:                      "quota_pressure",
		Provider:                      "codex",
		CooldownDefaultSeconds:        60,
		CooldownFixedBonusSeconds:     30,
		MaxCooldownSeconds:            1800,
		AuthFailureCooldownSeconds:    1800,
		AutoDisableOnAuthFailure:      false,
		CircuitFailureThreshold:       5,
		CircuitOpenDurationSeconds:    1800,
		EscalationWindowSeconds:       600,
		EscalationThreshold:           3,
		EscalationMultiplier:          2.0,
		QuotaRefreshEnabled:           true,
		QuotaRefreshIntervalSeconds:   300,
		QuotaHardFloorPercent:         5,
		ExpiryPriorityEnabled:         true,
		ExpiryPriorityHorizonHours:    72,
		SessionAffinityEnabled:        true,
		SessionAffinityTTLSeconds:     3600,
		MaxParallelDefault:            5,
		FallbackReject:                true,
		InFlightTTLSeconds:            300,
		HistoryMaxEntries:             500,
		EvalHistoryMaxEntries:         500,
		EvalEnabled:                   true,
		EvalIntervalMinutes:           30,
		EvalModel:                     "",
		EvalEffort:                    "low",
		EvalSkipAbnormal:              true,
		EvalSkipHighPressure:          true,
		EvalHighPressureParallel:      2,
		EvalHighPressureWindowSeconds: 600,
		EvalConcurrency:               3,
		RetryMaxAttempts:              2,
		RetryBackoffBaseSeconds:       1,
		RetryBackoffMaxSeconds:        4,
		RetryBackoffJitter:            true,
		OverloadRules:                 []OverloadRule{},
		Accounts:                      []AccountConfig{},
	}
}

var currentConfig atomic.Value

func init() { currentConfig.Store(DefaultConfig()) }

func activeConfig() Config {
	if v, ok := currentConfig.Load().(Config); ok {
		return v
	}
	return DefaultConfig()
}

// accountOverride returns the per-account config for authID, or nil.
func accountOverride(cfg Config, authID string) *AccountConfig {
	for i := range cfg.Accounts {
		if cfg.Accounts[i].ID == authID {
			return &cfg.Accounts[i]
		}
	}
	return nil
}

// upsertAccountOverride merges a per-account patch into the active config and
// stores the result. Only non-nil/non-empty fields in patch are applied, so
// callers can update a single dimension (role, weight, etc.) without clobbering
// the rest of the override.
func upsertAccountOverride(authID string, patch AccountConfig) Config {
	cfg := activeConfig()
	idx := -1
	for i := range cfg.Accounts {
		if cfg.Accounts[i].ID == authID {
			idx = i
			break
		}
	}
	var ac AccountConfig
	if idx >= 0 {
		ac = cfg.Accounts[idx]
	} else {
		ac = AccountConfig{ID: authID}
	}
	if patch.Role != "" {
		ac.Role = patch.Role
	}
	if patch.Weight != nil {
		ac.Weight = patch.Weight
	}
	if patch.MaxParallel != nil {
		ac.MaxParallel = patch.MaxParallel
	}
	if patch.Enabled != nil {
		ac.Enabled = patch.Enabled
	}
	if idx >= 0 {
		cfg.Accounts[idx] = ac
	} else {
		cfg.Accounts = append(cfg.Accounts, ac)
	}
	cfg = sanitizeConfig(cfg)
	currentConfig.Store(cfg)
	return cfg
}

func effectiveRole(cfg Config, authID string) string {
	if a := accountOverride(cfg, authID); a != nil && a.Role != "" {
		return a.Role
	}
	return "primary"
}

func effectiveWeight(cfg Config, authID string) float64 {
	if a := accountOverride(cfg, authID); a != nil && a.Weight != nil {
		return *a.Weight
	}
	return 1.0
}

func effectiveMaxParallel(cfg Config, authID string) int {
	if a := accountOverride(cfg, authID); a != nil && a.MaxParallel != nil {
		if *a.MaxParallel > 0 {
			return *a.MaxParallel
		}
	}
	return cfg.MaxParallelDefault
}

func effectiveEnabled(cfg Config, authID string) bool {
	if a := accountOverride(cfg, authID); a != nil && a.Enabled != nil {
		return *a.Enabled
	}
	return true
}

func effectiveOverloadRules(cfg Config, authID string) []OverloadRule {
	if a := accountOverride(cfg, authID); a != nil && a.OverloadRules != nil {
		return a.OverloadRules
	}
	return cfg.OverloadRules
}

func effectiveExpiresAt(cfg Config, authID string) time.Time {
	a := accountOverride(cfg, authID)
	if a == nil || a.ExpiresAt == "" {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339, a.ExpiresAt)
	if err != nil {
		return time.Time{}
	}
	return t
}

// effectiveWindowSeconds returns the per-account quota window length in
// seconds, or 0 when unset. It is a tier-aware fallback for accounts whose
// usage endpoint does not disclose a reset time.
func effectiveWindowSeconds(cfg Config, authID string) int64 {
	if a := accountOverride(cfg, authID); a != nil && a.WindowSeconds != nil {
		return *a.WindowSeconds
	}
	return 0
}

// registerRequest is the plugin.register / plugin.reconfigure payload shape.
// config_yaml is base64-encoded YAML (the host marshals []byte as base64).
type registerRequest struct {
	ConfigYAML []byte `json:"config_yaml"`
}

func decodeConfig(raw []byte) (Config, error) {
	cfg := DefaultConfig()
	if len(raw) == 0 {
		return cfg, nil
	}
	if err := yaml.Unmarshal(raw, &cfg); err != nil {
		return Config{}, err
	}
	return sanitizeConfig(cfg), nil
}

func sanitizeConfig(cfg Config) Config {
	if cfg.Provider == "" {
		cfg.Provider = "codex"
	}
	if cfg.Strategy == "" {
		cfg.Strategy = "quota_pressure"
	}
	if cfg.CooldownDefaultSeconds <= 0 {
		cfg.CooldownDefaultSeconds = 60
	}
	if cfg.MaxCooldownSeconds <= 0 {
		cfg.MaxCooldownSeconds = 1800
	}
	if cfg.AuthFailureCooldownSeconds <= 0 {
		cfg.AuthFailureCooldownSeconds = 1800
	}
	if cfg.CircuitOpenDurationSeconds <= 0 {
		cfg.CircuitOpenDurationSeconds = 1800
	}
	if cfg.EscalationWindowSeconds <= 0 {
		cfg.EscalationWindowSeconds = 600
	}
	if cfg.EscalationThreshold <= 0 {
		cfg.EscalationThreshold = 3
	}
	if cfg.EscalationMultiplier <= 0 {
		cfg.EscalationMultiplier = 2.0
	}
	if cfg.QuotaRefreshIntervalSeconds <= 0 {
		cfg.QuotaRefreshIntervalSeconds = 300
	}
	if cfg.MaxParallelDefault <= 0 {
		cfg.MaxParallelDefault = 5
	}
	if cfg.InFlightTTLSeconds <= 0 {
		cfg.InFlightTTLSeconds = 300
	}
	if cfg.SessionAffinityTTLSeconds <= 0 {
		cfg.SessionAffinityTTLSeconds = 3600
	}
	if cfg.HistoryMaxEntries <= 0 {
		cfg.HistoryMaxEntries = 500
	}
	if cfg.EvalHistoryMaxEntries <= 0 {
		cfg.EvalHistoryMaxEntries = 500
	}
	if cfg.RetryMaxAttempts < 0 {
		cfg.RetryMaxAttempts = 2
	}
	if cfg.RetryBackoffBaseSeconds <= 0 {
		cfg.RetryBackoffBaseSeconds = 1
	}
	if cfg.RetryBackoffMaxSeconds <= 0 {
		cfg.RetryBackoffMaxSeconds = 4
	}
	if cfg.RetryBackoffMaxSeconds < cfg.RetryBackoffBaseSeconds {
		cfg.RetryBackoffMaxSeconds = cfg.RetryBackoffBaseSeconds
	}
	if cfg.QuotaHardFloorPercent < 0 {
		cfg.QuotaHardFloorPercent = 0
	}
	if cfg.QuotaHardFloorPercent > 100 {
		cfg.QuotaHardFloorPercent = 100
	}
	if cfg.CooldownFixedBonusSeconds < 0 {
		cfg.CooldownFixedBonusSeconds = 0
	}
	if cfg.EvalIntervalMinutes < 0 {
		cfg.EvalIntervalMinutes = 0
	}
	if cfg.EvalHighPressureParallel < 1 {
		cfg.EvalHighPressureParallel = 1
	}
	if cfg.EvalHighPressureWindowSeconds <= 0 {
		cfg.EvalHighPressureWindowSeconds = 600
	}
	if cfg.EvalConcurrency < 1 {
		cfg.EvalConcurrency = 3
	}
	if cfg.EvalEffort == "" {
		cfg.EvalEffort = "low"
	}
	for i := range cfg.Accounts {
		ac := &cfg.Accounts[i]
		if ac.Weight != nil && *ac.Weight < 0 {
			*ac.Weight = 0
		}
		if ac.MaxParallel != nil && *ac.MaxParallel <= 0 {
			ac.MaxParallel = nil
		}
		if ac.WindowSeconds != nil && *ac.WindowSeconds <= 0 {
			ac.WindowSeconds = nil
		}
	}
	return cfg
}

func configure(raw []byte) error {
	var req registerRequest
	if len(raw) > 0 {
		if err := json.Unmarshal(raw, &req); err != nil {
			return err
		}
	}
	hostCfg, err := decodeConfig(req.ConfigYAML)
	if err != nil {
		return err
	}
	cfg := hostCfg
	if persisted, ok := loadPersistedConfig(); ok {
		cfg = persisted
		logf("info", "using persisted plugin settings from %s; host config_yaml ignored", pluginStatePath())
	}
	applyConfig(cfg)
	return nil
}

// applyConfig stores, persists, and activates a fully-decoded config, then
// starts the long-lived background loops it requires. configure() and the
// management PUT endpoint share this path so every settings update behaves
// identically (persistence + refresher + history persister).
func applyConfig(cfg Config) Config {
	cfg = sanitizeConfig(cfg)
	currentConfig.Store(cfg)
	if err := persistConfig(cfg); err != nil {
		logf("warn", "persist settings failed: %v", err)
	}
	maybeStartRefresher()
	startAffinitySweeper()
	startDispatchLogPersister()
	startQuotaPersister()
	loadDispatchLog()
	loadQuotaSnapshots()
	trimDispatchLogNow()
	startEvalHistoryPersister()
	loadEvalHistory()
	trimEvalHistoryNow()
	startEvalLoop()
	loadEvalResults()
	return cfg
}

const defaultPluginStatePath = "/CLIProxyAPI/plugins/linux/amd64/codex-guard.state.json"

// pluginStatePath returns the persistent settings file location. The plugin
// cannot write its config back through the host ABI, so settings survive
// container restarts by reading/writing a JSON file next to the plugin .so.
func pluginStatePath() string {
	if p := strings.TrimSpace(os.Getenv("CODEX_GUARD_STATE_FILE")); p != "" {
		return p
	}
	if dir := persistentPluginDir(); dir != "" {
		return filepath.Join(dir, "codex-guard.state.json")
	}
	if dir := selfPluginDir(); dir != "" && !strings.HasPrefix(dir, "/tmp/") {
		return filepath.Join(dir, "codex-guard.state.json")
	}
	return defaultPluginStatePath
}

// persistentPluginDir finds the persistent plugins directory (the one the
// plugin host copies the .so from) by scanning mounts for a "plugins" mount
// point and looking for a codex-guard shared object under linux/<arch>.
func persistentPluginDir() string {
	data, err := os.ReadFile("/proc/self/mounts")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		mountpoint := fields[1]
		if !strings.Contains(strings.ToLower(mountpoint), "plugin") {
			continue
		}
		for _, arch := range []string{"amd64", "arm64", "386", "arm"} {
			dir := filepath.Join(mountpoint, "linux", arch)
			if hasCodexGuardSO(dir) {
				return dir
			}
		}
	}
	return ""
}

// hasCodexGuardSO reports whether dir contains a codex-guard plugin .so file.
func hasCodexGuardSO(dir string) bool {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return false
	}
	for _, e := range entries {
		name := e.Name()
		if strings.HasPrefix(name, "codex-guard") && strings.HasSuffix(name, ".so") {
			return true
		}
	}
	return false
}

// selfPluginDir locates the directory containing this plugin .so by scanning
// /proc/self/maps for a codex-guard shared object.
func selfPluginDir() string {
	data, err := os.ReadFile("/proc/self/maps")
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 6 {
			continue
		}
		path := fields[len(fields)-1]
		if strings.Contains(path, "codex-guard") && strings.HasSuffix(path, ".so") {
			return filepath.Dir(path)
		}
	}
	return ""
}

func loadPersistedConfig() (Config, bool) {
	path := pluginStatePath()
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, false
	}
	cfg, err := decodeConfig(raw)
	if err != nil {
		logf("warn", "persisted config %s invalid: %v", path, err)
		return Config{}, false
	}
	return cfg, true
}

func persistConfig(cfg Config) error {
	path := pluginStatePath()
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}
