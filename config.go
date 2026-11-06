package main

import (
	"encoding/json"
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
	Role          string         `yaml:"role" json:"role"` // "primary" | "backup"
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

	OverloadRules []OverloadRule  `yaml:"overload_rules" json:"overload_rules"`
	Accounts      []AccountConfig `yaml:"accounts" json:"accounts"`
}

func DefaultConfig() Config {
	return Config{
		Strategy:                    "quota_pressure",
		Provider:                    "codex",
		CooldownDefaultSeconds:      60,
		CooldownFixedBonusSeconds:   30,
		MaxCooldownSeconds:          1800,
		AuthFailureCooldownSeconds:  1800,
		AutoDisableOnAuthFailure:    false,
		CircuitFailureThreshold:     5,
		CircuitOpenDurationSeconds:  1800,
		EscalationWindowSeconds:     600,
		EscalationThreshold:         3,
		EscalationMultiplier:        2.0,
		QuotaRefreshEnabled:         true,
		QuotaRefreshIntervalSeconds: 300,
		QuotaHardFloorPercent:       5,
		ExpiryPriorityEnabled:       true,
		ExpiryPriorityHorizonHours:  72,
		SessionAffinityEnabled:      true,
		SessionAffinityTTLSeconds:   3600,
		MaxParallelDefault:          5,
		FallbackReject:              true,
		InFlightTTLSeconds:          300,
		OverloadRules:               []OverloadRule{},
		Accounts:                    []AccountConfig{},
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
		return *a.MaxParallel
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
	for i := range cfg.Accounts {
		if cfg.Accounts[i].Role == "" {
			cfg.Accounts[i].Role = "primary"
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
	cfg, err := decodeConfig(req.ConfigYAML)
	if err != nil {
		return err
	}
	currentConfig.Store(cfg)
	maybeStartRefresher()
	return nil
}
