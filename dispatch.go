package main

import (
	"encoding/json"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginabi"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

// envelope is the plugin-side RPC response wrapper. The host unpacks OK/Result/Error.
type envelope struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *envelopeError  `json:"error,omitempty"`
}

type envelopeError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable,omitempty"`
}

func okEnvelope(result any) []byte {
	if result == nil {
		raw, _ := json.Marshal(envelope{OK: true})
		return raw
	}
	b, _ := json.Marshal(result)
	raw, _ := json.Marshal(envelope{OK: true, Result: b})
	return raw
}

func errorEnvelope(code, message string) []byte {
	raw, _ := json.Marshal(envelope{
		OK:    false,
		Error: &envelopeError{Code: code, Message: message},
	})
	return raw
}

// registration is the JSON-safe plugin registration payload. Capability fields
// must be booleans: the host decodes capabilities into bool flags, so we cannot
// marshal pluginapi.Capabilities (whose interface fields serialize as objects).
type registration struct {
	SchemaVersion uint32                 `json:"schema_version"`
	Metadata      pluginapi.Metadata     `json:"metadata"`
	Capabilities  registrationCapability `json:"capabilities"`
}

type registrationCapability struct {
	Scheduler                 bool `json:"scheduler"`
	SchedulerAcrossPriorities bool `json:"scheduler_across_priorities,omitempty"`
	RequestLifecyclePlugin    bool `json:"request_lifecycle_plugin"`
	StreamChunkInterceptor    bool `json:"response_stream_interceptor"`
	UsagePlugin               bool `json:"usage_plugin"`
	ManagementAPI             bool `json:"management_api"`
}

// handleMethod routes every host RPC to its handler.
func handleMethod(method string, request []byte) ([]byte, error) {
	switch method {
	case pluginabi.MethodPluginRegister, pluginabi.MethodPluginReconfigure:
		if err := configure(request); err != nil {
			return nil, err
		}
		return okEnvelope(PluginRegistration()), nil

	case pluginabi.MethodSchedulerPick:
		return handleSchedulerPick(request)

	case pluginabi.MethodRequestComplete:
		return handleRequestComplete(request)

	case pluginabi.MethodResponseInterceptStreamChunk:
		return handleStreamChunk(request)

	case pluginabi.MethodUsageHandle:
		return handleUsageHandle(request)

	case pluginabi.MethodManagementRegister:
		return okEnvelope(RegisterManagement()), nil

	case pluginabi.MethodManagementHandle:
		return handleManagementHandle(request)

	case pluginabi.MethodPluginQuiesce:
		return okEnvelope(map[string]any{}), nil

	case pluginabi.MethodPluginShutdown:
		cliproxyPluginShutdown()
		return okEnvelope(map[string]any{}), nil

	default:
		return errorEnvelope("unknown_method", "unknown method: "+method), nil
	}
}

// PluginRegistration returns the plugin declaration sent at register time.
func PluginRegistration() registration {
	return registration{
		SchemaVersion: pluginabi.SchemaVersion,
		Metadata: pluginapi.Metadata{
			Name:             "Codex Guard",
			Version:          "0.2.0",
			Author:           "WindJun2000",
			GitHubRepository: "https://github.com/WindJun2000/codex_guard",
			ConfigFields: []pluginapi.ConfigField{
				{Name: "strategy", Type: pluginapi.ConfigFieldTypeString, Description: "quota_pressure | quota | expiry"},
				{Name: "provider", Type: pluginapi.ConfigFieldTypeString, Description: "auth provider to schedule"},
				{Name: "cooldown_default_seconds", Type: pluginapi.ConfigFieldTypeNumber, Description: "fallback cooldown seconds"},
				{Name: "cooldown_fixed_bonus_seconds", Type: pluginapi.ConfigFieldTypeNumber, Description: "fixed wait added on overload cooldown"},
				{Name: "max_cooldown_seconds", Type: pluginapi.ConfigFieldTypeNumber, Description: "hard cap on cooldown"},
				{Name: "auth_failure_cooldown_seconds", Type: pluginapi.ConfigFieldTypeNumber, Description: "cooldown on 401/403 when not disabling"},
				{Name: "auto_disable_on_auth_failure", Type: pluginapi.ConfigFieldTypeBoolean, Description: "permanently park on 401/403"},
				{Name: "circuit_failure_threshold", Type: pluginapi.ConfigFieldTypeNumber, Description: "failures before circuit opens"},
				{Name: "circuit_open_duration_seconds", Type: pluginapi.ConfigFieldTypeNumber, Description: "circuit open duration"},
				{Name: "escalation_window_seconds", Type: pluginapi.ConfigFieldTypeNumber, Description: "escalation observation window"},
				{Name: "escalation_threshold", Type: pluginapi.ConfigFieldTypeNumber, Description: "cooldowns in window before escalation"},
				{Name: "escalation_multiplier", Type: pluginapi.ConfigFieldTypeNumber, Description: "cooldown multiplier once escalated"},
				{Name: "quota_refresh_enabled", Type: pluginapi.ConfigFieldTypeBoolean, Description: "background quota polling"},
				{Name: "quota_refresh_interval_seconds", Type: pluginapi.ConfigFieldTypeNumber, Description: "quota poll cadence"},
				{Name: "quota_hard_floor_percent", Type: pluginapi.ConfigFieldTypeNumber, Description: "remaining % below which account is exhausted"},
				{Name: "expiry_priority_enabled", Type: pluginapi.ConfigFieldTypeBoolean, Description: "prefer accounts nearing subscription expiry"},
				{Name: "session_affinity_enabled", Type: pluginapi.ConfigFieldTypeBoolean, Description: "sticky session-to-account binding"},
				{Name: "session_affinity_ttl_seconds", Type: pluginapi.ConfigFieldTypeNumber, Description: "affinity binding TTL"},
				{Name: "max_parallel_default", Type: pluginapi.ConfigFieldTypeNumber, Description: "default max concurrent requests per account"},
				{Name: "in_flight_ttl_seconds", Type: pluginapi.ConfigFieldTypeNumber, Description: "in-flight slot lease TTL seconds"},
				{Name: "fallback_reject", Type: pluginapi.ConfigFieldTypeBoolean, Description: "reject when all accounts unavailable"},
				{Name: "history_max_entries", Type: pluginapi.ConfigFieldTypeNumber, Description: "max dispatch history entries retained"},
				{Name: "eval_enabled", Type: pluginapi.ConfigFieldTypeBoolean, Description: "periodic degradation (candy+fingerprint) checks"},
				{Name: "eval_interval_minutes", Type: pluginapi.ConfigFieldTypeNumber, Description: "minutes between degradation checks (0 disables)"},
				{Name: "eval_model", Type: pluginapi.ConfigFieldTypeString, Description: "fallback model for degradation checks"},
				{Name: "eval_effort", Type: pluginapi.ConfigFieldTypeString, Description: "reasoning effort for degradation checks"},
				{Name: "eval_skip_abnormal", Type: pluginapi.ConfigFieldTypeBoolean, Description: "skip accounts in cooldown/circuit/overload"},
				{Name: "eval_skip_high_pressure", Type: pluginapi.ConfigFieldTypeBoolean, Description: "skip accounts with high concurrency"},
				{Name: "eval_high_pressure_parallel", Type: pluginapi.ConfigFieldTypeNumber, Description: "parallel threshold for high-pressure skip"},
				{Name: "eval_high_pressure_window_seconds", Type: pluginapi.ConfigFieldTypeNumber, Description: "high-pressure observation window seconds"},
				{Name: "retry_max_attempts", Type: pluginapi.ConfigFieldTypeNumber, Description: "extra retry attempts for safe transport/server errors"},
				{Name: "retry_backoff_base_seconds", Type: pluginapi.ConfigFieldTypeNumber, Description: "first retry backoff seconds"},
				{Name: "retry_backoff_max_seconds", Type: pluginapi.ConfigFieldTypeNumber, Description: "max retry backoff seconds"},
				{Name: "retry_backoff_jitter", Type: pluginapi.ConfigFieldTypeBoolean, Description: "add jitter to retry backoff"},
			},
		},
		Capabilities: registrationCapability{
			Scheduler:                 true,
			SchedulerAcrossPriorities: true,
			RequestLifecyclePlugin:    true,
			StreamChunkInterceptor:    true,
			UsagePlugin:               true,
			ManagementAPI:             true,
		},
	}
}
