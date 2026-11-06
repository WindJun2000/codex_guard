package main

import (
	"context"
	"encoding/json"
	"net/http"
	"sort"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/sdk/pluginapi"
)

const managementBasePath = "/codex-guard"

// RegisterManagement declares the plugin-owned management API and resource routes.
func RegisterManagement() pluginapi.ManagementRegistrationResponse {
	h := managementHandler{}
	return pluginapi.ManagementRegistrationResponse{
		Resources: []pluginapi.ResourceRoute{
			{Path: "/", Menu: "Codex Guard", Description: "Codex account scheduling console.", Handler: h},
		},
		Routes: []pluginapi.ManagementRoute{
			{Method: http.MethodGet, Path: managementBasePath + "/settings", Description: "Read plugin settings.", Handler: h},
			{Method: http.MethodPut, Path: managementBasePath + "/settings", Description: "Update plugin settings.", Handler: h},
			{Method: http.MethodGet, Path: managementBasePath + "/status", Description: "Account runtime status.", Handler: h},
			{Method: http.MethodPost, Path: managementBasePath + "/account", Description: "Per-account action.", Handler: h},
		},
	}
}

// managementHandler dispatches management requests by method + path.
type managementHandler struct{}

func (managementHandler) HandleManagement(ctx context.Context, req pluginapi.ManagementRequest) (pluginapi.ManagementResponse, error) {
	switch {
	case req.Method == http.MethodGet && req.Path == managementBasePath+"/settings":
		return jsonManagementResponse(http.StatusOK, activeConfig()), nil
	case req.Method == http.MethodPut && req.Path == managementBasePath+"/settings":
		return handlePutSettings(req)
	case req.Method == http.MethodGet && req.Path == managementBasePath+"/status":
		return jsonManagementResponse(http.StatusOK, statusPayload()), nil
	case req.Method == http.MethodPost && req.Path == managementBasePath+"/account":
		return handleAccountAction(req)
	case req.Path == "/" || req.Path == "":
		return htmlManagementResponse(managementHTML), nil
	default:
		return jsonManagementResponse(http.StatusNotFound, map[string]string{"error": "not found"}), nil
	}
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
	currentConfig.Store(cfg)
	maybeStartRefresher()
	return jsonManagementResponse(http.StatusOK, cfg), nil
}

func handleAccountAction(req pluginapi.ManagementRequest) (pluginapi.ManagementResponse, error) {
	var action struct {
		ID     string `json:"id"`
		Action string `json:"action"`
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
	default:
		return jsonManagementResponse(http.StatusBadRequest, map[string]string{"error": "unknown action"}), nil
	}
	return jsonManagementResponse(http.StatusOK, map[string]bool{"ok": true}), nil
}

type accountStatusView struct {
	AuthID           string  `json:"auth_id"`
	Role             string  `json:"role"`
	Weight           float64 `json:"weight"`
	Enabled          bool    `json:"enabled"`
	MaxParallel      int     `json:"max_parallel"`
	InFlight         int     `json:"in_flight"`
	RemainingPercent float64 `json:"remaining_percent"`
	ResetAt          string  `json:"reset_at,omitempty"`
	CooldownUntil    string  `json:"cooldown_until,omitempty"`
	CircuitOpenUntil string  `json:"circuit_open_until,omitempty"`
	OverloadUntil    string  `json:"overload_until,omitempty"`
	Disabled         bool    `json:"disabled"`
}

func statusPayload() map[string]any {
	cfg := activeConfig()
	states := snapshotStates()
	views := make([]accountStatusView, 0, len(states))
	for id, st := range states {
		views = append(views, accountStatusView{
			AuthID:           id,
			Role:             effectiveRole(cfg, id),
			Weight:           effectiveWeight(cfg, id),
			Enabled:          effectiveEnabled(cfg, id),
			MaxParallel:      effectiveMaxParallel(cfg, id),
			InFlight:         inFlightCount(id),
			RemainingPercent: st.Quota.RemainingPercent,
			ResetAt:          fmtTime(st.Quota.ResetAt),
			CooldownUntil:    fmtTime(st.CooldownUntil),
			CircuitOpenUntil: fmtTime(st.CircuitOpenUntil),
			OverloadUntil:    fmtTime(st.OverloadUntil),
			Disabled:         st.Disabled,
		})
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

const managementHTML = `<!doctype html>
<html lang="zh">
<head>
<meta charset="utf-8">
<title>Codex Guard</title>
<style>
body{font-family:system-ui,sans-serif;margin:2rem;color:#1a1a1a}
table{border-collapse:collapse;width:100%;margin-bottom:2rem}
th,td{border:1px solid #ddd;padding:6px 10px;text-align:left;font-size:14px}
th{background:#f5f5f5}
textarea{width:100%;height:480px;font-family:monospace;font-size:13px}
button{margin-top:10px;padding:8px 16px;cursor:pointer}
.badge{display:inline-block;padding:1px 8px;border-radius:10px;font-size:12px}
.ok{background:#e6f4ea;color:#1e7e34}
.bad{background:#fdecea;color:#c62828}
.warn{background:#fff3e0;color:#e65100}
</style>
</head>
<body>
<h1>Codex Guard</h1>
<h2>账号状态</h2>
<table id="status"><thead><tr><th>账号</th><th>角色</th><th>权重</th><th>并行</th><th>剩余%</th><th>冷却</th><th>熔断</th><th>超负荷</th><th>状态</th></tr></thead><tbody></tbody></table>
<h2>配置（JSON / YAML）</h2>
<textarea id="cfg"></textarea><br>
<button onclick="loadCfg()">刷新</button>
<button onclick="saveCfg()">保存配置</button>
<pre id="msg"></pre>
<script>
var BASE = "/v0/management/codex-guard";
function badge(t, cls, label){ return t ? '<span class="badge '+cls+'">'+label+'</span>' : ''; }
async function loadStatus(){
  var r = await fetch(BASE + "/status");
  var d = await r.json();
  var tb = document.querySelector("#status tbody");
  tb.innerHTML = "";
  (d.accounts||[]).forEach(function(a){
    var tr = document.createElement("tr");
    var st = a.disabled ? '<span class="badge bad">禁用</span>' : '<span class="badge ok">可用</span>';
    tr.innerHTML = "<td>"+a.auth_id+"</td><td>"+a.role+"</td><td>"+a.weight+"</td><td>"+a.in_flight+"/"+a.max_parallel+"</td><td>"+a.remaining_percent.toFixed(1)+"</td><td>"+badge(a.cooldown_until,"warn","冷却")+"</td><td>"+badge(a.circuit_open_until,"warn","熔断")+"</td><td>"+badge(a.overload_until,"warn","超负荷")+"</td><td>"+st+"</td>";
    tb.appendChild(tr);
  });
}
async function loadCfg(){
  var r = await fetch(BASE + "/settings");
  var d = await r.json();
  document.querySelector("#cfg").value = JSON.stringify(d, null, 2);
  document.querySelector("#msg").textContent = "";
}
async function saveCfg(){
  var body = document.querySelector("#cfg").value;
  var r = await fetch(BASE + "/settings", {method:"PUT", headers:{"Content-Type":"application/json"}, body: body});
  var d = await r.json();
  document.querySelector("#msg").textContent = r.ok ? "已保存" : ("失败: " + JSON.stringify(d));
  loadStatus();
}
loadStatus(); loadCfg();
</script>
</body>
</html>
`
