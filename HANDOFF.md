# Codex Guard — 上下文交接文档（供新环境恢复用）

> 阅读本文件即可完全恢复上下文。任务已实现但**从未编译**（原环境无 Go/gcc）。
> 你的目标：在新环境（Go 1.26+ / CGO / gcc）完成 `go build` 并把报错清零。
> 参考源码已下载在 `C:\code\cpa\_sdk_ref\`（含 SDK 与 codex-quota-scheduler 全量源码）。

---

## 1. 任务一句话

为 **CLIProxyAPI (CPA)** 编写一个 Go 动态库插件 `codex-guard`，**完全接管 codex 账号调度**，
实现配额感知调度 + 会话亲和 + 错误冷却/熔断/超负荷保护 + 自建管理页。

---

## 2. 已确认的需求决策（12 项，已拍板，勿再讨论）

1. 接管范围：仅 `codex`（`provider` 可配）
2. 主/备：**硬分层**（主用层优先，全不可用才轮备用）
3. 权重：独立字段（默认 1.0，0=软停用），`得分 = 配额压力 × 权重`
4. 最大并行：**并发请求口径**（pick +1 / 终态 −1，TTL 防泄漏）
5. 超负荷：**双口径**（每条规则带 `metric`: token / quota_percent）
6. 超负荷规则：多条，OR 触发，取最大停用时长
7. 兜底：**默认直接拒绝**（Reject=true）
8. 订阅过期：独立维度，快过期优先消耗
9. 会话亲和：**路径 A 插件自维护**（非复用宿主）
10. 冷却固定等待：**默认 30s**
11. 升级策略：**10min 内 3 次冷却 → 下次翻倍**
12. 管理页：整体配置 + 每账号粒度配置，账号未覆盖继承全局

完整设计见同目录 `DESIGN.md`。

---

## 3. 代码现状清单（全部已写，未编译）

| 文件 | 角色 | 关键导出/内容 |
|---|---|---|
| `main.go` | ABI 入口 | C 导出函数、`callHostRaw`/`callHostJSON`/`logf`、host 回调 |
| `dispatch.go` | RPC 路由 | `handleMethod` 分发、`PluginRegistration()`、ConfigFields |
| `config.go` | 配置 | `Config`/`OverloadRule`/`AccountConfig`、`decodeConfig`(yaml)、`effective*` 继承函数 |
| `scheduler.go` | 调度 | `Pick` 全流程、`buildPool`、`pressure`、`sessionIDFromRequest`、`selectable`、`quotaFromMetadata`、`numberFromAny` |
| `state.go` | 状态 | `AccountState`、`applyCooldown`(升级翻倍)、`recordFailure`、`acquireSlot/releaseSlot`、`updateQuota`、`applyOverload` |
| `errors.go` | 错误分类 | `classifyError`、`ErrorKind`、`extractRetryAfter` |
| `lifecycle.go` | 错误处理 | `usageService.HandleUsage`、`applyFailurePolicy`、`lifecycleService`、`streamObserver`、`quotaFromHeaders` |
| `affinity.go` | 会话亲和 | `affinityLookup/Bind/Forget`，`affinityBinding` 映射 |
| `overload.go` | 超负荷 | `recordTokenUsage`、`recordQuotaObservation`、`evaluateOverload`、`maybeApplyOverload` |
| `refresh.go` | 主动配额刷新 | host.auth.list/get + oauth/token + wham/usage，`quotaRefresher` |
| `management.go` | 管理页 | `RegisterManagement()`、`managementHandler`、`statusPayload`、HTML |
| `go.mod` | 依赖 | `CLIProxyAPI/v8 v8.0.36` + `gopkg.in/yaml.v3 v3.0.1` |
| `build.ps1` / `Makefile` | 构建 | `-buildmode=c-shared` |

---

## 4. 已核实的 SDK 事实（关键，避免重新推导）

### 4.1 ABI 与常量
- `pluginabi.ABIVersion = 1`，`pluginabi.SchemaVersion = 10`
- C 导出：`cliproxy_plugin_init / cliproxyPluginCall / cliproxyPluginFree / cliproxyPluginShutdown`
- `plugin.init` 必须把 `plugin.call / free_buffer / shutdown` 指向 C 函数指针（不能为 nil）

### 4.2 host RPC 方法名（pluginabi 常量，已核实精确字符串）
```
MethodPluginRegister      = "plugin.register"
MethodPluginReconfigure   = "plugin.reconfigure"
MethodPluginQuiesce       = "plugin.quiesce"
MethodPluginShutdown      = "plugin.shutdown"
MethodSchedulerPick       = "scheduler.pick"
MethodUsageHandle         = "usage.handle"
MethodRequestComplete     = "request.complete"
MethodResponseInterceptStreamChunk = "response.intercept_stream_chunk"
MethodManagementRegister  = "management.register"
MethodManagementHandle    = "management.handle"
MethodHostAuthList        = "host.auth.list"
MethodHostAuthGet         = "host.auth.get"
MethodHostAuthSave        = "host.auth.save"
MethodHostHTTPDo          = "host.http.do"
MethodHostLog             = "host.log"
MethodHostAffinityLookup  = "host.affinity.lookup"
MethodHostRoutingResetCooldown = "host.routing.reset_cooldown"
```

### 4.3 响应信封
```go
// pluginabi.Envelope
type Envelope struct {
    OK     bool            `json:"ok"`
    Result json.RawMessage `json:"result,omitempty"`
    Error  *Error          `json:"error,omitempty"`
}
// pluginabi.Error: Code string; Message string; Retryable bool; HTTPStatus int; RetryAfterSeconds int
```
宿主所有回调响应都包在 Envelope 里；插件侧 `callHostJSON` 负责拆 `env.Result`。

### 4.4 关键类型字段（pluginapi）
```go
SchedulerPickRequest{ Plugin, Provider, Providers, Model, Stream,
    Options SchedulerOptions{ Headers http.Header; Metadata map[string]any }, Candidates []SchedulerAuthCandidate }
SchedulerAuthCandidate{ ID, Provider, Priority, Status, Attributes, Metadata }
SchedulerPickResponse{ AuthID, DelegateBuiltin, Handled, Reject, RejectReason, RejectCode }
UsageRecord{ RequestID, SessionID, ParentSessionID, AuthID, AuthIndex, Provider,
    Failed bool, Failure UsageFailure{StatusCode int; Body string},
    Detail UsageDetail{InputTokens, OutputTokens, ReasoningTokens, TotalTokens ...}, ResponseHeaders }
RequestCompletion{ RequestID, Outcome RequestCompletionOutcome, StatusCode, Error, Metadata }
RequestCompletionOutcome: RequestCompletionSucceeded/Failed/Rejected/Canceled
HostAuthGetRequest{ AuthIndex }  →  HostAuthGetResponse{ AuthIndex, Name, Path, JSON json.RawMessage }
HostAuthSaveRequest{ Name, JSON }  →  HostAuthSaveResponse{ Name, Path }
HTTPRequest{ Method, URL, Headers http.Header, Body []byte }  →  HTTPResponse{ StatusCode, Headers, Body }
ManagementRequest{ Method, Path, Headers, Query url.Values, Body []byte }
ManagementResponse{ StatusCode, Headers http.Header, Body []byte }
ManagementRegistrationResponse{ Routes []ManagementRoute, Resources []ResourceRoute }
ManagementRoute{ Method, Path, Menu, Description, Handler ManagementHandler }
ConfigFieldType: String / Number / Integer / Boolean / Enum / Array / Object
```

### 4.5 会话标识（关键结论）
- `SchedulerPickRequest` **没有** session 字段；但宿主在 pick 前把规范化 session id 写入
  `Options.Metadata["canonical_session_id"]`（还有 `"parent_session_id"`）。
- 精确键值（已核实 `sdk/cliproxy/executor`）：
  `CanonicalSessionIDMetadataKey = "canonical_session_id"`
  `ParentSessionIDMetadataKey = "parent_session_id"`
  `SelectionIDMetadataKey = "selection_id"`
  `SessionAffinityProviderMetadataKey = "session_affinity_provider"`
- 代码里硬编码了 `"canonical_session_id"` / `"parent_session_id"`（见 scheduler.go）。

### 4.6 配置 wire 格式（关键）
- `plugin.register` 请求是 JSON：`{"config_yaml": "<base64 编码的 YAML>"}`
  （宿主侧 `ConfigYAML []byte` 序列化为 base64；插件侧 `[]byte` 自动 base64 解码）。
- 用 `gopkg.in/yaml.v3` 的 `yaml.Unmarshal` 解析（YAML 是 JSON 超集，故也接受 JSON）。
- 代码：`registerRequest{ ConfigYAML []byte json:"config_yaml" }` + `decodeConfig`。

### 4.7 端点
```
配额:   GET https://chatgpt.com/backend-api/wham/usage
         解析 rate_limit.primary_window{used_percent, reset_after_seconds}
令牌:   POST https://auth.openai.com/oauth/token
         client_id=app_EMoamEEZ73f0CkXaXp7hrann, grant_type=refresh_token, scope=openid profile email
凭据 JSON: {access_token, refresh_token, id_token, account_id}
请求头:  Authorization: Bearer <token>; Chatgpt-Account-Id: <account_id>
```

---

## 5. 关键实现逻辑速查

- **调度顺序**：亲和命中 → 筛选 → 硬分层 → `pressure×weight` 排序 → 并发上限 → 兜底。
- **筛选排除**：disabled / 冷却中 / 熔断中 / 超负荷中 / `InFlight>=MaxParallel` / `剩余%<=QuotaHardFloorPercent`。
- **pressure**：`quota_pressure` = `剩余% ÷ max(距重置小时,0.5)`；`quota` = 剩余%；`expiry` = `-距重置分钟`；再叠加订阅过期 boost（×[1,2]）。
- **冷却**：429→`retry-after`(缺失用默认)；529→`retry-after + 30`；401/403→冷却或禁用；5xx→熔断计数。
- **升级**：`applyCooldown` 内，窗口内已有 ≥3 次冷却事件 → 本次 ×2。
- **超负荷**：`token` 事件累计窗口内 tokens；`quota_percent` 用窗口内剩余% 最大-最小差。OR 触发取最大 stop。
- **并发**：pick `acquireSlot(+1)`，request.complete 任意终态 `releaseSlot(-1)`（带 `>0` 守卫防负）。
- **失败处理唯一入口**：`usage.handle` 调 `applyFailurePolicy`；`request.complete` 只 releaseSlot+recordSuccess，不重复应用失败策略（避免双重冷却）。

---

## 6. 已知风险与开放项（编译/上线前必查）

1. **未编译**：字段名/常量名已逐一对照 SDK 源码，但可能有零星偏差。报错时查 `C:\code\cpa\_sdk_ref\pluginapi_types.go` / `pluginabi_types.go`（本地已下载）。
2. **go.sum 缺 yaml.v3 校验和**：构建前必须 `go mod tidy`（或 `go mod download gopkg.in/yaml.v3`）。
3. **config_yaml 是 base64**：若 `plugin.register` 报配置解析错误，优先怀疑 base64 假设（对照参考实现 `cqs_dispatch.go:38` 的 `ConfigYAML []byte`）。
4. **管理页 HTML 的 fetch 地址**：硬编码 `/v0/management/codex-guard`，best-effort；JSON API（settings/status/account）是主体，HTML 取不到数据改 URL 前缀即可。
5. **并发计数泄漏**：取消/拒绝请求可能不发终态事件，InFlight 只增不减；设计预留 TTL 租约（P6），MVP 未实现。
6. **未持久化**：所有状态内存态，插件重载即重置（P6）。
7. **未实现**：熔断半开探测（P6）、状态落盘、`host.routing.reset_cooldown` 复用。
8. 未使用函数 `parseErrorPayload`(errors.go)、`redact`(lifecycle.go) 是合法的（Go 不报错）。

---

## 7. 编译步骤（新环境执行）

```powershell
cd C:\code\cpa\codex-guard
go mod tidy          # 必须：补 yaml.v3 的 go.sum
.\build.ps1          # Windows → dist/codex-guard.dll
# Linux/macOS: make build  → codex-guard.so / .dylib
```

预期报错类别与修法：
- `missing go.sum entry` → `go mod tidy`
- `imported and not used` / `declared and not used` → 删对应 import/变量
- `undefined: xxx` / 字段不存在 → 到 `_sdk_ref` 核对真实类型，改字段名
- `constant string "xxx"` 拼写 → 对照本文 §4.2 的精确字符串

---

## 8. 参考资源位置

- SDK 类型：`C:\code\cpa\_sdk_ref\pluginapi_types.go`、`pluginabi_types.go`
- 参考实现：`C:\code\cpa\_sdk_ref\cqs_*.go`（`cqs_dispatch.go`=路由/配置解析、`cqs_management.go`=管理页、`cqs_refresh.go`=刷新、`cqs_quota.go`=配额解析、`cqs_scheduler.go`=调度）
- 宿主源码：`C:\code\cpa\_sdk_ref\host_*.go`（`host_sdk_cliproxy_auth_selector.go`=会话亲和、`host_sdk_cliproxy_auth_conductor_selection.go`=Pick 构造、`host_sdk_cliproxy_executor_types.go`=metadata 键值）
- 设计文档：`C:\code\cpa\codex-guard\DESIGN.md`
