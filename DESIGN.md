# Codex Guard — 需求与设计规格

> 本文档是唯一需求与设计参照，实现前先对齐本文。所有 SDK 事实均对照
> `github.com/router-for-me/CLIProxyAPI/v8`（v8.0.36）与参考实现
> `codex-quota-scheduler` 的真实源码逐字核实，非推测。

---

## 1. 概述与目标

以插件形式完全接管 CPA 的 Codex 账号调度，实现：

1. **完全接管调度** —— 插件注册 `Scheduler` 能力，对所有 `codex` 请求返回
   `Handled=true`，取代 CPA 内置调度器。
2. **可视化配置** —— 全局参数通过 `Metadata.ConfigFields` 暴露；每账号参数通过
   自建管理页编辑。
3. **配额感知调度** —— 按「剩余额度 ÷ 距重置时间」× 权重，结合主/备硬分层，
   最大程度消耗即将过期的 token，同时避免单账号被过度使用。
4. **策略保护** —— 理解上游错误，执行冷却/熔断/禁用/超负荷停用，最大程度
   避免账号违反 OpenAI 政策。

---

## 2. 已确认的需求决策（汇总）

| # | 项 | 决策 |
|---|---|---|
| 1 | 接管范围 | 仅 `codex`（`provider` 可配） |
| 2 | 主用/备用 | **硬分层**：主用层优先，全部不可用才轮到备用层 |
| 3 | 权重 | 独立字段（默认 1.0，`0` = 软停用），`排序得分 = 配额压力 × 权重` |
| 4 | 最大并行 | **并发请求口径**：pick +1 / 终态 −1，in-flight 带 TTL 防泄漏 |
| 5 | 超负荷口径 | **双口径**：每条规则带 `metric`（`token` / `quota_percent`） |
| 6 | 超负荷规则 | 支持多条，任一命中即触发（OR），取最大停用时长 |
| 7 | 兜底策略 | **默认直接拒绝**（`Reject=true` + 原因） |
| 8 | 订阅过期 | **独立维度**：快过期账号优先消耗（use-it-or-lose-it） |
| 9 | 会话亲和 | **路径 A：插件自维护** session→AuthID 映射（带 TTL） |
| 10 | 冷却固定等待 | **默认 30s**（`retry-after + 固定等待`，每类错误可配） |
| 11 | 升级策略 | **10min 内 3 次冷却 → 下一次冷却翻倍** |
| 12 | 管理页 | **整体配置 + 每账号粒度配置**；默认用整体配置，账号页可覆盖 |

---

## 3. 技术底座（已核实的 SDK 事实）

### 3.1 ABI 与能力

- 插件为 Go `-buildmode=c-shared` 动态库，导出
  `cliproxy_plugin_init / cliproxyPluginCall / cliproxyPluginFree /
  cliproxyPluginShutdown`。
- `pluginabi.ABIVersion = 1`，`pluginabi.SchemaVersion = 10`。
- 注册结构 `pluginapi.Plugin{Metadata, Capabilities, SchemaVersion}`。
- 本插件能力：
  - `Scheduler`（`scheduler.pick`）—— 主调度入口。
  - `UsagePlugin`（`usage.handle`）—— 主错误处理入口。
  - `RequestLifecyclePlugin`（`request.complete`）—— 终态补充 + 并发释放。
  - `StreamChunkInterceptor` —— 只读观测响应头配额。
  - 管理页 `management.register` / `management.handle`。

### 3.2 关键类型（字段名已核实）

```go
// 调度请求（无 session 字段，但 Options.Metadata 携带 canonical session id）
type SchedulerPickRequest struct {
    Plugin     Metadata
    Provider   string
    Providers  []string
    Model      string
    Stream     bool
    Options    SchedulerOptions          // { Headers http.Header; Metadata map[string]any }
    Candidates []SchedulerAuthCandidate  // { ID, Provider, Priority, Status, Attributes, Metadata }
}

type SchedulerPickResponse struct {
    AuthID          string
    DelegateBuiltin string
    Handled         bool
    Reject          bool
    RejectReason    string
    RejectCode      string
}

type UsageRecord struct {
    RequestID string; SessionID string; ParentSessionID string
    AuthID string; AuthIndex string; Provider string
    Failed bool; Failure UsageFailure      // { StatusCode int; Body string }
    Detail UsageDetail                    // { InputTokens, OutputTokens, TotalTokens ... }
    ResponseHeaders http.Header
}

type RequestCompletion struct {
    RequestID string; Outcome RequestCompletionOutcome
    StatusCode int; Error string; Metadata map[string]any
}
```

### 3.3 宿主 RPC（插件可调用）

| 方法常量 | 用途 |
|---|---|
| `host.auth.list` | 拿 codex 账号 roster（`files[{id, auth_index, provider}]`） |
| `host.auth.get` | 按 `auth_index` 拿凭据 JSON（`{access_token, refresh_token, id_token, account_id}`） |
| `host.auth.save` | 写回刷新后的凭据 |
| `host.http.do` | 代发上游 HTTP（插件不自建连接） |
| `host.log` | 日志 |
| `host.affinity.lookup` | 查询宿主会话亲和（本方案不依赖，见 6.1） |

### 3.4 会话标识（关键结论）

宿主在 pick 前会把规范化 session id 写入
`Options.Metadata[executor.CanonicalSessionIDMetadataKey]`（及
`ParentSessionIDMetadataKey`）。因此插件可在 `scheduler.pick` 阶段通过
`req.Options.Metadata` 读取 session id。

宿主的 session id 提取优先级（`ExtractSessionID`）：
`X-Claude-Code-Session-Id` → `metadata.user_id` → `Session-Id/Session_id`
(Codex) → `X-Session-ID` → `X-Session-Affinity` → `X-Client-Request-Id` →
body `session_id` → `prompt_cache_key`/conversation → `conversation_id` →
执行元数据 → 上下文派生 → 消息哈希。

> 依赖包：`github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor`（常量
> `CanonicalSessionIDMetadataKey` 等）与 `sdk/cliproxy/auth`（`CanonicalSessionID`）。
> 插件可直接 import（均为 `sdk/` 公共包）。

### 3.5 配额端点

- 配额：`https://chatgpt.com/backend-api/wham/usage`，解析
  `rate_limit.primary_window{used_percent, reset_after_seconds}`。
- 令牌刷新：`https://auth.openai.com/oauth/token`，`client_id=app_EMoamEEZ73f0CkXaXp7hrann`，
  `grant_type=refresh_token`。

---

## 4. 总体架构

```
main.go        ABI 入口（C 导出函数）+ host 回调封装
dispatch.go    host RPC 路由（plugin.register / scheduler.pick / usage.handle /
               request.complete / stream chunk / management.register / management.handle）
config.go      全局配置 + 每账号配置（解析/校验/默认值）
scheduler.go   调度器 Pick（亲和 → 筛选 → 分层 → 压力×权重 → 并发 → 兜底）
state.go       账号状态（冷却/熔断/禁用/配额/并发/in-flight/会话绑定，内存态）
errors.go      上游错误分类 + retry-after 解析
overload.go    超负荷滑动窗口计数（token + quota_percent 双口径）
affinity.go    会话亲和（session→AuthID 映射 + TTL + 失效）
refresh.go     主动配额轮询 + 令牌刷新（host.auth.* / host.http.do）
management.go  管理页（整体配置 + 每账号配置，读/写）
```

---

## 5. 配置模型

### 5.1 全局配置（`ConfigFields` 标量 + 规则数组）

| 配置 | 默认 | 说明 |
|---|---|---|
| `strategy` | `quota_pressure` | 压力公式：`quota_pressure` / `quota` / `expiry` |
| `provider` | `codex` | 接管范围 |
| `cooldown_default_seconds` | 60 | 上游未给 retry-after 时的兜底冷却 |
| `cooldown_fixed_bonus_seconds` | 30 | 「retry-after + 固定等待」的固定值 |
| `max_cooldown_seconds` | 1800 | 单次冷却上限 |
| `auth_failure_cooldown_seconds` | 1800 | 401/403 冷却（未启用禁用时） |
| `auto_disable_on_auth_failure` | false | 401/403 是否永久禁用 |
| `circuit_failure_threshold` | 5 | 连续 5xx 失败多少次熔断 |
| `circuit_open_duration_seconds` | 1800 | 熔断保持时长 |
| `escalation_window_seconds` | 600 | 升级策略观察窗口（10min） |
| `escalation_threshold` | 3 | 窗口内冷却触发次数 |
| `escalation_multiplier` | 2.0 | 超阈值后冷却翻倍倍数 |
| `quota_refresh_enabled` | true | 主动配额轮询开关 |
| `quota_refresh_interval_seconds` | 300 | 轮询间隔 |
| `quota_hard_floor_percent` | 5 | 剩余额度低于此百分比视为「额度耗尽」 |
| `expiry_priority_enabled` | true | 订阅过期优先开关 |
| `expiry_priority_horizon_hours` | 72 | 过期小于此时长的账号开始加权 |
| `session_affinity_enabled` | true | 会话亲和开关 |
| `session_affinity_ttl_seconds` | 3600 | 绑定 TTL |
| `max_parallel_default` | 5 | 每账号默认最大并发请求 |
| `fallback_reject` | true | 全不可用时是否直接拒绝 |
| `overload_rules` | `[]` | 全局默认超负荷规则（见 7.4） |

### 5.2 每账号配置（覆盖全局）

| 字段 | 默认 | 说明 |
|---|---|---|
| `id` | — | 账号 id（对应 `candidate.ID`） |
| `enabled` | true | 是否参与调度 |
| `role` | `primary` | `primary` / `backup` |
| `weight` | 1.0 | 权重（`0` = 软停用） |
| `max_parallel` | 全局默认 | 覆盖全局 `max_parallel_default` |
| `overload_rules` | 全局默认 | 覆盖/追加全局 `overload_rules` |
| `expires_at` | 无 | 订阅过期时间（可选） |

> 规则：**账号页未显式覆盖的字段一律继承全局配置**；`overload_rules` 采用
> 「账号显式设置了就整体替换全局，否则继承全局」。

---

## 6. 调度算法（`Scheduler.Pick` 完整流程）

按顺序执行，命中即返回：

```
1. 会话亲和       若 enabled 且 session id 存在，查绑定；绑定账号仍可选且未超并发 → 直接返回
2. 筛选           过滤不可用账号（见 6.2）
3. 分层           主用层 / 备用层（硬分层）
4. 排序           层内按「配额压力 × 权重」降序（见 6.3）
5. 并发上限       跳过已到 max_parallel 的账号，取下一个
6. 记录绑定       选定后写 session→AuthID 绑定、in-flight +1
7. 兜底           无可用 → Reject（见 6.5）
```

### 6.1 会话亲和（路径 A，自维护）

- **读取 session id**：优先
  `req.Options.Metadata[executor.CanonicalSessionIDMetadataKey]`，其次
  `ParentSessionIDMetadataKey`，最后回退 `Options.Headers`（按 3.4 的 header 优先级）。
- **数据结构**：`sessionBindings: map[sessionKey]binding{AuthID, LastSeen}`，
  `sessionKey = provider + "::" + sessionID`。
- **命中条件**：绑定存在、未超 TTL、且该 AuthID 当前仍「可筛选通过」且未超并发。
- **失效**：绑定账号一旦进入不可用/超并发，立即解绑重选。
- **TTL**：`LastSeen` 超 `session_affinity_ttl_seconds` 未命中则清理。
- **与并发上限关系**：粘住的账号已到 `max_parallel` 时解绑重选（亲和让位于并发保护）。

### 6.2 筛选（排除不可用）

| 状态 | 条件 |
|---|---|
| 禁用 | `Disabled == true`（401/403 且启用禁用，或手动禁用） |
| 冷却中 | `now < CooldownUntil` |
| 熔断中 | `now < CircuitOpenUntil` |
| 超负荷停用中 | `now < OverloadUntil` |
| 额度耗尽 | `RemainingPercent <= quota_hard_floor_percent` |
| 超并发 | `inFlight >= max_parallel` |

### 6.3 分层与排序

- **主用层**：`role == primary` 且通过筛选的账号。
- **备用层**：`role == backup`，仅当主用层为空时才进入。
- **层内排序**：`score = pressure(strategy, quota, expiry) × weight` 降序。
  - `quota_pressure`：`RemainingPercent ÷ max(hours-until-reset, 0.5)`
  - `expiry`（订阅过期优先开启时）：`expires_at` 越近分越高（use-it-or-lose-it），
    作为乘法因子叠加在压力分上。
- **平局**：`ResetAt` 更早 → `LastUsed` 更早（LRU 防饥饿）→ `ID` 字典序。

### 6.4 并发上限

- `inFlight: map[authID]count`。
- pick 时 `count++`；`request.complete`（任意终态）或 `usage.handle` 时 `count--`。
- 每条 in-flight 记录带租约 TTL，超时自动释放（防取消/拒绝请求漏减）。

### 6.5 兜底

- `fallback_reject=true`（默认）：返回
  `SchedulerPickResponse{Handled:true, Reject:true, RejectCode:"auth_unavailable",
  RejectReason:"all codex accounts unavailable"}`。
- `fallback_reject=false`：返回 `Handled:false` 交回 CPA 内置调度器。

---

## 7. 保护策略

### 7.1 错误分类与冷却

| 错误类 | 触发 | 动作 | 冷却公式 |
|---|---|---|---|
| 限流 | 429 | 冷却 | `retry-after`（缺失用 `cooldown_default_seconds`），封顶 `max_cooldown_seconds` |
| 过载 | 529 / `server_is_overloaded` | 冷却 | `retry-after + cooldown_fixed_bonus_seconds`，封顶 `max_cooldown_seconds` |
| 认证 | 401 / 403 | 冷却或禁用 | 未启用禁用 → `auth_failure_cooldown_seconds`；启用禁用 → `Disabled=true` |
| 服务器 | 500/502/503/504 | 熔断计数 | `recordFailure`，连续 `circuit_failure_threshold` 次 → 熔断 |
| 未知 | 其他 | 熔断计数 | 同上 |

`retry-after` 同时支持秒（`retry-after`）与毫秒（`retry-after-ms`）。

### 7.2 升级策略（冷却翻倍）

- 每个账号维护冷却事件时间戳队列（滑动窗口 `escalation_window_seconds`）。
- 若窗口内冷却次数 ≥ `escalation_threshold`（3 次），则**下一次**冷却时长
  × `escalation_multiplier`（2.0）。
- 翻倍后仍受 `max_cooldown_seconds` 封顶。

### 7.3 熔断器

- 连续非配额失败计数（`ConsecutiveFailures`）。
- 达 `circuit_failure_threshold` → 熔断 `circuit_open_duration_seconds`。
- 成功请求清零计数。半开探测（熔断结束后放 1 个试探请求）作为后续增强。

### 7.4 超负荷规则（多条，双口径）

规则结构：
```
{ id, metric: "token" | "quota_percent", window_seconds, limit, stop_seconds }
```

- `metric=token`：累计 `UsageRecord.Detail.TotalTokens`（带时间戳，滚动窗口）。
- `metric=quota_percent`：观测 `wham/usage` 的 `used_percent`（或剩余变化率）。
- **判定**：任一规则命中（窗口内消耗 > `limit`）→ 超负荷停用 `stop_seconds`
  （取命中规则中的最大 `stop_seconds`）。
- **恢复**：停用期间不计数；停用结束清零该窗口计数。
- 规则来源：全局 `overload_rules` 作为默认，账号 `overload_rules` 可整体覆盖。

### 7.5 配额硬下限

- `RemainingPercent <= quota_hard_floor_percent` → 视为「额度耗尽」，从可用集排除，
  与「超负荷」区分（恢复条件为 OpenAI 重置，非插件停用）。

### 7.6 订阅过期优先

- 若账号配置了 `expires_at` 且 `expiry_priority_enabled`：
  - 距过期 < `expiry_priority_horizon_hours` → 压力分乘以递增系数（越近越大）。
  - 目的：把快过期账号的额度优先烧掉。

---

## 8. 主动配额刷新

后台轮询（`refresh.go`）补齐被动观测的缺口：

```
host.auth.list → codex 账号（id + auth_index）
  → host.auth.get → 凭据 JSON（access_token/refresh_token/id_token/account_id）
    → 若 access_token 过期 → oauth/token 刷新 + host.auth.save 写回 + 重新启用账号
      → host.http.do GET wham/usage → 解析 rate_limit.primary_window → 写 AccountState.Quota
```

- 刷新成功且账号此前被禁用 → `reEnableAccount` 恢复。
- 所有外呼经 `host.http.do`，插件不自建连接、不落盘密钥。

---

## 9. 状态模型（内存态）

```go
type AccountState struct {
    AuthID              string
    Role                string    // primary | backup
    Weight              float64
    Enabled             bool
    MaxParallel         int
    CooldownUntil       time.Time // 冷却
    CircuitOpenUntil    time.Time // 熔断
    OverloadUntil       time.Time // 超负荷停用
    ConsecutiveFailures int       // 熔断计数
    CooldownEvents      []time.Time // 升级策略窗口
    Disabled            bool      // 永久禁用
    Quota               QuotaSnapshot // { RemainingPercent, ResetAt, ObservedAt }
    ExpiresAt           time.Time // 订阅过期
    InFlight            int       // 并发请求
    LastUsed            time.Time
}
```

会话绑定：`sessionBindings: map[sessionKey]{AuthID, LastSeen}`。

> 当前为内存态，插件重载即重置。持久化（磁盘/WAL）为后续阶段。

---

## 10. 管理页

通过 `management.register` 注册，`management.handle` 处理读写。分为两级：

1. **整体配置页**：5.1 表内所有全局参数（含全局默认 `overload_rules`）。
2. **每账号粒度配置页**：账号列表，每账号可覆盖
   `enabled / role / weight / max_parallel / overload_rules / expires_at`；
   未覆盖字段继承全局。

页面同时展示每账号**实时状态**（冷却/熔断/超负荷/并发/配额/排除原因），
并提供手动操作：启用/禁用/重置冷却/重置超负荷计数。

---

## 11. 可观测性与持久化

- **日志**：每次排除账号记录原因；每次冷却/熔断/超负荷/禁用记录事件与剩余时长。
- **排除原因**：`scheduler.pick` 保留完整筛选过程，供管理页与日志呈现。
- **持久化**（后续阶段）：冷却/熔断/超负荷计数/会话绑定/配额快照落盘，重载恢复。

---

## 12. 模块与实施阶段

| 阶段 | 内容 | 依赖 |
|---|---|---|
| P0 编译基线 | 现有 ABI/调度/冷却/配额刷新能编译通过 | Go 1.26 + CGO |
| P1 调度核心 | 主/备硬分层 + 权重 + 压力排序 + 并发上限 + 兜底 Reject | config/state |
| P2 保护策略 | 错误分类冷却 + 固定等待 + 升级翻倍 + 熔断 + 超负荷双口径 + 配额硬下限 | errors/overload |
| P3 会话亲和 | 自维护 session→AuthID 映射 + TTL + 失效 | affinity |
| P4 订阅过期 | expires_at 独立维度加权 | scheduler |
| P5 管理页 | 整体配置 + 每账号配置 + 实时状态 + 手动操作 | management |
| P6 加固 | 状态持久化 + 半开探测 + 日志/告警完善 | 全部 |

---

## 13. 风险与开放项

1. **编译未验证**：本机无 Go/gcc，字段名已对照 SDK v8.0.36，但需本地 `go build`
   验证，首次编译可能需微调常量名（如 `executor.*MetadataKey` 的实际字符串值）。
2. **会话 id 在 `Options.Metadata` 的精确 key**：依赖 `CanonicalSessionIDMetadataKey`
   常量，实现时 import `sdk/cliproxy/executor` 而非硬编码字符串。
3. **并发计数泄漏**：取消/拒绝请求可能不发终态事件，需 TTL 租约兜底，实际漏减率
   需上线后观察。
4. **管理页编辑体验**：`overload_rules` 为结构化数组，HTML 表单编辑体验需打磨。
5. **完全接管覆盖宿主原生亲和**：已通过路径 A 自维护亲和规避；但需确认
   `Options.Metadata` 在 Codex 实际流量下确实携带 canonical session id（P3 前实测）。
