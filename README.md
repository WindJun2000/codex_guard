# Codex Guard

一个适用于 **CLIProxyAPI (CPA)** 的 Codex 账号调度与保护插件。参考
`codex-quota-scheduler` 的架构，实现三个核心目标：

1. **按配额调度** —— 优先把请求分给「剩余额度多 + 即将过期」的账号，尽可能把
   即将过期的 token 消耗掉，同时避免单账号被过度使用。
2. **理解上游错误** —— 解析 OpenAI / Codex / Anthropic 返回的各种错误，区分
   限流(429)、过载(529)、认证失败(401/403)、服务器错误(5xx)。
3. **自动冷却与熔断** —— 按上游返回的 `retry-after` 时间对账号冷却；认证失败
   自动禁用；服务器错误熔断，最大程度避免账号因异常请求违反 OpenAI 政策。

## 调度策略

插件注册 `Scheduler` 能力，在 CPA 内置调度器之前挑选账号。每层决策如下：

1. **过滤不可用账号**：跳过处于冷却中、熔断中、已禁用的账号。
2. **按策略排序**（`strategy` 配置）：
   - `quota_pressure`（默认）：`剩余额度百分比 ÷ 距重置小时数`，值越大越优先。
     同时兼顾「剩余多（fill-first）」和「快过期（别浪费）」。
   - `quota`：剩余额度多的优先。
   - `expiry`：重置时间早的优先。
3. 平局时按「更早重置 → 账号 ID」稳定排序。

当所有账号都不可用时，返回 `Handled=false`，交回 CPA 内置调度器兜底。

## 错误处理与冷却

插件同时注册 `UsagePlugin`（主入口）和 `RequestLifecyclePlugin`（补充），
在请求结束/失败时按错误分类执行策略：

| 错误 | 动作 |
|------|------|
| 429 限流 / 配额耗尽 | 按 `retry-after`（缺失则用 `cooldown_default_seconds`）冷却该账号 |
| 529 过载（`server_is_overloaded`） | 按 `retry-after` 冷却该账号 |
| 401 / 403 认证失败 | 默认冷却 `auth_failure_cooldown_seconds` 后自动恢复；`auto_disable_on_auth_failure=true` 时改为永久禁用 |
| 500 / 502 / 503 / 504 | 计入熔断器，连续 `circuit_failure_threshold` 次后熔断 |
| 成功 | 清除冷却/失败计数 |

`retry-after` 同时支持秒（`"retry-after"`）和毫秒（`"retry-after-ms"`）两种写法，
并受 `max_cooldown_seconds` 上限约束。

## 主动配额刷新

调度器的被动证据（响应头 `x-codex-*`、候选 `Metadata`）只在实际有流量命中某
账号时才会更新。插件内置一个后台轮询器补齐这个缺口：

1. 通过 host RPC `host.auth.list` 拿到所有 `codex` 账号（`id` + `auth_index`）。
2. 通过 `host.auth.get` 取到每个账号的凭据 JSON，解析 `access_token` /
   `refresh_token` / `id_token` / `account_id`。
3. 若 `access_token` 已过期，用 `refresh_token` 走
   `https://auth.openai.com/oauth/token` 换新，并通过 `host.auth.save` 写回。
4. 用新 token 请求 `https://chatgpt.com/backend-api/wham/usage`，解析
   `rate_limit.primary_window` 的 `used_percent`，以及重置时间（优先
   `reset_at`，次选 `reset_after_seconds`），写入 `AccountState.Quota` 供调度
   排序使用；窗口长度 `window_seconds` 仅作为兜底提示。

> 说明：所有外呼都通过 host RPC `host.http.do` 由 CPA 代发，插件不自己建立
> TCP 连接，也不额外持久化密钥。刷新成功会同时把「已禁用但凭据已修复」的
> 账号重新启用（`reEnableAccount`）。

### 订阅档位适配

插件不按 `plus` / `pro` 等档位名硬编码，而是依赖每个账号自己的
`used_percent` 与重置时间做归一化排序，因此 Plus 的 5 小时周窗口、Pro 的
7 天窗口等都能用同一套逻辑处理。当 `wham/usage` 未返回重置时间时，可在
账号配置里显式声明窗口长度作为兜底：

```yaml
accounts:
  - id: acc_plus
    window_seconds: 18000    # 5 小时窗口
  - id: acc_pro
    window_seconds: 604800   # 7 天窗口
```

> 该兜底把窗口长度当作剩余时间上限估算；只要 endpoint 正常返回
> `reset_after_seconds` 或 `reset_at`，就会优先使用真实重置时间，不会用到
> `window_seconds`。缺失重置时间且未配置 `window_seconds` 时，回落到纯
> 剩余百分比排序，不再猜测固定 5 小时。

## 编译

需要 Go 1.26+、CGO 与 C 编译器（gcc/clang）。

### Windows（PowerShell）

```powershell
.\build.ps1
# 产出 dist/codex-guard.dll
```

### Linux / macOS

```bash
make build
# 产出 codex-guard.so / codex-guard.dylib
```

## 安装

把编译产物放到 CPA 的插件目录：

- Linux:  `codex-guard.so` → `CLIProxyAPI/plugins/linux/amd64/`
- macOS:  `codex-guard.dylib` → `CLIProxyAPI/plugins/darwin/arm64/`
- Windows: `codex-guard.dll` → `CLIProxyAPI/plugins/windows/amd64/`

然后在 CPA 配置里启用：

```yaml
plugins:
  enabled: true
  configs:
    codex-guard:
      enabled: true
```

> ⚠️ 与 `codex-quota-scheduler` 一样，插件在 CPA 进程内以完整权限运行。
> 请通过 CPA 插件商店或可信来源安装，只启用你信任的插件。

## 配置项

| 配置 | 默认值 | 说明 |
|------|--------|------|
| `strategy` | `quota_pressure` | `quota` / `expiry` / `quota_pressure` |
| `provider` | `codex` | 只调度该 provider 的账号 |
| `cooldown_default_seconds` | `60` | 上游未给 retry-after 时的兜底冷却秒数 |
| `max_cooldown_seconds` | `1800` | 单次冷却上限（30 分钟） |
| `circuit_failure_threshold` | `5` | 连续失败多少次后熔断（0 关闭熔断） |
| `circuit_open_duration_seconds` | `1800` | 熔断保持时长 |
| `auto_disable_on_auth_failure` | `false` | 401/403 时永久禁用（`false` 时改为冷却后自动恢复） |
| `auth_failure_cooldown_seconds` | `1800` | 401/403 且未启用禁用时的冷却秒数 |
| `quota_refresh_enabled` | `true` | 是否启用后台主动配额轮询 |
| `quota_refresh_interval_seconds` | `300` | 配额轮询间隔 |

## 架构说明

```
main.go        ABI 入口（C 导出函数）+ host 回调封装
dispatch.go    路由 host RPC（plugin.register / scheduler.pick / usage.handle ...）
config.go      配置加载与默认值
scheduler.go   调度器（Scheduler.Pick）
lifecycle.go   错误处理（UsagePlugin / RequestLifecyclePlugin / StreamChunkInterceptor）
state.go       账号状态（冷却 / 熔断 / 禁用 / 配额，内存态）
errors.go      上游错误分类与 retry-after 解析
refresh.go     主动配额轮询 + 令牌刷新（host.auth.* / host.http.do）
```

> 当前为 MVP：账号状态保存在内存，插件重载后重置。配额来源为「主动轮询
> `wham/usage`」+「响应头 `x-codex-*`」+「候选 `Metadata`」三者互补；账号的
> 令牌刷新只做「过期即换新」，尚未实现 `codex-quota-scheduler` 那样的指纹
> 校验、事务写回与磁盘持久化，如需强一致可参照其 `credential_management.go`
> / `credential_wal.go` 扩展。
