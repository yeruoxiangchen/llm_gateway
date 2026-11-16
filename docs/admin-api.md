# 管理 API

管理端使用 `Authorization: Bearer <管理员 Token>`。普通应用 Key 无法调用这些接口。
默认地址为 `http://127.0.0.1:29480`；数据库查询及本轮新增修改接口使用 5 秒总时间预算。

## 账户和 Key

| 接口 | 参数或返回内容 |
| --- | --- |
| `GET /admin/accounts` | `q`：名称包含的文本，最长 120 字节；`limit`、`cursor`：分页 |
| `GET /admin/accounts/{id}` | 账户名称、总额度、已用、临时占用和可用 Token |
| `GET /admin/accounts/{id}/keys` | `active=true/false` 可选；支持 `limit`、`cursor` |
| `GET /admin/keys/{id}` | Key 的 ID、账户 ID、名称、前缀、RPM、`active` |
| `PATCH /admin/keys/{id}` | 修改 `name` 和/或 `requests_per_minute`（1–1000000） |

列表查询不会返回完整 Key 或其摘要；Key 明文仍仅在原有签发接口中返回一次。
账户不存在时，其 Key 列表返回 404；已有账户没有 Key 时返回空数组。
修改 Key 的名称/RPM 不会重新启用已经撤销的 Key。撤销继续使用 `DELETE /admin/keys/{id}`。

```bash
curl -H "Authorization: Bearer $LLMGW_ADMIN_TOKEN" \
  'http://127.0.0.1:29480/admin/accounts?q=studio&limit=20'

curl -X PATCH -H "Authorization: Bearer $LLMGW_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"name":"studio-server","requests_per_minute":60}' \
  'http://127.0.0.1:29480/admin/keys/<key-id>'
```

账户创建、额度修改、Key 签发和撤销沿用 README 中的已有接口。

## 渠道配置修改

`GET /admin/providers/{id}` 获取单个渠道；`GET /admin/providers` 继续返回完整渠道列表。
`PATCH /admin/providers/{id}` 接受以下字段的任意非空组合：

| 字段 | 含义 |
| --- | --- |
| `name` | 渠道名称，1–120 字节 |
| `model_alias` | 对外模型名，1–100 字节 |
| `upstream_model` | 上游模型名，1–160 字节 |
| `base_url` | 上游 HTTP(S) API 地址，最长 500 字节；不能携带用户名、密码、查询参数或 fragment |
| `api_key` | 新上游密钥，最长 4096 字节；省略保留旧值，空字符串显式清空 |
| `priority` | 有符号 32 位整数，数值越大优先级越高 |
| `enabled` | 启用或停用 |
| `input_micro_usd_per_million_tokens` | 输入单价，0–1000000000 |
| `output_micro_usd_per_million_tokens` | 输出单价，0–1000000000 |

只修改传入字段，`0` 和 `false` 均作为有效值处理。空对象、`null`、未知字段和非法值返回 400，
在校验通过前不会修改任何配置。返回更新后的渠道对象，其中不包含上游密钥或加密密文。
相同值重复修改也返回成功；不存在的渠道返回 404。

```bash
curl -X PATCH -H "Authorization: Bearer $LLMGW_ADMIN_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"model_alias":"editing","priority":20,"enabled":true}' \
  'http://127.0.0.1:29480/admin/providers/<provider-id>'
```

原有 `{"enabled":false}` 请求保持可用。配置变化作用于后续读取配置的请求；
已经选定上游的请求继续使用其原配置，已保存的单价快照保持不变。
Key、渠道修改写入管理员操作日志，日志记录资源 ID；渠道日志额外记录是否更换凭证，不记录凭证值。

## 请求筛选和详情

`GET /admin/requests` 支持以下可组合条件：

| 参数 | 条件 |
| --- | --- |
| `account_id`、`key_id`、`provider_id` | 对应 UUID，精确匹配 |
| `model` | 对外模型别名，精确匹配 |
| `state` | `reserved`、`dispatched`、`pending_usage`、`settled`、`released` |
| `outcome` | `succeeded`、`failed`、`interrupted`、`cancelled` |
| `stream` | `true` 或 `false` |
| `http_status` | HTTP 状态码；`0` 表示尚无最终状态码 |
| `error_code` | 错误码，精确匹配 |
| `since`、`until` | RFC3339 时间，查询范围为 `[since, until)`；两者可独立传入 |
| `limit`、`cursor` | 分页 |

```bash
curl -H "Authorization: Bearer $LLMGW_ADMIN_TOKEN" \
  'http://127.0.0.1:29480/admin/requests?state=pending_usage&limit=20'

curl -H "Authorization: Bearer $LLMGW_ADMIN_TOKEN" \
  'http://127.0.0.1:29480/admin/requests/<request-id>'
```

详情保留原请求字段，额外返回 `attempts` 数组，每项包含上游渠道 ID、HTTP 状态码、错误、
开始和结束时间。尚未结束的尝试省略结束时间。列表与详情均不读取模型回答缓存，也不返回请求摘要、
幂等键或密钥。需要处理未知用量时，继续使用原有 `POST /admin/requests/{id}/resolve`。

## 分页约定

账户、Key、请求列表统一返回：

```json
{
  "data": [],
  "limit": 100,
  "has_more": false
}
```

`limit` 默认 100，允许 1–200。存在下一页时 `has_more=true` 并返回不透明的 `next_cursor`。
把该值作为下一次请求的 `cursor`，同时保留原筛选条件；最后一页省略 `next_cursor`。
游标绑定资源类型、所属账户及筛选条件，串用或改变筛选条件返回 400；可以改变每页数量。

账户、请求按创建时间倒序，同一时间按 ID 倒序；Key 按 ID 倒序。
翻页使用最后一条记录的位置，避免偏移分页在新记录插入后重复返回旧记录。
这是实时查询；需要查看变化后的请求状态时从第一页重新刷新。
重复或未知查询参数、非法时间、无效 UUID、损坏游标返回 400。

原有 `/admin/requests?account_id=...` 仍返回 `data` 数组，默认最多 100 条，新增分页字段为附加字段。

## 验证

`internal/gateway/admin_test.go` 使用真实隔离 MariaDB，经 HTTP handler 检查管理员鉴权、
账户/Key 隔离、同时间戳分页、跨账户游标拒绝、修改后鉴权使用新 RPM、撤销后不能因编辑重新启用、
渠道部分修改和密钥加密更新、请求组合筛选及详情内容保护。
`bash run.sh test` 会连同原有并发、调用和恢复回归一起执行。
