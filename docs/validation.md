# 验收记录

## 当前版本：真实模型扩测（2026-11-14）

复用已有本地 Qwen3-VL-8B，正式评测共 1382 次完成调用。数据、子集与生成参数提前固定，模型答错、输出截断和接口失败分别记录。

| 验收 | 规模及结果 |
| --- | --- |
| Go race / 真实 MariaDB、Redis | 49 项通过（含子测试），跳过 0 |
| HTTP 验收 | 21/21 通过 |
| 工具参数及流式评分回归 | Python 4 项通过 |
| GSM8K | 100 题、三路各 94/100 |
| BoolQ | 100 题、三路各 86/100 |
| BFCL 单轮固定子集 | 50 题、三路各 46/50，工具名与参数逐题 50/50 一致 |
| 真实模型负载 | 10 组、632/632 正式请求成功，所有网关账户和输出 Token 对账通过 |

题库扩测发现固定 15 秒响应头等待导致合法的普通生成超时；先保留初轮失败，再修复为按请求总预算等待，并完成同题及整轮复测。600 次问答复测接口错误为 0；数学题中的 1 次输出上限截断保留在每路分母。另有一道数学题在原轮 SSE 和 JSON 上答案不同，直连重复调用也复现了生成差异；录制的网关上游参数、原始响应、客户端文本和回执一致，原轮差异保留。

本机模型的单序列部署约为 0.6 请求/秒，并发 8 时观测到 1 个生成中、7 个排队。网关和模型的吞吐及延迟分别记录；此前模拟上游 10 RPS 长测测量的是网关和存储链路。

协议、逐组负载结果和题目差异见 [公开数据与真实负载记录](public-qa.md)。归档目录为 `/data/zjr/llm_gateway/checks/expanded/20261114T055004Z`，包含源码及二进制摘要、初轮失败、修复回归、正式评测和差异核查。

## 网关稳态验收（2026-11-14，扩测前构建）

本轮完成查询连接隔离和 Redis 限流幂等重试。负载实例的数据库连接总额仍为 16，分为事务 14、查询 2；限流操作在 3 秒总预算内至多尝试两次，丢失回包后复用原结果。数据库同步刷盘、Redis AOF 与原有 SLO 保持原值。

| 验收 | 结果 |
| --- | --- |
| Go race / 真实 MariaDB、Redis | 46 项通过（含子测试），跳过 0，10.220 秒 |
| 完整 HTTP，含既有本机模型普通及 SSE 调用 | 21/21，34.285 秒 |
| combined + UUID7 双实例业务与崩溃恢复 | 12/12，76.904 秒 |
| 持续负载 | 10 RPS、4 账户、32 VU、1800 秒，全部检查通过 |
| 请求 / 成功 / 失败 / 未发出迭代 | 18001 / 18001 / 0 / 0 |
| P95 / P99 | 148.29 / 350.50 ms，低于原有 1000 / 2000 ms 阈值 |
| 模型调用 / 结算 / 客户端交付 | 18001 次 / 216012 Token / 216012 Token，预热另计 192 Token |
| 本轮账户、账本、请求用量 | 四个账户逐一相等，预占、未结清请求和待重放回执均为 0 |
| 事务提交 / 死锁 | 36002 次（每次成功调用 2 次）/ 0 |
| 事务池 / 查询池累计等待 | 26 次、5.461 秒 / 0 次、0 秒 |
| 限流脚本尝试 / 重试 / 结果复用 | 18001 / 0 / 0；丢失回包路径由真实 Redis 故障回归验证 |
| RSS / goroutine 增长 | 5.31 MiB / -2，通过原有资源检查 |

负载目录为 `/data/zjr/llm_gateway/checks/soak/20261114T004828Z`，完整流程 1803.865 秒。k6 实际发出 18001 次，满足原有到达数量检查的 ±1 容差。宿主机采样 1802 次，采样错误 0，I/O full 单次采样峰值 59.57%。事务池曾在关联窗口中占满 14 个连接，查询池仍有独立额度，整个负载阶段没有查询池等待或入口拒绝。

源码摘要、实际负载二进制摘要、回归报告和最终逐账户核对保存在 `/data/zjr/llm_gateway/checks/entry-protection/20261114T003800Z_pool-rate`。源文件在持续负载期间保持一致，实际负载二进制与归档构建摘要相符。两项机制的作用由可控连接耗尽与回包丢失验证；本轮延迟对应当时的共享宿主机条件，各轮资源负载另行记录。

正常 API 已切换到 combined + UUID7，健康检查通过；主 API 的总连接额度为 32，分为事务 30、查询 2。实验发压器、隔离网关及协议上游已退出并清理。实现、故障场景与历史失败见 [准入及入口保护记录](admission-optimization.md)。

## 历史版本（2026-11-13）

- Go race / 真实 MariaDB、Redis：36 项检查通过（含子测试），无跳过，38.102 秒。
- UUIDv7 的完整 HTTP 验收：21/21 通过，34.469 秒，含既有本机模型调用。
- UUIDv7、combined 的崩溃及双实例业务验证：12/12 通过，66.607 秒。
- 新增鉴权前并发保护、查询及准入时间预算；真实连接池耗尽、账户锁等待、取消、恢复重试均有回归。
- UUID4 / UUID7 / UUID7 / UUID4 短对照：802/802 成功，无丢弃。每种版本各 401 次调用，缓冲池物理读取 891 / 24 页，该次归一化对照下降 97.31%。
- UUID7、combined 的 1800 秒负载已结束：18000 次发出，17967 次成功，33 次失败，1 次迭代未发出。P95 538.03 ms、P99 1057.25 ms，延迟目标通过；零失败及零丢弃未通过，结果保持 `completed_with_slo_failures`。
- 本轮模型调用 17967 次，结算及客户端交付均为 215604 Token；四个账户计数、账本与请求用量一致。预占、未结清请求、待重放回执均为 0，死锁 0，成功调用恰为两次事务提交。RSS 增长约 6.00 MiB、goroutine 增长 4，均通过原有增长检查。
- 当前正常 API 使用 UUID7，事务模式保留 split，健康检查通过；实验发压器、隔离网关及协议上游均已退出并清理。

源码摘要与当轮 Go / HTTP / 深度 / 对照报告归档于 `/data/zjr/llm_gateway/checks/entry-protection/20261113T150741Z_uuid7`。版本、实验条件、失败历史与物理读取解释见 [准入及入口保护记录](admission-optimization.md)。不同轮次的负载与 I/O 条件分别记录。

最终负载报告：`/data/zjr/llm_gateway/checks/soak/20261113T150806Z/report.json`。本次改动和回归验证已完成；严格持续负载 SLO 尚未全部达到，共享盘 I/O 下的数据库及 Redis 访问失败仍保留为待解决项。

## 历史功能与故障测试（2026-11-11）

历史快照导出时间：2026-11-11 08:04 UTC。对应 Go 源码、SQL 与依赖锁文件摘要：`abfd548f3ae32fe01549de580f0448a77bdea87bb26a6d27b88dc7a3986677fb`。

- Go race / 真实 MariaDB 集成：**passed**，8 组顶层测试；跳过 0，失败 0。
- HTTP / 故障验收：**passed**，21 / 21 组。
- 实测覆盖双实例幂等与额度竞争、Redis 中断、数据库中断与结算凭据重复回放、SIGKILL 与租约恢复、流式超时和取消。
- 真实本机 Qwen3-VL-8B 的普通 / SSE 响应均取得有效 usage，并与账户用量核对。

| HTTP 组 | 结果 | 秒 |
| --- | --- | ---: |
| `basic_security` | passed | 0.23 |
| `validation` | passed | 0.055 |
| `normal_usage_and_tools` | passed | 0.12 |
| `streaming` | passed | 0.128 |
| `fallback_and_circuit` | passed | 0.238 |
| `round_robin_and_disable` | passed | 0.273 |
| `rejection_and_oversize` | passed | 0.598 |
| `concurrent_idempotency` | passed | 0.95 |
| `concurrent_quota` | passed | 0.128 |
| `rate_limit` | passed | 0.183 |
| `missing_usage_and_resolution` | passed | 0.106 |
| `normalized_total` | passed | 0.204 |
| `actual_usage_over_reservation` | passed | 0.11 |
| `interrupted_streams` | passed | 0.124 |
| `deadlines_and_cancellation` | passed | 4.494 |
| `concurrency_bound` | passed | 0.253 |
| `redis_outage` | passed | 1.151 |
| `database_outage_receipt_replay` | passed | 8.513 |
| `process_kill_recovery` | passed | 8.553 |
| `real_local_model` | passed | 0.668 |
| `final_invariants` | passed | 0.053 |

## 历史短负载（2026-11-11）

上游为确定性的 CPU 协议服务，结果衡量网关和存储链路。

- 结果：**passed**；60 秒，10 RPS，4 账户，最大 16 并发。
- 已完成 / 成功 / 因客户端并发槽位不足未发出：600 / 600 / 0。
- 实际吞吐：10.003 RPS；P50 / P95 / P99：62.183 / 581.378 / 721.05 ms。
- 负载阶段实际结算 / 期望 Token：7200 / 7200。预热用量另计。
- RSS 增量：0.438 MiB；goroutine / FD 增量：-3.0 / -2.0。

| 检查 | 通过 |
| --- | --- |
| `all_requests_successful` | True |
| `p95_under_1000ms` | True |
| `p99_under_2000ms` | True |
| `target_rate_achieved` | True |
| `actual_usage_exact` | True |
| `upstream_calls_exact` | True |
| `no_reservation_leak` | True |
| `bounded_rss_growth` | True |
| `bounded_goroutine_growth` | True |
| `bounded_fd_growth` | True |
| `no_inflight_leak` | True |

## 调试与负载历史

初版把实例心跳写到请求表的二级索引范围，InnoDB 保留了它与结算事务的死锁记录。修复后实例心跳写独立 `owner_leases` 表，恢复时重新锁定并检查实例租约；回归测试覆盖心跳与结算并发、心跳不改写请求、活实例不被恢复以及过期实例按状态恢复。

随后把成功调用审计和结算合并到同一事务，正常调用路径减少一次同步提交。各轮结果保留，下面包括不达标数据；不同账户数和并发数应分别看待。

| 时间 UTC | 账户 | 并发 | 目标 RPS | 完成 | 实际 RPS | P95 ms | 结果 |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| 04:16:20 | 1 | 8 | 20 | 0 | — | — | failed |
| 04:29:18 | 1 | 8 | 20 | 803 | 13.247 | 1161.302 | failed |
| 04:39:15 | 4 | 8 | 20 | 1023 | 17.023 | 692.11 | failed |
| 04:50:42 | 4 | 16 | 20 | 913 | 14.998 | 1813.573 | failed |
| 05:02:11 | 4 | 16 | 20 | 1165 | 19.423 | 815.738 | failed |
| 05:05:52 | 4 | 16 | 10 | 600 | 9.997 | 337.93 | passed |
| 05:13:37 | 4 | 16 | 10 | 600 | 10.003 | 581.378 | passed |

原始报告、逐项检查、资源采样和日志保存在 `/data/zjr/llm_gateway/checks/` 与 `/data/zjr/llm_gateway/logs/`。死锁修复前证据保存于 `checks/deadlock-heartbeat-before.txt`。

这些是共享服务器上的短时开发验收结果，模型推理吞吐及长时运行需要使用对应场景单独测量。长时负载可以沿用同一工具，例如 `bash run.sh stability --duration 3600 --rps 10 --accounts 4 --concurrency 16`；用 `bash run.sh status` 和 `tail -f /data/zjr/llm_gateway/logs/stability.log` 查看结果。
