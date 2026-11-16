# 企业多 Agent 实际接入

2026-11-16，已将现有“知页”企业应用的在线模型生成改为经过 Go 网关。

## 当前调用链

```text
用户 / 管理员 → 知页 HTTP API :8768
                       ↓
              business_native_v1
              DeepAgents 原生主管 / 专业 Agent
                       ↓
              Go 网关 :29480/v1
              独立应用账户、Key、渠道、用量记录
                       ↓
              现有 Qwen3-8B :18768/v1
```

对外模型别名为 `kb-qwen3-8b`，上游模型也是该名称。
Token 计数使用原 `:18768/tokenize`。应用新增可选的 `KB_TOKENIZE_API_KEY`，
让计数接口使用其自身凭证；未设置时沿用原来的共用凭证行为，空值表示无需认证。
生成请求通过应用自己的网关 Key 鉴权，网关管理 Token 额度。
本机模型的价格配置为零，Token 额度正常扣除。

应用在线配置保存在 `/data/zjr/enterprise_kb_agent/app.env`；
专用网关账户、Key 和渠道信息保存在该项目 `config/gateway-client.json`，权限为 0600。
原在线配置备份在本轮验证目录 `app.env.before`，同样为 0600。
原 `model.env` 保留；研究评测入口继续使用其各自的模型配置。

## 验证内容与结果

使用实际运行的服务、真实模型和独立测试资料，通过 HTTP 完成：

1. 管理员创建测试成员、上传并实际索引 VPN 文档。
2. 成员提出 Windows 11 / VPN HTTP 401 问题，原生 Agent 委派 evidence 专业 Agent 检索资料。
3. service 专业 Agent 创建唯一工单草稿，最终回答包含有效引用。
4. 通过成员身份显式提交草稿，管理员接单并将测试工单标为已解决。
5. 成员在同一会话追问，Agent 实际查询并返回已解决状态和管理员备注。
6. 从网关管理接口查询对应账户的调用记录，核对应用用量、请求用量及账户扣除量。

本轮 10 次模型请求全部完成；Agent、网关、账户新增消耗均为 28,268 Token，
临时占用额度为零。实际调用了 evidence、service 两种专业 Agent。
这是现有应用的接入与业务流程验证，使用测试文档和测试工单。

报告目录：`/data/zjr/llm_gateway/checks/enterprise-agent-20261116-v1/`。

- `report.md`：业务检查与两轮实际回答。
- `report.json`：逐项检查、请求编号、Token 核对及专业委派记录。
- `application-events.json`：本次应用事件。
- `connection.json`：模型、路由和账户 ID，不包含 Key。
- `app.env.before`：原配置，包含私密信息，仅保存在数据盘。

日志：`/data/zjr/llm_gateway/logs/enterprise-agent-20261116-v1.log`。

## 再次验证

使用新的输出目录运行，避免覆盖历史结果。脚本复用已配置的网关账户、现有应用和模型；
每次创建独立测试用户、文档与工单，并通过原生应用接口执行完整流程。

```bash
cd /home/zjr/Tracker/internship_project_lab/llm_gateway
source scripts/env.sh
/home/zjr/anaconda3/bin/python -B scripts/enterprise_agent_integration.py \
  --output /data/zjr/llm_gateway/checks/enterprise-agent-<新标签>
```

首次接入要求应用没有正在运行或排队的请求，备份配置后只重启应用进程。
生成任务超过十分钟会请求取消，失败会保存报告，不自动重跑。
在线应用继续通过 `enterprise_kb_agent/scripts/run.sh start|stop|status` 管理。
