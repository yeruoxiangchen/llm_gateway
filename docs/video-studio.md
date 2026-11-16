# 视频工作台接入

视频工作台使用独立的网关账户和服务端 Key。剪辑需求理解、剪辑命令生成及字幕翻译经 Go 网关调用既有本地模型：

```text
浏览器 → 视频工作台 API / 译制 Worker → Go 网关 :29480
                                          ├─ video-studio-planning → Qwen3.6-27B
                                          └─ video-studio-translation → Qwen3-VL-8B-Instruct
```

视频分析、Embedding、重排、ASR、Piper 和 FFmpeg 使用各自的专用服务。多模态原生叙事接口保持现有配置；本次网关接入覆盖文本规划与翻译调用。

## 配置与凭证

连接脚本位于 `scripts/video_studio_integration.py`，会在修改部署配置前检查视频任务与 Agent 执行均为空闲。原 `.env` 备份及生成的 Key 保存在数据盘，权限为 0600。对外报告只包含账户、渠道、请求编号和用量。

视频工作台设置：

- `CAMCAT_LLM_BASE_URL`、`CAMCAT_TRANSLATION_BASE_URL` 指向网关；Docker 内通过既有宿主机桥接地址访问。
- `CAMCAT_LLM_HEALTH_PATH=/v1/models` 使用带鉴权的模型列表检查。
- `CAMCAT_LLM_MAX_OUTPUT_TOKENS=4096` 明确结构化计划的输出预算。
- `CAMCAT_GATEWAY_RECEIPTS_DIR=/var/lib/camcat/gateway-receipts` 保存应用侧用量回执。

应用回执保存客户端请求编号、网关请求编号、模型、HTTP 状态和 Token 用量，不保存提示词、媒体地址或密钥。写入失败不会将已成功的生成变成模型重试。

连接：

```bash
cd /home/zjr/Tracker/internship_project_lab/llm_gateway
env PYTHONDONTWRITEBYTECODE=1 /data/zjr/camcat_studio/venv/bin/python -B \
  scripts/video_studio_integration.py connect \
  --output /data/zjr/llm_gateway/checks/video-studio-20261116-v1
```

工作台的 `configure-local` 会根据已保存连接恢复网关路由、Key、健康检查与输出预算；修改上游模型前需要同步更新网关渠道。连接脚本复用已保存的账户与渠道，不重复创建。

## 实际业务验收

复用视频工作台现有两个 Playwright 旅程，使用真实本地模型与媒体服务：

1. 原片上传、分析、剪辑规划、导出、多轮修改、回滚及版本冲突。
2. 中文原片上传与 ASR、文本触发翻译和配音、试听、审校、渲染及交付包下载。

原生浏览器报告保存在本轮目录的 `browser-report.json`，录屏、截图和交付文件位于 `browser-results/`。运行后执行：

```bash
env PYTHONDONTWRITEBYTECODE=1 /data/zjr/camcat_studio/venv/bin/python -B \
  scripts/video_studio_integration.py verify \
  --output /data/zjr/llm_gateway/checks/video-studio-20261116-v1
```

核对器要求应用回执与网关请求集合相等、逐笔用量相等、账户扣额相等且预占归零，同时要求两个浏览器旅程均通过且未跳过。成功结果写入 `report.json`；失败直接退出，保留原始记录。

## 2026-11-16 本轮结果

本轮两个浏览器旅程均通过，无跳过或重试，总用时 544.70 秒；英文译制旅程在 164.52 秒完成下载。以下用量包含一次真实结构化协议检查及两个业务旅程的生成请求。

| 检查 | 结果 |
| --- | --- |
| 生成请求 | 11 次 |
| 应用回执 / 网关请求 / 账户扣额 | 28,214 / 28,214 / 28,214 Token |
| 请求 ID 与逐笔用量 | 全部对应一致 |
| 残留预占 | 0 |
| 浏览器旅程 | 剪辑、多轮修改、回滚与版本冲突；识别、翻译、配音、播放及交付下载，2/2 通过 |
| 工程检查 | 后端 550 项、前端 37 项通过；Ruff 与 mypy 通过 |
| 运行代码 | API、分析、译制及渲染 Worker 的配置与 Provider 实现均与本次宿主机源码一致 |

完整收据保存在 `/data/zjr/llm_gateway/checks/video-studio-20261116-v1/report.json`，实际英文成片为同目录 `english-demo.mp4`。账户持续供视频工作台使用；模型和其他业务应用的配置保持各自独立。
