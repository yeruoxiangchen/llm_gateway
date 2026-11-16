# 版本与复现

| 入口 | 用途 |
| --- | --- |
| [`resume-20261116`](https://github.com/yeruoxiangchen/llm_gateway/tree/resume-20261116) | 固定网关核心、模型协议/并发/用量测试，以及企业 Agent、视频工作台两项应用接入。 |
| [`main`](https://github.com/yeruoxiangchen/llm_gateway/tree/main) | 最新发布版本；额外包含账户、Key、渠道与请求查询管理 API 的补充。 |

```bash
git clone https://github.com/yeruoxiangchen/llm_gateway.git
cd llm_gateway
git checkout resume-20261116
```

最新版本使用 `git checkout main`。简历标签在既有公开核心代码上固定已验证的应用接入，
管理 API 补充单独保存为后续提交。两项应用分别使用独立账户，验收用量互不混合。

[视频工作台：28,214 Token](video-studio.md) · [企业 Agent：28,268 Token](enterprise-agent.md) ·
[既有压测和协议数据](public-qa.md) · [运行与配置](../README.md)。
演示视频保留在 `docs/media/`。本机密钥、数据库、模型及运行原始日志均留在数据目录。
