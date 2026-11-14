# 演示录制

视频展示本机现有 Go 网关、MariaDB、Redis 和 Qwen3-VL-8B 的真实 API 交互。
依次演示普通 JSON 响应、SSE 流式响应、相同请求的幂等重放、账户额度拒绝和 Key 吊销。
演示脚本创建独立临时账户与渠道，完成后清理 Key 和渠道，保留请求账本。

2026-11-14 这一轮五步通过，实际用量 117 Token，重放新增用量为 0，预占余额为 0。
客户端检查了两次生成的用量回执、重放内容和请求 ID、HTTP 402、HTTP 401，以及账户用量与逐请求回执合计。

## 生成方式

asciinema 记录实际进程的终端输出和单调时钟时间；pyte 负责终端转义序列、换行和宽字符；Pillow 绘制字符与中文说明，FFmpeg 编码为 H.264 MP4。
录制时由 `showcase --presentation` 在每步结果后停留，便于阅读；保留真实流式输出的时间间隔。
渲染器在最后一个终端事件后再停留 12 秒显示结果页。视频无旁白。

大文件均存于 `/data/zjr/llm_gateway/media/`，依赖与字体放在本项目的数据目录或使用已有字体的明确路径。
GitHub 中 `docs/media/` 是发布副本；本机源码工作区稀疏省略这个目录，物理文件保留在数据盘。

准备 Python 3.10+、FFmpeg（含 libx264）和可显示中文的字体。以下命令适用于当前服务器：

```bash
cd /home/zjr/Tracker/internship_project_lab/llm_gateway
source scripts/env.sh
python -B -m pip install --cache-dir "$LLMGW_DATA_ROOT/cache/pip" \
  --target "$LLMGW_DATA_ROOT/toolchain/recording" \
  -r scripts/requirements-recording.txt
export PYTHONPATH="$LLMGW_DATA_ROOT/toolchain/recording${PYTHONPATH:+:$PYTHONPATH}"
export XDG_CONFIG_HOME="$LLMGW_DATA_ROOT/config"
export ASCIINEMA_CONFIG_HOME="$LLMGW_DATA_ROOT/config/asciinema"
export TERM=xterm-256color
llmgw_recording="$LLMGW_DATA_ROOT/media/$(date -u +%Y%m%dT%H%M%SZ)"
mkdir -p "$llmgw_recording" "$ASCIINEMA_CONFIG_HOME"
python -B -m asciinema rec --cols 110 --rows 21 --env TERM \
  --command 'bash run.sh showcase --presentation' \
  "$llmgw_recording/llm-gateway-demo.cast"

# 当前机器复用已经安装的 FFmpeg 和 Noto CJK 字体；其他机器替换这两个路径。
python -B scripts/render_recording.py \
  "$llmgw_recording/llm-gateway-demo.cast" \
  "$llmgw_recording/llm-gateway-demo.mp4" \
  --ffmpeg /data/zjr/camcat_studio/tools/ffmpeg/ffmpeg \
  --font-cjk /data/zjr/camcat_studio/tools/fonts/NotoSansCJKsc-Regular.otf
```

输出包括 MP4、JPG 封面、原始 `.cast`、章节及编码信息 `.json` 和本地 FFmpeg 日志。
录制内容包含请求 ID 和演示结果；脚本将管理 Token 和 API Key 保存在客户端内存中。
上传前核对录制输出与当前配置中的密钥，发布演示相关文件。
