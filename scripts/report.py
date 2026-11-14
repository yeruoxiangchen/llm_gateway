"""Export small, non-secret verification notes into the source repository."""
import datetime
import hashlib
import json

import runtime


def read(name):
    return json.loads((runtime.ROOT / "checks" / name).read_text())


def main():
    go = read("go-test.json")
    verification = read("verification.json")
    load = read("stability.json")
    if load["state"] == "running" or verification["state"] == "running":
        raise RuntimeError("Wait for the active test to finish before exporting its report")
    digest = hashlib.sha256()
    for path in sorted(list(runtime.SOURCE.glob("cmd/**/*.go")) + list(runtime.SOURCE.glob("internal/**/*.go")) + list(runtime.SOURCE.glob("internal/**/*.sql")) + [runtime.SOURCE / "go.mod", runtime.SOURCE / "go.sum"]):
        digest.update(str(path.relative_to(runtime.SOURCE)).encode())
        digest.update(path.read_bytes())
    when = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    top = [name for name in go["passed"] if "/" not in name]
    lines = ["# 验收记录", "", f"导出时间：{when}。Go 源码、SQL 与依赖锁文件摘要：`{digest.hexdigest()}`。", "",
             "## 功能与故障测试", "",
             f"- Go race / 真实 MariaDB 集成：**{go['state']}**，{len(top)} 组顶层测试；跳过 {len(go['skipped'])}，失败 {len(go['failed'])}。",
             f"- HTTP / 故障验收：**{verification['state']}**，{verification.get('passed',0)} / {verification.get('total',21)} 组。",
             "- 实测覆盖双实例幂等与额度竞争、Redis 中断、数据库中断与结算凭据重复回放、SIGKILL 与租约恢复、流式超时和取消。",
             "- 真实本机 Qwen3-VL-8B 的普通 / SSE 响应均取得有效 usage，并与账户用量核对。", "",
             "| HTTP 组 | 结果 | 秒 |", "| --- | --- | ---: |"]
    for case in verification["cases"]:
        lines.append(f"| `{case['name']}` | {case['state']} | {case['elapsed_s']} |")
    lines += ["", "## 最新负载", "",
              "上游为确定性的 CPU 协议服务，结果衡量网关和存储链路。", "",
              f"- 结果：**{load['state']}**；{load['duration_s']} 秒，{load['target_rps']} RPS，{load.get('accounts',1)} 账户，最大 {load['concurrency']} 并发。",
              f"- 已完成 / 成功 / 因客户端并发槽位不足未发出：{load.get('completed',0)} / {load.get('successful',0)} / {load.get('skipped',0)}。",
              f"- 实际吞吐：{load.get('achieved_rps','—')} RPS；P50 / P95 / P99：{load.get('p50_ms','—')} / {load.get('p95_ms','—')} / {load.get('p99_ms','—')} ms。",
              f"- 负载阶段实际结算 / 期望 Token：{load.get('settled_tokens','—')} / {load.get('expected_tokens','—')}。预热用量另计。",
              f"- RSS 增量：{load.get('rss_growth_mib','—')} MiB；goroutine / FD 增量：{load.get('goroutine_growth','—')} / {load.get('fd_growth','—')}。", "",
              "| 检查 | 通过 |", "| --- | --- |"]
    for name, passed in load.get("checks", {}).items():
        lines.append(f"| `{name}` | {passed} |")
    lines += ["", "## 调试与负载历史", "",
              "初版把实例心跳写到请求表的二级索引范围，InnoDB 保留了它与结算事务的死锁记录。修复后实例心跳写独立 `owner_leases` 表，恢复时重新锁定并检查实例租约；回归测试覆盖心跳与结算并发、心跳不改写请求、活实例不被恢复以及过期实例按状态恢复。",
              "", "随后把成功调用审计和结算合并到同一事务，正常调用路径减少一次同步提交。各轮结果保留，下面包括不达标数据；不同账户数和并发数应分别看待。", "",
              "| 时间 UTC | 账户 | 并发 | 目标 RPS | 完成 | 实际 RPS | P95 ms | 结果 |", "| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |"]
    for path in sorted((runtime.ROOT / "checks").glob("stability-*.json")):
        previous = json.loads(path.read_text())
        stamp = datetime.datetime.fromtimestamp(previous["started"], datetime.timezone.utc).strftime("%H:%M:%S")
        lines.append(f"| {stamp} | {previous.get('accounts',1)} | {previous['concurrency']} | {previous['target_rps']} | {previous.get('completed',0)} | {previous.get('achieved_rps','—')} | {previous.get('p95_ms','—')} | {previous['state']} |")
    lines += ["", "原始报告、逐项检查、资源采样和日志保存在 `/data/zjr/llm_gateway/checks/` 与 `/data/zjr/llm_gateway/logs/`。死锁修复前证据保存于 `checks/deadlock-heartbeat-before.txt`。", "",
              "这些是共享服务器上的短时开发验收结果，模型推理吞吐及长时运行需要使用对应场景单独测量。长时负载可以沿用同一工具，例如 `bash run.sh stability --duration 3600 --rps 10 --accounts 4 --concurrency 16`；用 `bash run.sh status` 和 `tail -f /data/zjr/llm_gateway/logs/stability.log` 查看结果。", ""]
    target = runtime.SOURCE / "docs/validation.md"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("\n".join(lines))
    print(target)


if __name__ == "__main__":
    main()
