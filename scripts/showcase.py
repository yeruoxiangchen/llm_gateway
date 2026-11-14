"""Demonstrate a complete gateway workflow with an isolated temporary account."""
import argparse
import json
import time
import traceback

import runtime
from benchmark import atomic_json
from client import API, eventually
from demo import model_options, streamed_completion


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--presentation", action="store_true", help="Readable pages and pauses for recording")
    args = parser.parse_args()
    api = API()
    folder = runtime.ROOT/"checks/showcase"/(time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())+"_"+api.prefix)
    folder.mkdir(parents=True); folder.chmod(0o700)
    report = {"state": "running", "started": time.time(), "steps": [], "namespace": api.prefix}
    def save():
        report["elapsed_s"] = round(time.time()-report["started"], 3)
        atomic_json(folder/"report.json", report)
    def record(name, **fields):
        value = {"name": name, "state": "passed", **fields}
        report["steps"].append(value); save()
        print(json.dumps(value, ensure_ascii=False, indent=2 if args.presentation else None), flush=True)
        if args.presentation:
            time.sleep(9)
    def page(title):
        if args.presentation:
            print("\033[2J\033[H$ bash run.sh showcase --presentation\n", end="", flush=True)
        print(title, flush=True)
    def receipt(identity, key, usage):
        value = eventually(lambda: (r if (r := api.receipt(identity, key))["state"] == "settled" else None), timeout=20)
        assert all(value["usage"][k] == usage[k] for k in ("prompt_tokens", "completion_tokens", "total_tokens"))
        return value
    save()
    try:
        if args.presentation:
            page("LLM Gateway / Go 大模型网关与用量管理")
            print("\n应用  →  网关  →  本机 Qwen3-VL-8B\n"
                  "           ├─ MySQL / MariaDB：账户、额度与请求账本\n"
                  "           └─ Redis：API Key 请求频率限制\n\n"
                  "本次运行真实调用 API，使用独立临时账户。\n"
                  "五步演示：普通响应 / SSE / 幂等 / 额度 / Key 撤销\n", flush=True)
            time.sleep(10)
        account, key, credential = api.account("showcase", quota=100000, rpm=60)
        provider = api.provider("showcase", **model_options())
        report.update(account_id=account["id"], provider_id=provider["id"])
        body = api.body(provider["model_alias"], messages=[{"role": "user", "content": "小明有12个苹果，又买8个，送出5个，还剩几个？只回答数字。"}], max_tokens=32, temperature=0)
        identity = "showcase-"+api.prefix
        page("1/5 普通响应：调用本机模型并记录用量")
        if args.presentation:
            print("POST /v1/chat/completions   stream=false\n问题：12 个苹果 + 买 8 个 - 送出 5 个，还剩几个？\n模型回答：", flush=True)
        normal, headers = api.json("POST", "/v1/chat/completions", body, key=key, headers={"Idempotency-Key": identity}, timeout=150)
        print(normal["choices"][0]["message"]["content"], flush=True)
        receipt(headers["x-request-id"], key, normal["usage"])
        record("json", request_id=headers["x-request-id"], usage=normal["usage"])

        page("2/5 流式响应：边生成边显示")
        if args.presentation:
            print("POST /v1/chat/completions   stream=true\n模型生成的文字通过 SSE 持续返回：\n", flush=True)
        stream_body = api.body(provider["model_alias"], stream=True, messages=[{"role": "user", "content": "这个示例网关支持独立API Key、每Key请求频率限制和每账户Token总额度。请仅根据这些能力，用两句简短中文介绍它的用途。"}], max_tokens=128, temperature=0)
        stream = streamed_completion(api, key, stream_body, {"Idempotency-Key": identity+"-stream"})
        receipt(stream["request_id"], key, stream["usage"])
        record("sse", request_id=stream["request_id"], usage=stream["usage"])

        page("3/5 幂等重放：重复请求复用结果，用量不再增加")
        before = api.usage(key)["account"]
        replay, replay_headers = api.json("POST", "/v1/chat/completions", body, key=key, headers={"Idempotency-Key": identity}, timeout=30)
        after = api.usage(key)["account"]
        assert replay == normal and replay_headers["x-request-id"] == headers["x-request-id"]
        assert replay_headers.get("x-idempotency-replayed") == "true" and after["used_tokens"] == before["used_tokens"]
        if args.presentation:
            print(f"相同请求体 + 相同 Idempotency-Key\n"
                  f"X-Idempotency-Replayed: {replay_headers['x-idempotency-replayed']}\n"
                  f"第一次 request_id: {headers['x-request-id']}\n"
                  f"重放后 request_id: {replay_headers['x-request-id']}\n"
                  f"重放前已用 Token: {before['used_tokens']}\n"
                  f"重放后已用 Token: {after['used_tokens']}\n", flush=True)
        record("idempotent_replay", same_request_id=True, added_used_tokens=0)

        page("4/5 额度控制：将本演示账户的可用额度调到0，再请求")
        api.json("PUT", "/admin/accounts/"+account["id"]+"/quota", {"quota_tokens": after["used_tokens"]}, admin=True)
        status, _, raw = api.chat(key, provider["model_alias"], max_tokens=16)
        error = json.loads(raw)["error"]
        assert status == 402 and error["code"] == "quota_exceeded"
        if args.presentation:
            print(f"账户总额度 = 已用量 = {after['used_tokens']} Token\n"
                  "再次 POST /v1/chat/completions\n"
                  f"HTTP {status} / {error['code']}\n", flush=True)
        record("quota_rejection", http_status=status, code=error["code"])
        final_usage = api.usage(key)
        expected = normal["usage"]["total_tokens"]+stream["usage"]["total_tokens"]
        assert final_usage["account"]["used_tokens"] == expected
        assert final_usage["account"]["reserved_tokens"] == 0
        assert all(r["state"] == "settled" for r in final_usage["requests"])
        assert sum(r.get("usage", {}).get("total_tokens", 0) for r in final_usage["requests"]) == expected

        page("5/5 Key管理：吊销本演示Key，后续访问被拒绝")
        api.json("DELETE", "/admin/keys/"+credential["id"], admin=True)
        status, _, _ = api.call("GET", "/v1/models", key=key)
        assert status == 401
        if args.presentation:
            print(f"DELETE /admin/keys/{{id}}：撤销演示 Key\n"
                  f"使用同一 Key 访问 GET /v1/models → HTTP {status}\n", flush=True)
        record("key_revocation", http_status=status)
        report.update(state="passed", used_tokens=expected, account_and_receipts_match=True)
    except Exception as exc:
        report.update(state="failed", error=type(exc).__name__+": "+str(exc)); traceback.print_exc()
    finally:
        try:api.cleanup(); report["cleanup_completed"] = True
        except Exception as exc:report.update(state="failed", cleanup_error=str(exc))
        save()
    if args.presentation:
        page("运行结果 / 本次真实 API 演示")
        print(f"\n通过步骤：{len(report['steps'])}/5\n"
              f"运行状态：{report['state']}\n"
              f"账户用量与请求回执一致：{report.get('account_and_receipts_match', False)}\n"
              f"临时 Key 与渠道已清理：{report.get('cleanup_completed', False)}\n", flush=True)
    print(json.dumps({"state": report["state"], "passed": len(report["steps"]), "used_tokens": report.get("used_tokens"),
                      "report": str(folder/"report.json")}, ensure_ascii=False), flush=True)
    if args.presentation:
        time.sleep(12)
    return 0 if report["state"] == "passed" else 1


if __name__ == "__main__":raise SystemExit(main())
