"""Demonstrate a complete gateway workflow with an isolated temporary account."""
import json
import time
import traceback

import runtime
from benchmark import atomic_json
from client import API, eventually
from demo import model_options, streamed_completion


def main():
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
        print(json.dumps(value, ensure_ascii=False), flush=True)
    def receipt(identity, key, usage):
        value = eventually(lambda: (r if (r := api.receipt(identity, key))["state"] == "settled" else None), timeout=20)
        assert all(value["usage"][k] == usage[k] for k in ("prompt_tokens", "completion_tokens", "total_tokens"))
        return value
    save()
    try:
        account, key, credential = api.account("showcase", quota=100000, rpm=60)
        provider = api.provider("showcase", **model_options())
        report.update(account_id=account["id"], provider_id=provider["id"])
        body = api.body(provider["model_alias"], messages=[{"role": "user", "content": "小明有12个苹果，又买8个，送出5个，还剩几个？只回答数字。"}], max_tokens=32, temperature=0)
        identity = "showcase-"+api.prefix
        print("1/5 普通响应：调用本机模型并记录用量", flush=True)
        normal, headers = api.json("POST", "/v1/chat/completions", body, key=key, headers={"Idempotency-Key": identity}, timeout=150)
        print(normal["choices"][0]["message"]["content"], flush=True)
        receipt(headers["x-request-id"], key, normal["usage"])
        record("json", request_id=headers["x-request-id"], usage=normal["usage"])

        print("2/5 流式响应：边生成边显示", flush=True)
        stream_body = api.body(provider["model_alias"], stream=True, messages=[{"role": "user", "content": "这个示例网关支持独立API Key、每Key请求频率限制和每账户Token总额度。请仅根据这些能力，用两句简短中文介绍它的用途。"}], max_tokens=128, temperature=0)
        stream = streamed_completion(api, key, stream_body, {"Idempotency-Key": identity+"-stream"})
        receipt(stream["request_id"], key, stream["usage"])
        record("sse", request_id=stream["request_id"], usage=stream["usage"])

        print("3/5 幂等重放：重复请求复用结果，用量不再增加", flush=True)
        before = api.usage(key)["account"]
        replay, replay_headers = api.json("POST", "/v1/chat/completions", body, key=key, headers={"Idempotency-Key": identity}, timeout=30)
        after = api.usage(key)["account"]
        assert replay == normal and replay_headers["x-request-id"] == headers["x-request-id"]
        assert replay_headers.get("x-idempotency-replayed") == "true" and after["used_tokens"] == before["used_tokens"]
        record("idempotent_replay", same_request_id=True, added_used_tokens=0)

        print("4/5 额度控制：将本演示账户的可用额度调到0，再请求", flush=True)
        api.json("PUT", "/admin/accounts/"+account["id"]+"/quota", {"quota_tokens": after["used_tokens"]}, admin=True)
        status, _, raw = api.chat(key, provider["model_alias"], max_tokens=16)
        error = json.loads(raw)["error"]
        assert status == 402 and error["code"] == "quota_exceeded"
        record("quota_rejection", http_status=status, code=error["code"])
        final_usage = api.usage(key)
        expected = normal["usage"]["total_tokens"]+stream["usage"]["total_tokens"]
        assert final_usage["account"]["used_tokens"] == expected
        assert final_usage["account"]["reserved_tokens"] == 0
        assert all(r["state"] == "settled" for r in final_usage["requests"])
        assert sum(r.get("usage", {}).get("total_tokens", 0) for r in final_usage["requests"]) == expected

        print("5/5 Key管理：吊销本演示Key，后续访问被拒绝", flush=True)
        api.json("DELETE", "/admin/keys/"+credential["id"], admin=True)
        status, _, _ = api.call("GET", "/v1/models", key=key)
        assert status == 401
        record("key_revocation", http_status=status)
        report.update(state="passed", used_tokens=expected, account_and_receipts_match=True)
    except Exception as exc:
        report.update(state="failed", error=type(exc).__name__+": "+str(exc)); traceback.print_exc()
    finally:
        try:api.cleanup(); report["cleanup_completed"] = True
        except Exception as exc:report.update(state="failed", cleanup_error=str(exc))
        save()
    print(json.dumps({"state": report["state"], "passed": len(report["steps"]), "used_tokens": report.get("used_tokens"),
                      "report": str(folder/"report.json")}, ensure_ascii=False), flush=True)
    return 0 if report["state"] == "passed" else 1


if __name__ == "__main__":raise SystemExit(main())
