"""Provision a persistent local-model demo and print results without API keys."""
import argparse
import json
import os
import time
from pathlib import Path

from client import API, eventually
import runtime


def model_options():
    values = runtime.read_env()
    return {"upstream_model": os.environ.get("LLMGW_MODEL_NAME", values.get("LLMGW_MODEL_NAME", "office-qwen3-vl-8b")),
            "base_url": os.environ.get("LLMGW_MODEL_BASE_URL", values.get("LLMGW_MODEL_BASE_URL", "http://127.0.0.1:28282/v1")),
            "api_key": os.environ.get("LLMGW_MODEL_API_KEY", values.get("LLMGW_MODEL_API_KEY", "local-reuse")),
            "input_micro_usd_per_million_tokens": 0, "output_micro_usd_per_million_tokens": 0}


def provision(api):
    path = runtime.ROOT / "config/demo-client.json"
    if path.exists():
        config = json.loads(path.read_text())
        if api.call("GET", "/v1/models", key=config["api_key"])[0] == 200:
            return config
        raise RuntimeError("Saved demo key has been revoked; provision a new account through the admin API")
    account, secret, credential = api.account("demo-user", quota=100000, rpm=60)
    provider = api.provider("demo-local-qwen", alias="local-qwen", **model_options())
    config = {"base_url": "http://127.0.0.1:29480/v1", "api_key": secret, "account_id": account["id"],
              "key_id": credential["id"], "model": "local-qwen", "provider_id": provider["id"]}
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "w") as target:
        json.dump(config, target, ensure_ascii=False, indent=2)
    print("演示配置已保存：", path, "（API Key 保存在此文件中）", flush=True)
    return config


def streamed_completion(api, key, body, headers=None):
    import urllib.request
    from client import HTTP
    request = urllib.request.Request(api.base + "/v1/chat/completions", data=json.dumps(body, ensure_ascii=False).encode(),
                                     headers={"Content-Type": "application/json", "Authorization": "Bearer "+key, **(headers or {})})
    chunks = []; usage = None; done = False
    with HTTP.open(request, timeout=150) as response:
        identity = response.headers["X-Request-ID"]
        replayed = response.headers.get("X-Idempotency-Replayed") == "true"
        for line in response:
            if not line.startswith(b"data:"):continue
            data = line[5:].strip()
            if data == b"[DONE]":done = True; continue
            chunk = json.loads(data)
            if "error" in chunk:raise RuntimeError(chunk["error"])
            usage = chunk.get("usage") or usage
            for choice in chunk.get("choices", []):
                text = choice.get("delta", {}).get("content") or ""
                chunks.append(text); print(text, end="", flush=True)
    if not done:raise RuntimeError("Stream ended without its completion marker")
    print()
    return {"request_id": identity, "text": "".join(chunks), "usage": usage, "replayed": replayed}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--prompt", default="用两句话说明大模型网关如何管理用量。")
    parser.add_argument("--stream", action="store_true")
    parser.add_argument("--usage", action="store_true")
    parser.add_argument("--idempotency-key", help="Repeat the same key and request to demonstrate cached replay")
    parser.add_argument("--max-tokens", type=int, default=256)
    args = parser.parse_args()
    api = API()
    config = provision(api)
    key = config["api_key"]
    if args.usage:
        usage = api.usage(key)
        print(json.dumps(usage, ensure_ascii=False, indent=2))
        return
    headers = {"Idempotency-Key": args.idempotency_key or "demo-"+str(time.time_ns())}
    body = api.body(config["model"], stream=args.stream, messages=[{"role": "user", "content": args.prompt}], max_tokens=args.max_tokens)
    if args.stream:
        result = streamed_completion(api, key, body, headers)
        identity = result["request_id"]; replayed = result["replayed"]
    else:
        result, response_headers = api.json("POST", "/v1/chat/completions", body, key=key, headers=headers, timeout=120)
        identity = response_headers["x-request-id"]
        replayed = response_headers.get("x-idempotency-replayed") == "true"
        print(result["choices"][0]["message"]["content"])
    receipt = eventually(lambda: (r if (r := api.receipt(identity, key))["state"] in ("settled", "released", "pending_usage") else None))
    print(json.dumps({"request_id": identity, "state": receipt["state"], "usage": receipt.get("usage"), "cost_micro_usd": receipt["cost_micro_usd"], "replayed": replayed}, ensure_ascii=False))


if __name__ == "__main__":
    main()
