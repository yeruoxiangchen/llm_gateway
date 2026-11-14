"""Black-box contract tests plus controlled outages of this project's services."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
from pathlib import Path
import threading
import time
import traceback

from client import API, BASE, PEER, FIXTURE, HTTP, eventually, first_event, ledger_snapshot, release, stats
import runtime

api = API()
REPORT = runtime.ROOT / "checks/verification.json"
report = {"state": "running", "started": time.time(), "namespace": api.prefix,
          "scope": "real HTTP + MariaDB + Redis, deterministic fault upstream and existing local Qwen", "cases": []}


def save():
    REPORT.write_text(json.dumps(report, ensure_ascii=False, indent=2))


def checked(condition, message):
    if not condition:
        raise AssertionError(message)


def done_receipt(identity, key, state="settled", timeout=15):
    return eventually(lambda: (r if (r := api.receipt(identity, key))["state"] == state else None), timeout, f"receipt {identity} did not reach {state}")


def basic_security():
    checked(api.call("GET", "/health/ready")[0] == 200, "readiness")
    checked(api.call("GET", "/v1/models")[0] == 401, "missing API key accepted")
    checked(api.call("GET", "/admin/providers")[0] == 401, "missing admin credential accepted")
    account, key, meta = api.account("auth")
    provider = api.provider("auth")
    code, headers, _ = api.chat(key, provider["model_alias"])
    checked(code == 200, "authenticated request failed")
    _, other, _ = api.account("other")
    checked(api.call("GET", "/v1/requests/" + headers["x-request-id"], key=other)[0] == 404, "cross-account receipt leaked")
    providers, _ = api.json("GET", "/admin/providers", admin=True)
    checked("fixture-secret" not in json.dumps(providers), "provider credential leaked")
    api.json("DELETE", "/admin/keys/" + meta["id"], admin=True)
    checked(api.call("GET", "/v1/models", key=key)[0] == 401, "revoked key accepted")
    result, _ = api.json("PUT", f"/admin/accounts/{account['id']}/quota", {"quota_tokens": account["quota_tokens"]}, admin=True)
    checked(result["quota_tokens"] == account["quota_tokens"], "setting the same quota failed")
    return {"cross_account_status": 404, "revoked_key_status": 401}


def validation():
    _, key, _ = api.account("validation")
    p = api.provider("validation")
    for change in [{"max_tokens": 4097}, {"max_completion_tokens": -1}, {"n": 2}, {"unsupported_option": True},
                   {"messages": [{"role": "user", "content": [{"type": "image_url", "image_url": {"url": "https://invalid/image.png"}}]}]},
                   {"messages": [{"role": "user", "content": "x" * 70000}]}]:
        checked(api.chat(key, p["model_alias"], **change)[0] == 400, "invalid request was admitted: " + str(list(change)))
    checked(api.chat(key, api.prefix + "-missing")[0] == 404, "unknown model accepted")
    checked(api.chat(key, p["model_alias"], idem="x"*129)[0] == 400, "oversized idempotency key accepted")
    checked(api.usage(key)["account"]["reserved_tokens"] == 0, "rejected input reserved quota")
    return {"invalid_inputs": 8}


def normal_usage_and_tools():
    _, key, _ = api.account("usage")
    p = api.provider("usage")
    code, headers, raw = api.chat(key, p["model_alias"])
    checked(code == 200, raw[:200])
    body = json.loads(raw)
    checked(body["model"] == p["model_alias"] and body["usage"]["total_tokens"] == 12, "OpenAI response mapping")
    receipt = done_receipt(headers["x-request-id"], key)
    checked(receipt["cost_micro_usd"] == 19, "provider price calculation")
    tool = {"type": "function", "function": {"name": "echo", "parameters": {"type": "object", "properties": {"value": {"type": "string"}}}}}
    code, _, raw = api.chat(key, p["model_alias"], tools=[tool], tool_choice="auto")
    checked(code == 200 and json.loads(raw)["choices"][0]["message"]["tool_calls"][0]["function"]["name"] == "echo", "tool call passthrough")
    account = api.usage(key)["account"]
    checked(account["used_tokens"] == 24 and account["reserved_tokens"] == 0, "usage settlement")
    attempt_count = runtime.sql(f"USE llm_gateway; SELECT COUNT(*) FROM attempts a JOIN requests r ON r.id=a.request_id WHERE r.account_id='{account['id']}' AND a.status=200 AND a.ended IS NOT NULL;")
    checked(attempt_count == "2", "attempt audit and usage did not commit together")
    for group in ["model", "account", "provider", "day"]:
        report, _ = api.json("GET", "/admin/usage?account_id=" + account["id"] + "&group_by=" + group, admin=True)
        checked(len(report["data"]) == 1 and report["data"][0]["total_tokens"] == 24 and report["data"][0]["cost_micro_usd"] == 38 and report["data"][0]["requests"] == 2, "usage aggregation mismatch: " + group)
    checked(api.call("GET", "/admin/usage?group_by=arbitrary-sql", admin=True)[0] == 400, "invalid grouping accepted")
    return {"used_tokens": 24, "first_request_cost_micro_usd": 19, "tool_call": "echo"}


def streaming():
    _, key, _ = api.account("stream")
    p = api.provider("stream")
    started = time.monotonic()
    with api.stream(key, p["model_alias"], idem="stream-replay") as response:
        identity = response.headers["X-Request-ID"]
        first = first_event(response)
        first_at = time.monotonic()
        raw = first + response.read()
    finished = time.monotonic()
    checked(finished-first_at >= 0.02, "SSE was buffered until the final event")
    checked(b"[DONE]" in raw and b'"total_tokens":12' in raw, "stream completion or usage missing")
    receipt = done_receipt(identity, key)
    checked(receipt["usage"]["total_tokens"] == 12, "stream usage not settled")
    code, headers, replay = api.chat(key, p["model_alias"], stream=True, idem="stream-replay")
    checked(code == 200 and headers.get("x-idempotency-replayed") == "true" and replay == raw, "cached SSE was not replayed exactly")
    return {"first_event_ms": round((first_at-started)*1000, 2), "completion_ms": round((finished-started)*1000, 2), "used_tokens": 12}


def fallback_and_circuit():
    _, key, _ = api.account("fallback")
    bad = api.provider("fallback-bad", "fail503", alias=api.prefix + "-fallback", priority=10)
    api.provider("fallback-good", alias=bad["model_alias"])
    before = stats()["calls"].get("fail503", 0)
    for _ in range(5):
        checked(api.chat(key, bad["model_alias"])[0] == 200, "fallback failed")
    failures = stats()["calls"].get("fail503", 0)-before
    checked(failures == 3, f"circuit failed to open after 3 consecutive failures: {failures}")
    checked(api.usage(key)["account"]["used_tokens"] == 60, "fallback double charged")
    return {"requests": 5, "failed_provider_attempts": failures, "successful_provider_attempts": 5}


def round_robin_and_disable():
    a, key, _ = api.account("rr")
    one = api.provider("rr-one", alias=api.prefix + "-rr")
    two = api.provider("rr-two", alias=one["model_alias"])
    for _ in range(4):
        checked(api.chat(key, one["model_alias"])[0] == 200, "RR request failed")
    receipts = api.usage(key)["requests"]
    checked(sum(r["provider_id"] == one["id"] for r in receipts) == 2 and sum(r["provider_id"] == two["id"] for r in receipts) == 2, "equal-priority round robin skew")
    api.json("PATCH", "/admin/providers/" + one["id"], {"enabled": False}, admin=True)
    code, headers, _ = api.chat(key, one["model_alias"])
    checked(code == 200 and done_receipt(headers["x-request-id"], key)["provider_id"] == two["id"], "disabled provider still selected")
    return {"initial_distribution": [2, 2], "disabled_provider_excluded": True}


def rejection_and_oversize():
    _, key, _ = api.account("rejections")
    bad = api.provider("bad400", "fail400", alias=api.prefix + "-bad400", priority=10)
    api.provider("unused-fallback", alias=bad["model_alias"])
    before = stats()["calls"].get("normal", 0)
    code, headers, _ = api.chat(key, bad["model_alias"])
    checked(code == 400 and stats()["calls"].get("normal", 0) == before, "400 was retried")
    done_receipt(headers["x-request-id"], key, "released")
    huge = api.provider("huge", "huge")
    code, headers, _ = api.chat(key, huge["model_alias"])
    checked(code == 502, f"oversized upstream response accepted: {code}")
    done_receipt(headers["x-request-id"], key, "released")
    checked(api.usage(key)["account"]["used_tokens"] == 0, "failed pre-response requests charged")
    return {"non_retryable_status": 400, "oversized_response_status": 502, "used_tokens": 0}


def concurrent_idempotency():
    _, key, _ = api.account("idempotency")
    p = api.provider("idempotency", "slow")
    before = stats()["calls"].get("slow", 0)
    barrier = threading.Barrier(8)
    def request(n):
        barrier.wait()
        return api.chat(key, p["model_alias"], idem="same-logical-request", base=BASE if n < 4 else PEER)
    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(request, range(8)))
    checked(all(code in (200, 409) for code, _, _ in responses), "unexpected duplicate response status")
    identities = {headers["x-request-id"] for _, headers, _ in responses}
    checked(len(identities) == 1 and stats()["calls"].get("slow", 0)-before == 1, "duplicate upstream execution")
    code, headers, _ = api.chat(key, p["model_alias"], idem="same-logical-request")
    checked(code == 200 and headers.get("x-idempotency-replayed") == "true", "completed response not replayed")
    changed, _, _ = api.chat(key, p["model_alias"], idem="same-logical-request", messages=[{"role": "user", "content": "different"}])
    checked(changed == 409, "same key accepted a different request")
    checked(api.usage(key)["account"]["used_tokens"] == 12, "duplicate request charged twice")
    # HTTP idempotency keys are byte-sensitive even with a case-insensitive DB default.
    for value in ["CaseSensitive", "casesensitive"]:
        checked(api.chat(key, p["model_alias"], idem=value)[0] == 200, "byte-distinct idempotency key conflicted")
    checked(stats()["calls"].get("slow", 0)-before == 3, "case-sensitive keys unexpectedly replayed")
    return {"concurrent_clients": 8, "instances": 2, "upstream_calls_for_duplicate_request": 1, "used_tokens_for_duplicate_request": 12, "byte_sensitive_keys": True}


def concurrent_quota():
    _, key, _ = api.account("quota", quota=2000)
    p = api.provider("quota", "hold")
    response = api.stream(key, p["model_alias"])
    try:
        first_event(response)
        account = api.usage(key)["account"]
        checked(1000 < account["reserved_tokens"] < 2000, "initial quota not reserved")
        with ThreadPoolExecutor(max_workers=8) as pool:
            codes = list(pool.map(lambda n: api.chat(key, p["model_alias"], base=BASE if n < 4 else PEER)[0], range(8)))
        checked(codes == [402]*8, f"quota over-admitted: {codes}")
        release()
        response.read()
    finally:
        response.close()
        release()
    eventually(lambda: api.usage(key)["account"]["reserved_tokens"] == 0)
    checked(api.usage(key)["account"]["used_tokens"] == 12, "reservation did not settle")
    return {"blocked_concurrent_requests": 8, "status": 402, "used_tokens": 12}


def rate_limit():
    if time.time() % 60 > 57:
        time.sleep(60 - time.time() % 60 + 0.05)
    _, key, _ = api.account("rpm", rpm=2)
    p = api.provider("rpm")
    with ThreadPoolExecutor(max_workers=10) as pool:
        responses = list(pool.map(lambda _: api.chat(key, p["model_alias"]), range(10)))
    codes = [r[0] for r in responses]
    checked(codes.count(200) == 2 and codes.count(429) == 8, f"atomic RPM limit: {codes}")
    checked(all(int(h["retry-after"]) > 0 for code, h, _ in responses if code == 429), "Retry-After missing")
    return {"successes": 2, "rate_limited": 8, "limit_rpm": 2}


def missing_usage_and_resolution():
    _, key, _ = api.account("pending")
    p = api.provider("pending", "nousage")
    code, headers, raw = api.chat(key, p["model_alias"])
    checked(code == 200, "missing usage should still return model output")
    checked("usage" not in json.loads(raw), "unknown usage was returned as fabricated zero tokens")
    identity = headers["x-request-id"]
    receipt = done_receipt(identity, key, "pending_usage")
    checked(receipt.get("usage") is None and api.usage(key)["account"]["reserved_tokens"] > 0, "unknown usage was fabricated or released")
    resolution = {"usage": {"prompt_tokens": 5, "completion_tokens": 7, "total_tokens": 12}, "reason": "fixture provider usage receipt reconciled"}
    api.json("POST", f"/admin/requests/{identity}/resolve", resolution, admin=True)
    checked(api.call("POST", f"/admin/requests/{identity}/resolve", resolution, admin=True)[0] == 409, "resolved receipt accepted another settlement")
    checked(api.usage(key)["account"]["used_tokens"] == 12 and api.usage(key)["account"]["reserved_tokens"] == 0, "manual usage settlement drift")
    reason = runtime.sql(f"USE llm_gateway; SELECT reason FROM ledger WHERE request_id='{identity}' AND event_type='settle';")
    checked(reason == resolution["reason"], "resolution reason was not audited")
    return {"unknown_state": "pending_usage", "resolved_usage": 12, "duplicate_resolution_status": 409}


def normalized_total():
    _, key, _ = api.account("normalized")
    p = api.provider("normalized", "omittotal")
    for stream in (False, True):
        code, headers, raw = api.chat(key, p["model_alias"], stream=stream)
        checked(code == 200, "valid usage components were rejected")
        if stream:
            checked(b'"total_tokens":12' in raw, "SSE usage disagrees with normalized total")
        else:
            checked(json.loads(raw)["usage"]["total_tokens"] == 12, "JSON usage disagrees with normalized total")
        checked(done_receipt(headers["x-request-id"], key)["usage"]["total_tokens"] == 12, "normalized ledger total")
    return {"json_and_sse_total": 12, "used_tokens": api.usage(key)["account"]["used_tokens"]}


def actual_usage_over_reservation():
    _, key, _ = api.account("overusage", quota=3000)
    p = api.provider("overusage", "overusage")
    code, headers, _ = api.chat(key, p["model_alias"])
    checked(code == 200, "overusage response failed")
    receipt = done_receipt(headers["x-request-id"], key)
    account = api.usage(key)["account"]
    checked(receipt["usage"]["total_tokens"] == 6001 and account["used_tokens"] == 6001 and account["available_tokens"] == -3001, "actual usage was clipped to its estimate")
    checked(api.chat(key, p["model_alias"])[0] == 402, "account debt did not block another request")
    return {"quota": 3000, "actual_usage": 6001, "available_tokens": -3001, "next_request_status": 402}


def interrupted_streams():
    _, key, _ = api.account("truncated")
    p = api.provider("truncated", "truncate", alias=api.prefix + "-truncate", priority=10)
    api.provider("truncate-fallback", alias=p["model_alias"])
    before = stats()["calls"].get("normal", 0)
    code, headers, raw = api.chat(key, p["model_alias"], stream=True)
    checked(code == 200 and b'"error"' in raw and b"[DONE]" not in raw, "truncated SSE reported success")
    checked(stats()["calls"].get("normal", 0) == before, "partial stream was retried")
    identity = headers["x-request-id"]
    done_receipt(identity, key, "pending_usage")
    api.refund(identity)
    return {"partial_response_retried": False, "unknown_usage_held": True, "refund_audited": True}


def deadlines_and_cancellation():
    _, key, _ = api.account("deadlines")
    delay = api.provider("deadline", "delay")
    started = time.monotonic()
    code, headers, _ = api.chat(key, delay["model_alias"], base=PEER)
    elapsed = time.monotonic()-started
    checked(code == 504 and 2.5 < elapsed < 7, f"total deadline failed: {code} {elapsed}")
    done_receipt(headers["x-request-id"], key, "released")
    stall = api.provider("stall", "stall")
    started = time.monotonic()
    code, headers, raw = api.chat(key, stall["model_alias"], stream=True, base=PEER)
    idle_elapsed = time.monotonic()-started
    checked(code == 200 and b'"error"' in raw and b"[DONE]" not in raw and 0.8 < idle_elapsed < 5, "stream idle deadline failed")
    done_receipt(headers["x-request-id"], key, "pending_usage")
    api.refund(headers["x-request-id"])
    long = api.provider("cancel", "long")
    before = stats()["cancelled"].get("long", 0)
    response = api.stream(key, long["model_alias"], base=PEER)
    first_event(response)
    identity = response.headers["X-Request-ID"]
    response.close()
    eventually(lambda: stats()["cancelled"].get("long", 0) > before, timeout=4, message="client disconnect did not cancel upstream")
    done_receipt(identity, key, "pending_usage")
    api.refund(identity)
    return {"overall_timeout_s": round(elapsed, 3), "stream_idle_timeout_s": round(idle_elapsed, 3), "cancellation_propagated": True}


def concurrency_bound():
    _, key, _ = api.account("inflight")
    p = api.provider("inflight", "hold")
    responses = []
    try:
        for _ in range(4):
            response = api.stream(key, p["model_alias"], base=PEER)
            responses.append(response)
            first_event(response)
        code, _, raw = api.chat(key, p["model_alias"], base=PEER)
        checked(code == 503 and json.loads(raw)["error"]["code"] == "gateway_busy", "inflight cap did not reject excess load")
        release()
        for response in responses:
            checked(b"[DONE]" in response.read(), "admitted stream failed")
    finally:
        release()
        for response in responses:
            response.close()
    eventually(lambda: api.usage(key)["account"]["reserved_tokens"] == 0)
    return {"instance_concurrency_cap": 4, "excess_status": 503, "admitted_requests": 4}


def redis_outage():
    _, key, _ = api.account("redis-outage")
    p = api.provider("redis-outage")
    before = stats()["calls"].get("normal", 0)
    runtime.stop(["redis"])
    try:
        checked(api.call("GET", "/health/ready")[0] == 503, "readiness ignored Redis outage")
        code, _, raw = api.chat(key, p["model_alias"])
        checked(code == 503 and json.loads(raw)["error"]["code"] == "rate_store_unavailable", "Redis outage bypassed rate limit")
        checked(stats()["calls"].get("normal", 0) == before, "Redis outage reached upstream")
    finally:
        runtime.launch("redis", [runtime.NATIVE / "bin/redis-server", runtime.ROOT / "config/redis.conf"], "128M")
        runtime.wait_http(BASE + "/health/ready")
    checked(api.chat(key, p["model_alias"])[0] == 200, "Redis recovery failed")
    return {"outage_status": 503, "upstream_calls_during_outage": 0, "recovered": True}


def database_outage_receipt_replay():
    _, key, _ = api.account("db-outage")
    p = api.provider("db-outage", "hold")
    response = api.stream(key, p["model_alias"])
    first = first_event(response)
    identity = response.headers["X-Request-ID"]
    spool = runtime.ROOT / "spool" / (identity + ".json")
    try:
        runtime.stop(["mysql"])
        checked(api.call("GET", "/health/ready")[0] == 503, "readiness ignored DB outage")
        checked(api.chat(key, p["model_alias"])[0] == 503, "DB outage admitted a new request")
        release()
        checked(b"[DONE]" in first + response.read(), "already-admitted stream was lost during DB outage")
        eventually(spool.exists, timeout=6, message="known usage was not spooled")
        receipt = json.loads(spool.read_text())
        checked(receipt["usage"]["total_tokens"] == 12 and spool.stat().st_mode & 0o077 == 0, "invalid or exposed spool receipt")
        saved = spool.read_bytes()
    finally:
        response.close()
        release()
        runtime.database_start()
        runtime.wait_http(BASE + "/health/ready")
    done_receipt(identity, key, timeout=16)
    eventually(lambda: not spool.exists(), timeout=16)
    # Replay the exact same durable receipt again: settlement must remain single.
    spool.write_bytes(saved)
    spool.chmod(0o600)
    eventually(lambda: not spool.exists(), timeout=16)
    account = api.usage(key)["account"]
    checked(account["used_tokens"] == 12 and account["reserved_tokens"] == 0, "replayed receipt charged twice")
    return {"known_usage_spooled": True, "replayed_twice": True, "used_tokens": 12, "reserved_tokens": 0}


def process_kill_recovery():
    _, key, _ = api.account("process-kill")
    p = api.provider("process-kill", "long")
    response = api.stream(key, p["model_alias"], base=PEER)
    first_event(response)
    identity = response.headers["X-Request-ID"]
    result = runtime.ctl("kill", "--signal=KILL", runtime.UNITS["peer"])
    checked(result.returncode == 0, "could not inject process kill")
    response.close()
    eventually(lambda: not runtime.active("peer"), timeout=5)
    # Explicit restart is part of this test; service Restart=no remains in force.
    runtime.launch("peer", [runtime.immutable_binary("gateway-race")], "768M", "200%", runtime.ROOT / "config/peer.env")
    runtime.wait_http(PEER + "/health/ready")
    receipt = done_receipt(identity, key, "pending_usage", timeout=18)
    checked(receipt["error"] == "owner_lease_expired" and api.usage(key)["account"]["reserved_tokens"] > 0, "killed request was silently refunded or fabricated")
    api.refund(identity)
    checked(api.chat(key, p["model_alias"], base=PEER)[0] == 200, "restarted instance cannot serve non-stream requests")
    return {"fault": "SIGKILL", "recovery": "pending_usage", "restart": "explicit", "unknown_usage_retained": True}


def real_local_model():
    # Reuse an already-running, free local model. No model/GPU job is launched here.
    _, key, _ = api.account("local-qwen")
    p = api.provider("local-qwen", upstream_model="office-qwen3-vl-8b", base_url="http://127.0.0.1:28282/v1", api_key="local-reuse", input_micro_usd_per_million_tokens=0, output_micro_usd_per_million_tokens=0)
    records = []
    prompt = [{"role": "user", "content": "只回答一个汉字：好"}]
    for stream in (False, True):
        code, headers, raw = api.chat(key, p["model_alias"], stream=stream, messages=prompt, max_tokens=32, temperature=0)
        checked(code == 200, f"real Qwen HTTP response: {code} {raw[:300]!r}")
        if stream:
            chunks = [json.loads(line[6:]) for line in raw.splitlines() if line.startswith(b"data: ") and line != b"data: [DONE]"]
            text = "".join(choice.get("delta", {}).get("content", "") for chunk in chunks for choice in chunk.get("choices", []))
            checked(b"[DONE]" in raw and bool(text.strip()), "real model SSE did not produce text")
        else:
            text = json.loads(raw)["choices"][0]["message"]["content"]
            checked(bool(text.strip()), "real model returned no text")
        receipt = done_receipt(headers["x-request-id"], key)
        checked(receipt["usage"]["total_tokens"] > 0 and receipt["cost_micro_usd"] == 0, "real model usage not recorded")
        records.append({"stream": stream, "request_id": receipt["id"], "usage": receipt["usage"], "output": text})
    checked(api.usage(key)["account"]["used_tokens"] == sum(r["usage"]["total_tokens"] for r in records), "real usage/account mismatch")
    return {"model": "office-qwen3-vl-8b", "endpoint": "existing local vLLM", "requests": records}


def final_invariants():
    rows = ledger_snapshot(api.accounts)
    checked(all(row["reserved"] == 0 for row in rows), "test left unresolved reservations")
    metrics = api.call("GET", "/metrics", admin=True)[2].decode()
    checked("llmgw_requests_total" in metrics and "llmgw_first_event_seconds" in metrics and "llmgw_inflight 0" in metrics, "Prometheus metrics missing or in-flight work leaked")
    races = list((runtime.ROOT / "logs").glob("race.*"))
    checked(not races, "race detector reported a data race; inspect logs/race.*")
    checked(runtime.active("peer"), "race-enabled instance stopped unexpectedly")
    return {"accounts_checked": len(rows), "account_equals_ledger": True, "unresolved_reservations": 0, "race_reports": 0, "rows": rows}


CASES = [basic_security, validation, normal_usage_and_tools, streaming, fallback_and_circuit,
         round_robin_and_disable, rejection_and_oversize, concurrent_idempotency, concurrent_quota,
         rate_limit, missing_usage_and_resolution, normalized_total, actual_usage_over_reservation, interrupted_streams,
         deadlines_and_cancellation, concurrency_bound, redis_outage, database_outage_receipt_replay,
         process_kill_recovery, real_local_model, final_invariants]


if __name__ == "__main__":
    report["total"] = len(CASES)
    save()
    try:
        for case in CASES:
            report["stage"] = case.__name__
            save()
            started = time.monotonic()
            details = case()
            report["cases"].append({"name": case.__name__, "state": "passed", "elapsed_s": round(time.monotonic()-started, 3), "details": details})
            print(case.__name__, "PASS", json.dumps(details, ensure_ascii=False), flush=True)
            save()
        report.update(state="passed", stage="finished", finished=time.time(), passed=len(CASES))
    except Exception as exc:
        report.update(state="failed", error=f"{type(exc).__name__}: {exc}", finished=time.time())
        traceback.print_exc()
    finally:
        try:
            api.cleanup()
            runtime.stop(["peer"])
            report["test_routes_and_keys_disabled"] = True
        except Exception as exc:
            report.update(state="failed", cleanup_error=str(exc))
        report["elapsed_s"] = round(time.time()-report["started"], 3)
        save()
    raise SystemExit(0 if report["state"] == "passed" else 1)
