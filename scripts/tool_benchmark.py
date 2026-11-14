"""Native tool-call transport check on a pinned BFCL subset.

Upstream AST matching code is unmodified. Only its model-name registry is
provided locally, so scoring does not import every remote model adapter.
The required-tool protocol is a local subset experiment, not a leaderboard run.
"""
import argparse
import hashlib
import json
import random
import sys
import time
import traceback
import types
import urllib.request
from functools import lru_cache

import runtime
from benchmark import ARMS, MODEL, MODEL_BASE, atomic_json
from client import API, HTTP, eventually, ledger_snapshot

BFCL_COMMIT = "6ea57973c7a6097fd7c5915698c54c17c5b1b6c8"
BFCL_ROOT = runtime.ROOT / "toolchain/bfcl" / BFCL_COMMIT


@lru_cache(maxsize=1)
def official_checker():
    # The checker consults this registry only for dot-to-underscore conversion.
    # Match the OpenAI tool-name encoding used by this transport adapter.
    registry = types.ModuleType("bfcl_eval.constants.model_config")
    registry.MODEL_CONFIG_MAPPING = {
        "llmgw": types.SimpleNamespace(underscore_to_dot=True)
    }
    sys.modules[registry.__name__] = registry
    sys.path.insert(0, str(BFCL_ROOT))
    from bfcl_eval.constants.enums import Language
    from bfcl_eval.constants.type_mappings import GORILLA_TO_OPENAPI
    from bfcl_eval.eval_checker.ast_eval.ast_checker import ast_checker
    return ast_checker, Language.PYTHON, GORILLA_TO_OPENAPI


def openapi_schema(value):
    if isinstance(value, list):
        return [openapi_schema(item) for item in value]
    if not isinstance(value, dict):
        return value
    mapping = official_checker()[2]
    # Go's encoding/json sorts map keys. Use the same schema order on direct
    # requests so a tool chat template sees the same property order in all arms.
    return {key: mapping.get(value[key], value[key]) if key == "type" and isinstance(value[key], str)
            else openapi_schema(value[key]) for key in sorted(value)}


def tools_for(item):
    return [{"type": "function", "function": {
        "name": value["name"].replace(".", "_"),
        "description": value["description"],
        "parameters": openapi_schema(value["parameters"]),
    }} for value in item["function"]]


def select_items(count, seed):
    if not 3 <= count <= 100:
        raise ValueError("Use 3..100 tool questions")
    counts = {"simple_python": max(1, count * 4 // 10),
              "multiple": max(1, count * 4 // 10)}
    counts["parallel"] = count - sum(counts.values())
    items = []
    for number, (category, amount) in enumerate(counts.items()):
        path = BFCL_ROOT / "bfcl_eval/data" / ("BFCL_v4_" + category + ".json")
        gold_path = path.parent / "possible_answer" / path.name
        questions = [json.loads(line) for line in path.read_text().splitlines() if line]
        answers = {row["id"]: row["ground_truth"] for row in
                   (json.loads(line) for line in gold_path.read_text().splitlines() if line)}
        indices = sorted(random.Random(seed + number).sample(range(len(questions)), amount))
        for index in indices:
            row = questions[index]
            if len(row["question"]) != 1:
                raise ValueError("Expected a single-turn BFCL item")
            items.append({**row, "category": category, "gold": answers[row["id"]]})
    return items


def canonical_tools(calls):
    result = []
    for call in calls or []:
        function = call["function"]
        arguments = json.loads(function["arguments"])
        if not isinstance(arguments, dict) or not function["name"]:
            raise ValueError("Expected named tools with JSON object arguments")
        result.append({function["name"]: arguments})
    return result


def normalized_calls(calls):
    return sorted(json.dumps(call, sort_keys=True, ensure_ascii=False) for call in calls)


def grade(item, calls):
    checker, language, _ = official_checker()
    return checker(item["function"], calls, item["gold"], language, item["category"], "llmgw")


def read_sse(response, started):
    """Merge OpenAI tool argument deltas by index; require the end marker."""
    calls = {}; text = []; usage = None; done = False; finish = None
    first_content = None; events = 0
    for line in response:
        if not line.startswith(b"data:"):
            continue
        data = line[5:].strip()
        if data == b"[DONE]":
            done = True
            continue
        chunk = json.loads(data); events += 1
        if "error" in chunk:
            raise ValueError("stream error: " + str(chunk["error"])[:250])
        usage = chunk.get("usage") or usage
        for choice in chunk.get("choices", []):
            delta = choice.get("delta", {})
            content = delta.get("content") or ""
            if content:
                text.append(content)
                if first_content is None:first_content = time.monotonic() - started
            for call in delta.get("tool_calls") or []:
                if first_content is None:first_content = time.monotonic() - started
                value = calls.setdefault(call["index"], {"id": "", "type": "function", "function": {"name": "", "arguments": ""}})
                if call.get("id"):
                    if value["id"] and value["id"] != call["id"]:
                        raise ValueError("Tool ID changed during a stream")
                    value["id"] = call["id"]
                for field in ("name", "arguments"):
                    value["function"][field] += (call.get("function") or {}).get(field) or ""
            finish = choice.get("finish_reason") or finish
    if not done:
        raise ValueError("SSE end marker is missing")
    if calls and sorted(calls) != list(range(len(calls))):
        raise ValueError("Non-contiguous tool indices")
    return {"tool_calls": [calls[i] for i in sorted(calls)], "content": "".join(text),
            "usage": usage, "finish_reason": finish, "events": events,
            "first_event_s": first_content, "done_received": done}


def infer(item, arm, api, key, alias, seed):
    gateway = arm != "direct"; streaming = arm == "gateway_sse"
    payload = {"model": alias if gateway else MODEL,
               "messages": item["question"][0], "tools": tools_for(item),
               "tool_choice": "required", "max_tokens": 512,
               "temperature": 0, "top_p": 1, "seed": seed, "stream": streaming}
    if streaming:payload["stream_options"] = {"include_usage": True}
    headers = {"Content-Type": "application/json", "Authorization": "Bearer " + (key if gateway else "local-reuse")}
    if gateway:headers["Idempotency-Key"] = api.prefix + "-" + item["id"] + "-" + arm
    url = (api.base + "/v1" if gateway else MODEL_BASE) + "/chat/completions"
    request = urllib.request.Request(url, json.dumps(payload).encode(), headers)
    started = time.monotonic()
    with HTTP.open(request, timeout=150) as response:
        identity = response.headers.get("X-Request-ID")
        if streaming:
            row = read_sse(response, started)
        else:
            value = json.load(response); choice = value["choices"][0]
            row = {"tool_calls": choice["message"].get("tool_calls") or [],
                   "content": choice["message"].get("content"),
                   "usage": value.get("usage"), "finish_reason": choice.get("finish_reason")}
    row.update(id=item["id"], category=item["category"], arm=arm, status="completed",
               elapsed_s=round(time.monotonic()-started, 3), request_id=identity)
    usage = row["usage"]
    if not usage or usage["total_tokens"] != usage["prompt_tokens"] + usage["completion_tokens"]:
        raise ValueError("Invalid model usage")
    try:
        row["calls"] = canonical_tools(row["tool_calls"])
        row["grading"] = grade(item, row["calls"])
        row["format_valid"] = bool(row["calls"])
    except (ValueError, KeyError, TypeError) as exc:
        row.update(calls=[], format_valid=False, grading={"valid": False, "error": [str(exc)]})
    row["correct"] = bool(row["grading"]["valid"])
    if gateway:
        receipt = eventually(lambda: (v if (v := api.receipt(identity, key))["state"] == "settled" else None), timeout=20)
        row["receipt_matches_usage"] = all(receipt["usage"][k] == usage[k] for k in ("prompt_tokens", "completion_tokens", "total_tokens"))
    return row


def summarize(records, items):
    result = {}
    for arm in ARMS:
        rows = [row for row in records if row["arm"] == arm]
        correct = sum(row.get("correct", False) for row in rows)
        result[arm] = {"completed": len(rows), "planned": len(items), "correct": correct,
                       "accuracy_pct": round(correct*100/len(items), 2) if len(rows) == len(items) else None,
                       "errors": sum(row["status"] == "error" for row in rows),
                       "format_failures": sum(row["status"] == "completed" and not row.get("format_valid") for row in rows),
                       "truncated": sum(row.get("finish_reason") == "length" for row in rows)}
    return result


def main(count=50, seed=20261114):
    runtime.install_stop_handler(); api = API()
    folder = runtime.ROOT / "checks/tools" / time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    folder.mkdir(parents=True, exist_ok=False); folder.chmod(0o700)
    items = select_items(count, seed)
    report = {"state": "running", "stage": "evaluate", "started": time.time(), "run_dir": str(folder),
              "namespace": api.prefix, "questions": count, "planned_calls": count*3, "completed": 0,
              "seed": seed, "bfcl_commit": BFCL_COMMIT, "records": [],
              "protocol": {"tool_choice": "required", "max_tokens": 512, "temperature": 0,
                           "arm_order": "rotate direct/JSON/SSE", "retries": 0,
                           "scope": "local single-turn BFCL subset; not official leaderboard score",
                           "checker": "unchanged official ast_checker; only local name-encoding registry supplied"}}
    def save():
        report["elapsed_s"] = round(time.time()-report["started"], 3)
        atomic_json(folder/"report.json", report); atomic_json(runtime.ROOT/"checks/tools.json", report)
    atomic_json(folder/"samples.json", items); save()
    try:
        account, key, _ = api.account("bfcl", quota=1000000, rpm=600)
        provider = api.provider("bfcl", upstream_model=MODEL, base_url=MODEL_BASE, api_key="local-reuse",
                                input_micro_usd_per_million_tokens=0, output_micro_usd_per_million_tokens=0)
        report["account_id"] = account["id"]
        with (folder/"responses.jsonl").open("w") as output:
            for index, item in enumerate(items):
                for arm in ARMS[index % 3:] + ARMS[:index % 3]:
                    report["current"] = {"id": item["id"], "arm": arm}; save()
                    try:row = infer(item, arm, api, key, provider["model_alias"], seed+index)
                    except runtime.TaskStopped:raise
                    except Exception as exc:
                        row = {"id": item["id"], "category": item["category"], "arm": arm,
                               "status": "error", "correct": False, "error": type(exc).__name__+": "+str(exc)[:300]}
                    report["records"].append(row); output.write(json.dumps(row, ensure_ascii=False)+"\n"); output.flush()
                    report.update(completed=len(report["records"]), scores=summarize(report["records"], items)); save()
        snapshot = ledger_snapshot([account["id"]], with_requests=True)[0]
        gateway = [r for r in report["records"] if r["arm"] != "direct"]
        checks = {"all_calls_completed": all(r["status"] == "completed" for r in report["records"]),
                  "receipts_match_usage": all(r.get("receipt_matches_usage", False) for r in gateway),
                  "ledger_exact": snapshot["used"] == snapshot["ledger_used"] == snapshot["request_tokens"] == sum(r.get("usage", {}).get("total_tokens", 0) for r in gateway),
                  "no_reservations": snapshot["reserved"] == snapshot["ledger_reserved"] == snapshot["unresolved_requests"] == 0}
        by_id = {(r["id"], r["arm"]): r for r in report["records"]}
        comparisons = {}
        for arm in ARMS[1:]:
            differences = []
            for item in items:
                a, b = by_id[item["id"], "direct"], by_id[item["id"], arm]
                if a["status"] != "completed" or b["status"] != "completed" or normalized_calls(a.get("calls", [])) != normalized_calls(b.get("calls", [])):
                    differences.append(item["id"])
            comparisons[arm] = {"total": count, "same_calls": count-len(differences), "different_items": differences}
        report.update(checks=checks, ledger=snapshot, comparisons=comparisons, stage="finished",
                      state="completed" if all(checks.values()) else "completed_with_validation_failures")
    except runtime.TaskStopped:report.update(state="stopped", error="explicit stop")
    except Exception as exc:
        report.update(state="failed", error=type(exc).__name__+": "+str(exc)); traceback.print_exc()
    finally:
        try:api.cleanup(); report["cleanup_completed"] = True
        except Exception as exc:report.update(state="failed", cleanup_error=str(exc))
        save()
    print(json.dumps({k:report.get(k) for k in ("state", "completed", "scores", "checks", "comparisons", "run_dir")}, ensure_ascii=False), flush=True)
    return 0 if report["state"] == "completed" else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(); parser.add_argument("--samples", type=int, default=50)
    parser.add_argument("--seed", type=int, default=20261114); args = parser.parse_args()
    raise SystemExit(main(args.samples, args.seed))
