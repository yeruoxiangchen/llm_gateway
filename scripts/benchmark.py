"""Small, reproducible public QA benchmark: direct model, gateway JSON and SSE.

Questions and gold labels come from the original GSM8K test and BoolQ dev files.
Prompts and subset sampling are this project's zero-shot evaluation protocol.
"""
import argparse
from decimal import Decimal, InvalidOperation
import hashlib
import io
import json
from pathlib import Path
import random
import re
import time
import traceback
import urllib.request

from client import API, HTTP, eventually, ledger_snapshot
import runtime

MODEL_BASE = "http://127.0.0.1:28282/v1"
MODEL = "office-qwen3-vl-8b"
ARMS = ("direct", "gateway_json", "gateway_sse")
SOURCES = {
    "gsm8k": {"url": "https://raw.githubusercontent.com/openai/grade-school-math/master/grade_school_math/data/test.jsonl",
              "homepage": "https://github.com/openai/grade-school-math", "split": "test", "rows": 1319,
              "license": "MIT", "sha256": "3730d312f6e3440559ace48831e51066acaca737f6eabec99bccb9e4b3c39d14"},
    "boolq": {"url": "https://huggingface.co/datasets/google/boolq/resolve/35b264d03638db9f4ce671b711558bf7ff0f80d5/data/validation-00000-of-00001.parquet",
              "homepage": "https://github.com/google-research-datasets/boolean-questions", "split": "validation", "rows": 3270,
              "original_split": "dev", "format": "parquet", "revision": "35b264d03638db9f4ce671b711558bf7ff0f80d5",
              "license": "CC-BY-SA-3.0"},
}


def atomic_json(path, value):
    temp = path.with_suffix(path.suffix + ".tmp")
    temp.write_text(json.dumps(value, ensure_ascii=False, indent=2))
    temp.chmod(0o600)
    temp.replace(path)


def dataset_files():
    folder = runtime.ROOT / "datasets/public-qa"
    folder.mkdir(parents=True, exist_ok=True)
    folder.chmod(0o700)
    manifest = {}
    for name, source in SOURCES.items():
        path = folder / (name + ".jsonl")
        download_path = path.with_suffix(".parquet") if source.get("format") == "parquet" else path
        if not download_path.exists():
            print("Downloading official dataset:", name, flush=True)
            with urllib.request.urlopen(source["url"], timeout=45) as response:
                raw = response.read(8 * 1024**2 + 1)
            if len(raw) > 8 * 1024**2:
                raise ValueError("Unexpected dataset size")
            temp = download_path.with_suffix(".download")
            temp.write_bytes(raw)
            temp.chmod(0o600)
            temp.replace(download_path)
        raw = download_path.read_bytes()
        digest = hashlib.sha256(raw).hexdigest()
        if source.get("sha256") and digest != source["sha256"]:
            raise ValueError("Dataset checksum changed: " + name)
        if source.get("format") == "parquet":
            import pyarrow.parquet
            records = pyarrow.parquet.read_table(io.BytesIO(raw)).to_pylist()
            converted = ("\n".join(json.dumps(row, ensure_ascii=False) for row in records) + "\n").encode()
            if not path.exists():
                path.write_bytes(converted);path.chmod(0o600)
            elif path.read_bytes() != converted:
                raise ValueError("Converted dataset file changed: " + name)
        else:
            records = [json.loads(line) for line in raw.splitlines() if line]
        if len(records) != source["rows"]:
            raise ValueError("Unexpected original split size: " + name)
        manifest[name] = {**source, "sha256": digest, "bytes": len(raw), "path": str(path),
                          "download_path": str(download_path), "jsonl_sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    prior = folder / "manifest.json"
    if prior.exists():
        old = json.loads(prior.read_text())
        for name in SOURCES:
            if old[name]["sha256"] != manifest[name]["sha256"]:
                raise ValueError("Frozen dataset file changed: " + name)
    atomic_json(prior, manifest)
    return manifest


def numeric_answer(text):
    matches = re.findall(r"####\s*([-+]?\d[\d,]*(?:\.\d+)?)", text)
    if not matches:
        return None
    try:
        value = Decimal(matches[-1].replace(",", ""))
        return format(value.normalize(), "f") if value.is_finite() else None
    except InvalidOperation:
        return None


def extract_answer(dataset, text):
    if dataset == "gsm8k":
        return numeric_answer(text)
    value = text.strip().lower()
    return value if value in ("yes", "no") else None


def subset(manifest, count, seed):
    if not 1 <= count <= 100:
        raise ValueError("Use 1..100 samples per dataset")
    result = []
    for number, (name, metadata) in enumerate(manifest.items()):
        rows = [json.loads(line) for line in Path(metadata["path"]).read_text().splitlines()]
        indices = sorted(random.Random(seed + number).sample(range(len(rows)), count))
        for index in indices:
            row = rows[index]
            if name == "gsm8k":
                gold = numeric_answer(row["answer"])
                prompt = ("Solve this arithmetic word problem. Give a brief solution, then end with a final line in the form #### <number>.\n\n" + row["question"])
                maximum = 512
            else:
                gold = "yes" if row["answer"] else "no"
                prompt = ("Read the passage and answer the question. Reply with exactly yes or no.\n\nPassage:\n" +
                          row["passage"] + "\n\nQuestion: " + row["question"])
                maximum = 16
            if gold is None:
                raise ValueError("Could not parse official answer")
            result.append({"id": name + "-" + str(index), "dataset": name, "split": metadata["split"],
                           "source_index": index, "gold": gold, "prompt": prompt, "max_tokens": maximum})
    return result


def inference(item, arm, seed, alias, key, prefix):
    stream = arm == "gateway_sse"
    gateway = arm != "direct"
    payload = {"model": alias if gateway else MODEL,
               "messages": [{"role": "user", "content": item["prompt"]}],
               "max_tokens": item["max_tokens"], "temperature": 0, "top_p": 1,
               "seed": seed, "stream": stream}
    if stream:
        payload["stream_options"] = {"include_usage": True}
    headers = {"Content-Type": "application/json", "Authorization": "Bearer " + (key if gateway else "local-reuse")}
    if gateway:
        headers["Idempotency-Key"] = prefix + "-" + item["id"] + "-" + arm
    url = ("http://127.0.0.1:29480/v1" if gateway else MODEL_BASE) + "/chat/completions"
    request = urllib.request.Request(url, json.dumps(payload).encode(), headers)
    started = time.monotonic()
    with HTTP.open(request, timeout=150) as response:
        request_id = response.headers.get("X-Request-ID")
        raw = response.read(4 * 1024**2 + 1)
        if len(raw) > 4 * 1024**2:
            raise ValueError("Response exceeded benchmark limit")
    if stream:
        text = "";usage = None;finished = None;done = False
        for line in raw.splitlines():
            if not line.startswith(b"data: "):
                continue
            if line[6:].strip() == b"[DONE]":
                done = True
                continue
            chunk = json.loads(line[6:])
            if "error" in chunk:
                raise ValueError("Gateway returned a stream error")
            usage = chunk.get("usage") or usage
            for choice in chunk.get("choices", []):
                text += choice.get("delta", {}).get("content", "") or ""
                finished = choice.get("finish_reason") or finished
        if not done:
            raise ValueError("SSE completion marker missing")
    else:
        data = json.loads(raw)
        text = data["choices"][0]["message"].get("content") or ""
        usage = data.get("usage")
        finished = data["choices"][0].get("finish_reason")
    if not usage or usage.get("total_tokens", -1) != usage.get("prompt_tokens", 0) + usage.get("completion_tokens", 0):
        raise ValueError("Valid usage missing from real model response")
    predicted = extract_answer(item["dataset"], text)
    return {"id": item["id"], "dataset": item["dataset"], "arm": arm,
            "status": "completed", "prediction": predicted, "gold": item["gold"],
            "correct": predicted is not None and predicted == item["gold"],
            "format_valid": predicted is not None, "text": text, "finish_reason": finished,
            "usage": usage, "elapsed_s": round(time.monotonic() - started, 3), "request_id": request_id}


def scores(records, items):
    result = {}
    for dataset in SOURCES:
        total = sum(item["dataset"] == dataset for item in items)
        result[dataset] = {}
        for arm in ARMS:
            rows = [row for row in records if row["dataset"] == dataset and row["arm"] == arm]
            correct = sum(row.get("correct", False) for row in rows)
            result[dataset][arm] = {"correct": correct, "total": total,
                "accuracy_pct": round(correct * 100 / total, 2) if len(rows) == total else None, "completed": len(rows),
                "errors": sum(row.get("status") == "error" for row in rows),
                "format_failures": sum(row.get("status") == "completed" and not row.get("format_valid") for row in rows),
                "truncated": sum(row.get("finish_reason") == "length" for row in rows)}
    return result


def main(count, seed):
    runtime.install_stop_handler()
    api = API()
    folder = runtime.ROOT / "checks/benchmark" / time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    folder.mkdir(parents=True, exist_ok=False)
    folder.chmod(0o700)
    report = {"state": "running", "stage": "prepare", "started": time.time(), "namespace": api.prefix,
              "run_dir": str(folder), "per_dataset": count, "seed": seed, "completed": 0,
              "planned_calls": count * 2 * 3, "model": MODEL, "records": [],
              "protocol": {"zero_shot": True, "temperature": 0, "top_p": 1, "concurrency": 1,
                           "arm_order": "rotate direct/JSON/SSE per question", "retries": 0,
                           "scoring": "GSM8K final #### numeric exact match; BoolQ exact yes/no"}}
    def save():
        report["elapsed_s"] = round(time.time() - report["started"], 3)
        atomic_json(folder / "report.json", report)
        atomic_json(runtime.ROOT / "checks/benchmark.json", report)
    save()
    try:
        manifest = dataset_files()
        report["sources"] = manifest
        items = subset(manifest, count, seed)
        atomic_json(folder / "samples.json", items)
        with HTTP.open(MODEL_BASE + "/models", timeout=5) as response:
            report["served_model_metadata"] = json.load(response)
        account, key, _ = api.account("public-benchmark", quota=1000000, rpm=600)
        report["account_id"] = account["id"]
        provider = api.provider("public-benchmark", upstream_model=MODEL, base_url=MODEL_BASE, api_key="local-reuse",
                                input_micro_usd_per_million_tokens=0, output_micro_usd_per_million_tokens=0)
        report.update(stage="evaluate", samples_path=str(folder / "samples.json"))
        save()
        with (folder / "responses.jsonl").open("w") as output:
            for index, item in enumerate(items):
                order = ARMS[index % 3:] + ARMS[:index % 3]
                for arm in order:
                    report["current"] = {"item": item["id"], "arm": arm};save()
                    try:
                        row = inference(item, arm, seed + index, provider["model_alias"], key, api.prefix)
                        if arm != "direct":
                            receipt = eventually(lambda: (value if (value := api.receipt(row["request_id"], key))["state"] == "settled" else None), timeout=15)
                            row["receipt_matches_usage"] = all(receipt["usage"][field] == row["usage"][field] for field in ("prompt_tokens", "completion_tokens", "total_tokens"))
                    except runtime.TaskStopped:
                        raise
                    except Exception as exc:
                        row = {"id": item["id"], "dataset": item["dataset"], "arm": arm,
                               "status": "error", "correct": False, "error": type(exc).__name__ + ": " + str(exc)[:250]}
                    report["records"].append(row)
                    output.write(json.dumps(row, ensure_ascii=False) + "\n");output.flush()
                    report.update(completed=len(report["records"]), scores=scores(report["records"], items))
                    print(item["id"], arm, row["status"], "correct=" + str(row.get("correct")), flush=True)
                    save()
        report["stage"] = "reconcile";save()
        usage = api.usage(key)["account"]
        snapshot = ledger_snapshot([account["id"]])
        gateway = [row for row in report["records"] if row["arm"] != "direct"]
        checks = {"all_calls_completed": all(row["status"] == "completed" for row in report["records"]),
                  "all_gateway_receipts_match": all(row.get("receipt_matches_usage", False) for row in gateway),
                  "account_matches_ledger": all(row["used"] == row["ledger_used"] and row["reserved"] == row["ledger_reserved"] for row in snapshot),
                  "no_reservations": usage["reserved_tokens"] == 0,
                  "used_equals_gateway_response_usage": usage["used_tokens"] == sum(row.get("usage", {}).get("total_tokens", 0) for row in gateway)}
        report.update(checks=checks, ledger=snapshot, gateway_used_tokens=usage["used_tokens"])
        by_id = {(row["id"], row["arm"]): row for row in report["records"]}
        comparisons = {}
        for arm in ARMS[1:]:
            pairs = [(by_id[(item["id"], "direct")], by_id[(item["id"], arm)]) for item in items]
            comparisons[arm] = {"total": len(items),
                "answer_agreement": sum(a.get("prediction") is not None and a.get("prediction") == b.get("prediction") for a, b in pairs),
                "exact_text_agreement": sum(a.get("status") == b.get("status") == "completed" and a["text"] == b["text"] for a, b in pairs),
                "different_items": [a["id"] for a, b in pairs if a.get("prediction") is None or a.get("prediction") != b.get("prediction")]}
        report.update(comparisons=comparisons, stage="finished", finished=time.time(),
                      state="completed" if all(checks.values()) else "completed_with_validation_failures")
    except runtime.TaskStopped:
        report.update(state="stopped", error="explicit stop request")
    except Exception as exc:
        report.update(state="failed", error=type(exc).__name__ + ": " + str(exc));traceback.print_exc()
    finally:
        try:api.cleanup();report["cleanup_completed"] = True
        except Exception as exc:report["cleanup_error"] = type(exc).__name__;report["state"] = "failed"
        save()
    print(json.dumps({key: report.get(key) for key in ("state", "completed", "scores", "comparisons", "checks", "run_dir")}, ensure_ascii=False, indent=2), flush=True)
    return 0 if report["state"] == "completed" else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--samples", type=int, default=20)
    parser.add_argument("--seed", type=int, default=20261113)
    args = parser.parse_args()
    if args.prepare_only:
        print(json.dumps(dataset_files(), ensure_ascii=False, indent=2))
    else:
        raise SystemExit(main(args.samples, args.seed))
