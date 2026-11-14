"""Real Qwen load via the installed, unmodified vLLM benchmark CLI.

Uses frozen official BoolQ passages, with a JSONL format adapter. There are no
model downloads or model-server changes. Input counts reported by the CLI are
tokenizer estimates; provider receipts remain authoritative for accounting.
"""
import argparse
import hashlib
import json
import os
import random
import subprocess
import time
import traceback
import uuid

import runtime
from benchmark import MODEL, MODEL_BASE, atomic_json
from client import API, ledger_snapshot

BENCH_PYTHON = "/data/zjr/deepsearch_rl/venv/bin/python"
TOKENIZER = "/data/zjr/models/Qwen3-VL-8B-Instruct"
OVERLAY = "/data/zjr/enterprise_kb_agent/judge_packages"


def datasets(folder, count, seed):
    source = runtime.ROOT / "datasets/public-qa/boolq.jsonl"
    rows = [json.loads(line) for line in source.read_text().splitlines()]
    indices = random.Random(seed).sample(range(len(rows)), count)
    short = [{"source_index": i, "prompt": "Read the passage and explain your yes/no answer in two sentences.\n\nPassage: " + rows[i]["passage"] + "\n\nQuestion: " + rows[i]["question"]} for i in indices]
    long_output = [{"source_index": i, "prompt": "Write a detailed English explanation of this passage and question in at least 500 words, then conclude with yes or no.\n\nPassage: " + rows[i]["passage"] + "\n\nQuestion: " + rows[i]["question"]} for i in indices[:8]]
    long_input = []
    for number in range(min(8, count)):
        order = random.Random(seed+100+number).sample(range(len(rows)), len(rows))
        words = []; used = []
        for i in order:
            words.extend(rows[i]["passage"].split()); used.append(i)
            if len(words) >= 5000:break
        prompt = "Summarize the main topics of these passages in three sentences.\n\n" + " ".join(words[:5000])
        if len(json.dumps({"messages": [{"role": "user", "content": prompt}]}).encode()) > 60*1024:
            raise ValueError("Long-input workload exceeds the gateway body budget")
        long_input.append({"source_indices": used, "prompt": prompt})
    result = {}
    for name, data in (("short", short), ("long-output", long_output), ("long-input", long_input)):
        path = folder/(name+".jsonl")
        path.write_text("\n".join(json.dumps(row, ensure_ascii=False) for row in data)+"\n"); path.chmod(0o600)
        result[name] = {"path": str(path), "rows": len(data), "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}
    atomic_json(folder/"dataset-manifest.json", {"source": str(source), "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
                "source_url": "https://huggingface.co/datasets/google/boolq", "seed": seed,
                "format": "vLLM custom JSONL; original passages retained; load instructions supplied locally", "workloads": result})
    return result


def request_totals(account_id):
    uuid.UUID(account_id)
    values = runtime.sql("USE llm_gateway; SELECT COUNT(*),COALESCE(SUM(prompt_tokens),0),COALESCE(SUM(completion_tokens),0),"
                         "COALESCE(SUM(total_tokens),0),COALESCE(SUM(state<>'settled' OR outcome<>'succeeded'),0) "
                         "FROM requests FORCE INDEX(requests_account) WHERE account_id='"+account_id+"';")
    return dict(zip(("requests", "prompt_tokens", "completion_tokens", "total_tokens", "not_succeeded"), map(int, values.split("\t"))))


def main(count=100, seed=20261114, pilot=False):
    if not 3 <= count <= 100:raise ValueError("Use 3..100 load samples")
    runtime.install_stop_handler(); api = API(); process = None
    folder = runtime.ROOT / "checks/real-load" / time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    folder.mkdir(parents=True, exist_ok=False); folder.chmod(0o700)
    inputs = datasets(folder, count, seed)
    report = {"state": "running", "stage": "prepare", "started": time.time(), "run_dir": str(folder), "namespace": api.prefix,
              "generator": "installed vLLM bench serve, unmodified", "runner_python": BENCH_PYTHON,
              "seed": seed, "pilot": pilot, "cases": [], "cases_completed": 0, "completed": 0,
              "protocol": {"workload": "frozen official BoolQ passages in vLLM custom JSONL format", "model_max_num_seqs": 1,
                           "client_concurrency": [1] if pilot else [1, 4, 8], "retries": 0,
                           "ready_probe": "disabled; one explicit tracked warmup per case",
                           "timing": "vLLM client-side TTFT/ITL/TPOT/E2EL; TTFT includes first streamed choice event",
                           "token_accounting": "model usage receipts; CLI input lengths exclude chat template tokens"}}
    def save():
        report["elapsed_s"] = round(time.time()-report["started"], 3)
        atomic_json(folder/"report.json", report); atomic_json(runtime.ROOT/"checks/real-load.json", report)
    save()
    try:
        provider = api.provider("real-load", upstream_model=MODEL, base_url=MODEL_BASE, api_key="local-reuse",
                                input_micro_usd_per_million_tokens=0, output_micro_usd_per_million_tokens=0)
        scenarios = [("short", concurrency, arm, 64) for concurrency in ([1] if pilot else [1, 4, 8])
                     for arm in (("gateway", "direct") if concurrency == 4 else ("direct", "gateway"))]
        if not pilot:scenarios += [(name, 1, arm, maximum) for name, maximum in (("long-output", 512), ("long-input", 128)) for arm in ("direct", "gateway")]
        for number, (workload, concurrency, arm, maximum) in enumerate(scenarios):
            name = f"{number+1:02d}-{workload}-c{concurrency}-{arm}"
            case_dir = folder/name; case_dir.mkdir(); case_dir.chmod(0o700)
            report.update(stage=name, current={"workload": workload, "concurrency": concurrency, "arm": arm}); save()
            rows = [json.loads(line) for line in __import__('pathlib').Path(inputs[workload]["path"]).read_text().splitlines()]
            random.Random(seed).shuffle(rows)
            gateway = arm == "gateway"; key = "local-reuse"; alias = MODEL; account = None
            if gateway:
                account, key, _ = api.account(name, quota=10000000, rpm=10000); alias = provider["model_alias"]
            base = api.base if gateway else MODEL_BASE.removesuffix("/v1")
            warm, _ = api.json("POST", "/v1/chat/completions", {"model": alias, "messages": [{"role": "user", "content": [{"type": "text", "text": rows[0]["prompt"]}]}],
                               "max_completion_tokens": maximum, "temperature": 0, "stream": False}, base=base, key=key, timeout=150)
            before = request_totals(account["id"]) if gateway else None
            command = [BENCH_PYTHON, "-B", "-m", "vllm.entrypoints.cli.main", "bench", "serve", "--backend", "openai-chat",
                       "--base-url", base, "--endpoint", "/v1/chat/completions", "--model", alias, "--tokenizer", TOKENIZER,
                       "--dataset-name", "custom", "--dataset-path", inputs[workload]["path"], "--custom-skip-chat-template",
                       "--custom-output-len", str(maximum), "--num-prompts", str(len(rows)), "--max-concurrency", str(concurrency),
                       "--request-rate", "inf", "--seed", str(seed), "--ready-check-timeout-sec", "0", "--disable-tqdm",
                       "--percentile-metrics", "ttft,tpot,itl,e2el", "--metric-percentiles", "50,95,99", "--save-result", "--save-detailed",
                       "--result-dir", str(case_dir), "--result-filename", "vllm.json"]
            env = {**os.environ, "PYTHONPATH": OVERLAY, "OPENAI_API_KEY": key, "HF_HUB_OFFLINE": "1", "TRANSFORMERS_OFFLINE": "1",
                   "VLLM_USAGE_STATS": "0", "DO_NOT_TRACK": "1", "OMP_NUM_THREADS": "2", "OPENBLAS_NUM_THREADS": "2", "TOKENIZERS_PARALLELISM": "false",
                   "CUDA_CACHE_PATH": str(runtime.ROOT/"cache/cuda"), "VLLM_CACHE_ROOT": str(runtime.ROOT/"cache/vllm")}
            atomic_json(case_dir/"command.json", {"argv": command, "api_key_source": "child environment, not persisted"})
            with (case_dir/"client.log").open("w") as log:
                process = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT)
                try:exit_code = process.wait(timeout=900)
                finally:
                    if process.poll() is None:
                        process.terminate()
                        try:process.wait(timeout=10)
                        except subprocess.TimeoutExpired:process.kill(); process.wait()
                process = None
            if exit_code != 0:raise RuntimeError(f"vLLM benchmark exited {exit_code}; see {case_dir/'client.log'}")
            value = json.loads((case_dir/"vllm.json").read_text())
            checks = {"all_completed": value["completed"] == len(rows), "no_client_errors": not any(value.get("errors", [])),
                      "nonempty_outputs": len(value.get("generated_texts", [])) == len(rows) and all(t.strip() for t in value["generated_texts"])}
            case = {"name": name, "arm": arm, "workload": workload, "concurrency": concurrency, "planned_calls": len(rows),
                    "successful": value["completed"], "warmup_usage": warm["usage"], "output_token_limit": maximum,
                    "duration_s": value["duration"], "rps": value["request_throughput"], "output_tokens_s": value["output_throughput"],
                    "input_tokens_range": [min(value["input_lens"]), max(value["input_lens"])],
                    "p95_ttft_ms": value["p95_ttft_ms"], "p95_e2el_ms": value["p95_e2el_ms"],
                    "p99_e2el_ms": value["p99_e2el_ms"], "mean_tpot_ms": value["mean_tpot_ms"], "checks": checks,
                    "raw_report": str(case_dir/"vllm.json")}
            if gateway:
                after = request_totals(account["id"]); snapshot = ledger_snapshot([account["id"]], with_requests=True)[0]
                delta = {k:after[k]-before[k] for k in before}
                checks.update(request_count_exact=delta["requests"] == len(rows), all_requests_settled=after["not_succeeded"] == 0,
                              output_tokens_match=delta["completion_tokens"] == value["total_output_tokens"],
                              ledger_exact=snapshot["used"] == snapshot["ledger_used"] == snapshot["request_tokens"],
                              no_reservations=snapshot["reserved"] == snapshot["ledger_reserved"] == snapshot["unresolved_requests"] == 0)
                case.update(account_id=account["id"], ledger=snapshot, provider_usage_delta=delta)
            case["state"] = "passed" if all(checks.values()) else "validation_failed"
            report["cases"].append(case); report["cases_completed"] = len(report["cases"])
            report["completed"] = sum(c["successful"] for c in report["cases"]); save()
            print(json.dumps({k:case[k] for k in ("name", "state", "successful", "duration_s", "p95_ttft_ms", "p95_e2el_ms", "checks")}, ensure_ascii=False), flush=True)
        report.update(state="passed" if all(c["state"] == "passed" for c in report["cases"]) else "completed_with_validation_failures", stage="finished")
    except runtime.TaskStopped:report.update(state="stopped", error="explicit stop")
    except Exception as exc:
        report.update(state="failed", error=type(exc).__name__+": "+str(exc)); traceback.print_exc()
    finally:
        if process is not None and process.poll() is None:
            process.terminate()
            try:process.wait(timeout=10)
            except subprocess.TimeoutExpired:process.kill(); process.wait()
        try:api.cleanup(); report["cleanup_completed"] = True
        except Exception as exc:report.update(state="failed", cleanup_error=str(exc))
        save()
    print(json.dumps({k:report.get(k) for k in ("state", "completed", "cases_completed", "error", "run_dir")}, ensure_ascii=False), flush=True)
    return 0 if report["state"] == "passed" else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser(); parser.add_argument("--samples", type=int, default=100)
    parser.add_argument("--seed", type=int, default=20261114); parser.add_argument("--pilot", action="store_true")
    args = parser.parse_args(); raise SystemExit(main(args.samples, args.seed, args.pilot))
