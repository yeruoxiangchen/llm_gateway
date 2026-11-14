"""Paced HTTP load, resource sampling and post-run ledger reconciliation.

The upstream is the deterministic protocol fixture. These numbers measure the
gateway/database chain, not Qwen inference throughput.
"""
from concurrent.futures import ThreadPoolExecutor
from collections import Counter
import argparse
import json
import math
import threading
import time
import traceback

from client import API, eventually, ledger_snapshot, stats
import runtime


def quantile(values, fraction):
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered)*fraction)-1)] if ordered else 0


def main(duration, rps, concurrency, account_count):
    if not (10 <= duration <= 86400 and 1 <= rps <= 200 and 1 <= concurrency <= 32 and 1 <= account_count <= 32):
        raise ValueError("duration 10..86400s, RPS 1..200, concurrency/accounts 1..32 required")
    api = API()
    path = runtime.ROOT / "checks/stability.json"
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    report = {"state": "running", "started": time.time(), "duration_s": duration, "target_rps": rps,
              "concurrency": concurrency, "accounts": account_count, "upstream": "deterministic CPU protocol fixture", "samples": [],
              "criteria": {"success_rate": 1, "p95_ms_max": 1000, "p99_ms_max": 2000,
                           "min_achieved_fraction_of_target": 0.95, "max_rss_growth_mib": 64,
                           "max_goroutine_growth": 20, "max_fd_growth": 32, "unsettled_reservations": 0}}
    failure_examples = []
    failure_lock = threading.Lock()
    def save():
        path.write_text(json.dumps(report, ensure_ascii=False, indent=2))

    def resources():
        code, _, raw = api.call("GET", "/metrics", admin=True)
        if code != 200:
            return {"metrics_status": code}
        names = {"go_goroutines", "process_resident_memory_bytes", "process_open_fds", "llmgw_inflight", "process_cpu_seconds_total"}
        result = {"elapsed_s": round(time.monotonic()-began, 3)}
        for line in raw.decode().splitlines():
            if line and not line.startswith("#"):
                parts = line.split()
                if parts[0] in names:
                    result[parts[0]] = float(parts[1])
        return result

    def database_counters():
        raw = runtime.sql("SHOW GLOBAL STATUS WHERE Variable_name IN ('Innodb_row_lock_time','Innodb_row_lock_waits','Innodb_os_log_fsyncs','Innodb_log_waits');")
        return {line.split("\t")[0]: int(line.split("\t")[1]) for line in raw.splitlines()}

    def request(index):
        start = time.monotonic()
        try:
            code, _, raw = api.call("POST", "/v1/chat/completions", api.body(provider["model_alias"]),
                                    key=keys[index % len(keys)], headers={"Idempotency-Key": "load-"+str(index)}, timeout=10)
            usage_ok = code == 200 and json.loads(raw).get("usage", {}).get("total_tokens") == 12
            if not usage_ok:
                with failure_lock:
                    if len(failure_examples) < 4:
                        failure_examples.append({"status": code, "error": json.loads(raw).get("error", {}).get("code", "invalid_usage")})
            return code, (time.monotonic()-start)*1000, usage_ok
        except Exception as exc:
            with failure_lock:
                if len(failure_examples) < 4:
                    failure_examples.append({"status": 0, "error": type(exc).__name__})
            return 0, (time.monotonic()-start)*1000, False

    began = time.monotonic()
    save()
    try:
        accounts, keys = [], []
        for index in range(account_count):
            account, key, _ = api.account("load-"+str(index), quota=1000000000, rpm=1000000)
            accounts.append(account)
            keys.append(key)
        provider = api.provider("load", input_micro_usd_per_million_tokens=0, output_micro_usd_per_million_tokens=0)
        with ThreadPoolExecutor(max_workers=concurrency) as warm_pool:
            warm_results = list(warm_pool.map(request, range(-concurrency, 0)))
        for warm in warm_results:
            if not warm[2]:
                raise AssertionError(f"load warm-up request failed: status={warm[0]}, latency_ms={warm[1]:.1f}")
        before_usage = sum(api.usage(key)["account"]["used_tokens"] for key in keys)
        before_calls = stats()["calls"].get("normal", 0)
        database_before = database_counters()
        began = time.monotonic()
        baseline = resources()
        report["baseline"] = baseline
        report["samples"].append(baseline)
        outputs, pending, skipped = [], set(), 0
        next_sample = 5
        scheduled = duration*rps
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            for index in range(scheduled):
                target = began + index/rps
                remaining = target-time.monotonic()
                if remaining > 0:
                    time.sleep(remaining)
                ready = {future for future in pending if future.done()}
                outputs.extend(future.result() for future in ready)
                pending -= ready
                if len(pending) >= concurrency:
                    skipped += 1
                else:
                    pending.add(pool.submit(request, index))
                elapsed = time.monotonic()-began
                if elapsed >= next_sample:
                    report["samples"].append(resources())
                    report.update(completed=len(outputs), submitted=index+1-skipped, skipped=skipped)
                    save()
                    next_sample += 5
            outputs.extend(future.result() for future in pending)
        elapsed = time.monotonic()-began
        eventually(lambda: all(api.usage(key)["account"]["reserved_tokens"] == 0 for key in keys))
        final = resources()
        report["samples"].append(final)
        accounts_end = [api.usage(key)["account"] for key in keys]
        snapshot = ledger_snapshot([account["id"] for account in accounts])
        database_after = database_counters()
        codes = Counter(code for code, _, _ in outputs)
        success = sum(code == 200 and valid for code, _, valid in outputs)
        latencies = [latency for _, latency, _ in outputs]
        measured_usage = sum(a["used_tokens"] for a in accounts_end)-before_usage
        call_count = stats()["calls"].get("normal", 0)-before_calls
        p95, p99 = quantile(latencies, 0.95), quantile(latencies, 0.99)
        rss_growth = (final["process_resident_memory_bytes"]-baseline["process_resident_memory_bytes"])/1024**2
        goroutine_growth = final["go_goroutines"]-baseline["go_goroutines"]
        fd_growth = final["process_open_fds"]-baseline["process_open_fds"]
        checks = {"all_requests_successful": success == scheduled and skipped == 0,
                  "p95_under_1000ms": p95 <= 1000, "p99_under_2000ms": p99 <= 2000,
                  "target_rate_achieved": success/elapsed >= 0.95*rps,
                  "actual_usage_exact": measured_usage == success*12,
                  "upstream_calls_exact": call_count == len(outputs),
                  "no_reservation_leak": all(a["reserved_tokens"] == 0 for a in accounts_end),
                  "bounded_rss_growth": rss_growth <= 64,
                  "bounded_goroutine_growth": goroutine_growth <= 20,
                  "bounded_fd_growth": fd_growth <= 32,
                  "no_inflight_leak": final["llmgw_inflight"] == 0}
        report.update(state="passed" if all(checks.values()) else "failed", checks=checks,
                      finished=time.time(), measured_duration_s=round(elapsed, 3), scheduled=scheduled,
                      completed=len(outputs), submitted=len(outputs), successful=success, skipped=skipped, statuses=dict(codes),
                      achieved_rps=round(success/elapsed, 3), p50_ms=round(quantile(latencies, 0.5), 3),
                      p95_ms=round(p95, 3), p99_ms=round(p99, 3), max_ms=round(max(latencies), 3),
                      rss_growth_mib=round(rss_growth, 3), goroutine_growth=goroutine_growth, fd_growth=fd_growth,
                      database_counter_delta={name: database_after[name]-value for name, value in database_before.items()},
                      settled_tokens=measured_usage, expected_tokens=success*12, ledger=snapshot)
    except Exception as exc:
        report.update(state="failed", error=f"{type(exc).__name__}: {exc}")
        traceback.print_exc()
    finally:
        try:
            api.cleanup()
            runtime.stop(["fixture"])
            report["test_routes_and_keys_disabled"] = True
        except Exception as exc:
            report.update(state="failed", cleanup_error=str(exc))
        report["failure_examples"] = failure_examples
        save()
        (runtime.ROOT / "checks" / f"stability-{stamp}.json").write_text(path.read_text())
    print(json.dumps({k: v for k, v in report.items() if k != "samples"}, ensure_ascii=False, indent=2))
    return 0 if report["state"] == "passed" else 1


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=int, default=60)
    parser.add_argument("--rps", type=int, default=10)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--accounts", type=int, default=4)
    args = parser.parse_args()
    raise SystemExit(main(args.duration, args.rps, args.concurrency, args.accounts))
