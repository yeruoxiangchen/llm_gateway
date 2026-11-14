"""Low-volume Linux diagnostics for this project's load experiments.

Counters are sampled read-only. JSONL is appended, without fsync; financial
receipts retain their separate durability policy. No command lines or keys are
collected. Disk counters describe the shared device, not a particular writer.
"""
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import subprocess
import threading
import time

import runtime


DB_COUNTERS = (
    "Innodb_data_fsyncs", "Innodb_data_pending_fsyncs", "Innodb_data_pending_reads",
    "Innodb_data_pending_writes", "Innodb_os_log_fsyncs", "Innodb_os_log_pending_fsyncs",
    "Innodb_log_waits", "Innodb_row_lock_time", "Innodb_row_lock_waits",
    "Threads_connected", "Threads_running", "Innodb_deadlocks",
)


def key_values(text):
    return {key.rstrip(":"): int(value) for key, value in
            (line.split()[:2] for line in text.splitlines() if len(line.split()) >= 2)}


def pressure(text):
    return {parts[0]: {key: (int(value) if key == "total" else float(value))
                      for key, value in (item.split("=", 1) for item in parts[1:])}
            for parts in (line.split() for line in text.splitlines()) if parts}


def process_stat(text):
    # comm can contain whitespace and ')' characters; fields start after its
    # final closing parenthesis. start_ticks guards against PID reuse.
    fields = text.rsplit(")", 1)[1].split()
    return {"state": fields[0], "cpu_ticks": int(fields[11]) + int(fields[12]),
            "start_ticks": int(fields[19]), "rss_pages": int(fields[21]),
            "block_io_ticks": int(fields[39]) if len(fields) > 39 else None}


def disk_stats(text, devices):
    result = {}
    for line in text.splitlines():
        fields = line.split()
        if len(fields) < 14 or fields[2] not in devices:
            continue
        values = [int(value) for value in fields[3:]]
        result[fields[2]] = dict(zip(("reads", "read_merges", "read_sectors", "read_ms",
            "writes", "write_merges", "write_sectors", "write_ms", "inflight",
            "busy_ms", "weighted_ms", "discards", "discard_merges", "discard_sectors",
            "discard_ms", "flushes", "flush_ms"), values))
    return result


def disk_delta(before, after, seconds):
    diff = {key: max(0, value - before.get(key, value)) for key, value in after.items()
            if key != "inflight"}
    result = {"inflight": after["inflight"], "io_time_ms_per_s": diff["busy_ms"] / seconds,
              "queue_time_ms_per_s": diff["weighted_ms"] / seconds,
              "read_mib_per_s": diff["read_sectors"] / seconds / 2048,
              "write_mib_per_s": diff["write_sectors"] / seconds / 2048}
    for kind in ("read", "write", "flush"):
        count = diff.get(kind + ("es" if kind == "flush" else "s"), 0)
        result[kind + "_await_ms"] = diff.get(kind + "_ms", 0) / count if count else None
    return result


def storage_devices(path):
    device = os.stat(path).st_dev
    link = Path(f"/sys/dev/block/{os.major(device)}:{os.minor(device)}").resolve()
    result = set()

    def visit(node):
        if node.name in result:
            return
        result.add(node.name)
        if (node / "partition").exists():
            visit(node.parent)
        if (node / "slaves").exists():
            for slave in (node / "slaves").iterdir():
                visit(slave.resolve())
    visit(link)
    return sorted(result)


def targets(k6_pid):
    names = ["mysql", "redis", "lab-a"]
    response = runtime.ctl("show", *(runtime.UNITS[name] for name in names),
                           "-p", "Id", "-p", "MainPID", "-p", "ControlGroup")
    if response.returncode:
        raise RuntimeError("Could not read gateway service identities")
    result = {}
    for block in response.stdout.strip().split("\n\n"):
        fields = dict(line.split("=", 1) for line in block.splitlines() if "=" in line)
        pid = int(fields.get("MainPID", "0"))
        if pid:
            result[fields["Id"]] = {"pid": pid, "cgroup": fields["ControlGroup"]}
    own_group = next((line.split(":", 2)[2] for line in
                      Path("/proc/self/cgroup").read_text().splitlines() if line.startswith("0::")), "")
    result["load-generator"] = {"pid": k6_pid, "cgroup": own_group}
    for identity in result.values():
        try:identity["start_ticks"] = process_stat(Path("/proc", str(identity["pid"]), "stat").read_text())["start_ticks"]
        except OSError:identity["start_ticks"] = None
    return result


def db_sample():
    began = time.monotonic()
    try:
        query = "SHOW GLOBAL STATUS WHERE Variable_name IN (" + ",".join("'" + name + "'" for name in DB_COUNTERS) + ");"
        rows = runtime.sql(query, timeout=2)
        result = {"counters": {name: int(value) for name, value in
                               (line.split("\t") for line in rows.splitlines())}}
    except subprocess.TimeoutExpired:
        result = {"error": "database sampling timed out after 2s"}
    except Exception as exc:
        result = {"error": type(exc).__name__}
    return {**result, "at": time.time(), "query_ms": (time.monotonic() - began) * 1000}


class HostMonitor:
    def __init__(self, path, k6_pid, interval=1):
        self.path = Path(path)
        self.interval = interval
        self.identities = targets(k6_pid)
        self.devices = storage_devices(runtime.ROOT / "db")
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="llmgw-host-monitor", daemon=True)
        self.summary = {"path": str(self.path), "interval_s": interval, "devices": self.devices,
                        "targets": self.identities, "samples": 0, "errors": 0, "database_samples": 0,
                        "database_errors": 0, "max_io_full_pct": 0, "max_disk_await_ms": 0,
                        "cgroup_throttled_seconds": {}}

    def __enter__(self):
        self._thread.start()
        return self

    def __exit__(self, *_):
        self._stop.set()
        self._thread.join(timeout=6)
        self.summary["stopped"] = not self._thread.is_alive()

    def sample(self):
        value = {"at": time.time(), "monotonic_s": time.monotonic(), "processes": {}, "cgroups": {},
                 "pressure": {name: pressure(Path("/proc/pressure", name).read_text())
                              for name in ("io", "cpu", "memory")},
                 "cpu_ticks": [int(item) for item in Path("/proc/stat").read_text().splitlines()[0].split()[1:9]],
                 "disks": disk_stats(Path("/proc/diskstats").read_text(), self.devices)}
        for name, identity in self.identities.items():
            proc = Path("/proc", str(identity["pid"]))
            try:
                fields = process_stat((proc / "stat").read_text())
                if fields["start_ticks"] != identity["start_ticks"]:
                    raise ValueError("target PID identity changed")
                value["processes"][name] = {**fields, "io": key_values((proc / "io").read_text()), "pid": identity["pid"]}
                if name == runtime.UNITS["mysql"] and value["pressure"]["io"].get("some", {}).get("avg10", 0) >= 1:
                    blocked = []
                    for task in list((proc / "task").iterdir())[:128]:
                        try:
                            if process_stat((task / "stat").read_text())["state"] == "D":
                                blocked.append({"tid": int(task.name), "wait_channel": (task / "wchan").read_text().strip()})
                        except OSError:continue
                    value["processes"][name]["blocked_threads"] = blocked
            except (OSError, ValueError, IndexError) as exc:
                value["processes"][name] = {"error": type(exc).__name__}
            group = Path("/sys/fs/cgroup") / identity["cgroup"].lstrip("/")
            if not identity["cgroup"]:
                continue
            stats = {}
            for filename in ("cpu.stat", "memory.events", "memory.current"):
                try:
                    text = (group / filename).read_text()
                    stats[filename] = int(text) if filename.endswith("current") else key_values(text)
                except OSError as exc:
                    stats[filename] = {"error": type(exc).__name__}
            for resource in ("cpu", "io", "memory"):
                try:
                    stats[resource + ".pressure"] = pressure((group / (resource + ".pressure")).read_text())
                except OSError as exc:
                    stats[resource + ".pressure"] = {"error": type(exc).__name__}
            try:
                stats["io.stat"] = {line.split()[0]: dict(item.split("=", 1) for item in line.split()[1:])
                                    for line in (group / "io.stat").read_text().splitlines()}
            except OSError as exc:
                stats["io.stat"] = {"error": type(exc).__name__}
            value["cgroups"][name] = stats
        return value

    def _run(self):
        previous = None
        future = None
        next_database = 0
        try:
            self.path.touch(mode=0o600)
            with self.path.open("w") as output, ThreadPoolExecutor(max_workers=1) as pool:
                while not self._stop.is_set():
                    began = time.monotonic()
                    try:
                        value = self.sample()
                        if future is not None and future.done():
                            value["database"] = future.result()
                            self.summary["database_samples"] += 1
                            self.summary["database_errors"] += int("error" in value["database"])
                            future = None
                        if future is None and began >= next_database:
                            future = pool.submit(db_sample)
                            next_database = began + 5
                        if previous:
                            seconds = value["monotonic_s"] - previous["monotonic_s"]
                            value["io_full_pct"] = max(0, value["pressure"]["io"].get("full", {}).get("total", 0) -
                                previous["pressure"]["io"].get("full", {}).get("total", 0)) / seconds / 10000
                            self.summary["max_io_full_pct"] = max(self.summary["max_io_full_pct"], value["io_full_pct"])
                            value["disk_rates"] = {device: disk_delta(previous["disks"][device], stats, seconds)
                                                   for device, stats in value["disks"].items() if device in previous["disks"]}
                            for rates in value["disk_rates"].values():
                                self.summary["max_disk_await_ms"] = max(self.summary["max_disk_await_ms"],
                                    *(rates.get(kind + "_await_ms") or 0 for kind in ("read", "write", "flush")))
                            for name, stats in value["cgroups"].items():
                                old = previous["cgroups"].get(name, {}).get("cpu.stat", {})
                                current = stats["cpu.stat"]
                                if "throttled_usec" in current and "throttled_usec" in old:
                                    total = max(0, current["throttled_usec"] - old["throttled_usec"]) / 1000000
                                    totals = self.summary["cgroup_throttled_seconds"]
                                    totals[name] = totals.get(name, 0) + total
                        output.write(json.dumps(value, separators=(",", ":")) + "\n")
                        output.flush()
                        previous = value
                        self.summary["samples"] += 1
                    except Exception as exc:
                        self.summary["errors"] += 1
                        output.write(json.dumps({"at": time.time(), "error": type(exc).__name__}) + "\n")
                        output.flush()
                    self._stop.wait(max(0, self.interval - (time.monotonic() - began)))
        except Exception as exc:
            self.summary["error"] = type(exc).__name__
