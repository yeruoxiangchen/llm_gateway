"""Align slow transaction windows with storage pressure and pool/cgroup waits."""
import argparse
from datetime import datetime, timezone
import heapq
import json
from pathlib import Path

from lab_tools import stage_delta
import runtime


def jsonl(path):
    with Path(path).open() as source:
        for line in source:
            try:yield json.loads(line)
            except ValueError:continue  # A live writer can have an incomplete final line.


def analyze_case(case_dir):
    case_dir=Path(case_dir)
    previous=None;worst=[];sequence=0
    for value in jsonl(case_dir/"gateway-samples.jsonl"):
        if previous:
            stages=stage_delta(previous,value)
            commits={key:stats for key,stats in stages.items() if key in ("reserve_commit","admit_commit","dispatch_commit","settle_commit")}
            if commits:
                score=max(stats["mean_ms"] for stats in commits.values())
                pool=value.get("db_pool",{});old_pool=previous.get("db_pool",{})
                metadata=value.get("metadata_db_pool",{});old_metadata=previous.get("metadata_db_pool",{})
                window={"from":previous["at"],"until":value["at"],
                        "until_utc":datetime.fromtimestamp(value["at"],timezone.utc).isoformat(),
                        "commit_stages":commits,"sampled_inflight":value["resources"].get("llmgw_inflight"),
                        "db_pool":{"in_use":pool.get("in_use_connections"),"maximum":pool.get("max_open_connections"),
                                   "wait_count_delta":pool.get("wait_count_total",0)-old_pool.get("wait_count_total",0),
                                   "wait_seconds_delta":pool.get("wait_duration_seconds_total",0)-old_pool.get("wait_duration_seconds_total",0)},
                        "metadata_db_pool":{"in_use":metadata.get("in_use_connections"),"maximum":metadata.get("max_open_connections"),
                                   "wait_count_delta":metadata.get("wait_count_total",0)-old_metadata.get("wait_count_total",0),
                                   "wait_seconds_delta":metadata.get("wait_duration_seconds_total",0)-old_metadata.get("wait_duration_seconds_total",0)},
                        "host_samples":0,"io_full_peak_pct":0,"disks":{},"cgroup_throttled_seconds":{},
                        "database_query_peak_ms":0,"database_sample_errors":0,"database_pending_peak":{},
                        "process_write_mib":{},"mysql_blocked_threads":[]}
                sequence+=1
                heapq.heappush(worst,(score,sequence,window))
                if len(worst)>5:heapq.heappop(worst)
        previous=value
    windows=[item[2] for item in sorted(worst,reverse=True)]
    previous=None
    host_count=0;host_errors=0;pool_throttle_available=set()
    for value in jsonl(case_dir/"host-samples.jsonl"):
        host_count+=1
        if "error" in value:
            host_errors+=1;continue
        for name,stats in value.get("cgroups",{}).items():
            if "throttled_usec" in stats.get("cpu.stat",{}):pool_throttle_available.add(name)
        for window in windows:
            if not window["from"]<=value["at"]<=window["until"]:continue
            window["host_samples"]+=1
            window["io_full_peak_pct"]=max(window["io_full_peak_pct"],value.get("io_full_pct",0))
            for device,rates in value.get("disk_rates",{}).items():
                peaks=window["disks"].setdefault(device,{})
                for key,number in rates.items():
                    if number is not None:peaks[key]=max(peaks.get(key,0),number)
            database=value.get("database",{})
            window["database_query_peak_ms"]=max(window["database_query_peak_ms"],database.get("query_ms",0))
            window["database_sample_errors"]+=int("error" in database)
            for key,number in database.get("counters",{}).items():
                if "pending" in key.lower() or key=="Threads_running":
                    window["database_pending_peak"][key]=max(window["database_pending_peak"].get(key,0),number)
            for blocked in value.get("processes",{}).get(runtime.UNITS["mysql"],{}).get("blocked_threads",[]):
                if blocked not in window["mysql_blocked_threads"]:window["mysql_blocked_threads"].append(blocked)
            if not previous:continue
            for name,stats in value.get("cgroups",{}).items():
                old=previous.get("cgroups",{}).get(name,{}).get("cpu.stat",{})
                current=stats.get("cpu.stat",{})
                if "throttled_usec" in current and "throttled_usec" in old:
                    window["cgroup_throttled_seconds"][name]=window["cgroup_throttled_seconds"].get(name,0)+max(0,current["throttled_usec"]-old["throttled_usec"])/1000000
            for name,stats in value.get("processes",{}).items():
                old=previous.get("processes",{}).get(name,{})
                if stats.get("start_ticks") is None or stats.get("start_ticks")!=old.get("start_ticks"):continue
                delta=max(0,stats.get("io",{}).get("write_bytes",0)-old.get("io",{}).get("write_bytes",0))
                window["process_write_mib"][name]=window["process_write_mib"].get(name,0)+delta/1024**2
        previous=value
    return {"case_dir":str(case_dir),"host_samples":host_count,"host_errors":host_errors,
            "cpu_throttle_counters_available":sorted(pool_throttle_available),"worst_commit_windows":windows,
            "interpretation":"Commit stages are window means; host metrics are samples/peaks. Shared-device I/O cannot identify other writers. Missing CPU throttle counters mean unavailable, not zero."}


def main(path):
    report_path=Path(path) if path else runtime.ROOT/"checks/soak.json"
    report=json.loads(report_path.read_text())
    folder=Path(report["run_dir"])
    cases=[folder/case["name"] for case in report.get("cases",[])]
    if report.get("state")=="running" and report.get("stage"):
        current=folder/report["stage"]
        if current not in cases:cases.append(current)
    output={"source_report":str(report_path),"cases":[analyze_case(case) for case in cases
            if (case/"host-samples.jsonl").exists() and (case/"gateway-samples.jsonl").exists()]}
    print(json.dumps(output,ensure_ascii=False,indent=2))


if __name__=="__main__":
    parser=argparse.ArgumentParser();parser.add_argument("--report")
    main(parser.parse_args().report)
