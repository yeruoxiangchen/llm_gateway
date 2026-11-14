"""Independent k6 arrival-rate experiments, stage profiles and ledger audit."""
from concurrent.futures import ThreadPoolExecutor
import argparse
import json
import os
from pathlib import Path
import subprocess
import time
import traceback

from client import API, eventually, ledger_snapshot, stats
from lab_tools import metric_snapshot, stage_delta, database_counters, capture
from host_monitor import HostMonitor
from analyze_latency import analyze_case
import runtime


def atomic_json(path,value):
    temp=path.with_suffix(path.suffix+".tmp")
    temp.write_text(json.dumps(value,ensure_ascii=False,indent=2));temp.chmod(0o600);temp.replace(path)


def slow_commit_events(path, offset, target):
    events=[]
    with path.open() as source, target.open("w") as output:
        source.seek(offset)
        for line in source:
            try:event=json.loads(line)
            except ValueError:continue
            if event.get("msg")=="slow database commit":
                events.append(event);output.write(json.dumps(event)+"\n")
    return {"path":str(target),"logged_events":len(events),"rate_limit":"one event per commit stage per second",
            "max_logged_ms":max((event["elapsed_ms"] for event in events),default=0)}


def await_settlement(account_ids, timeout=30):
    """Keep recovery time separate from k6 latency; never resolve unknown usage."""
    ids=",".join("'"+value+"'" for value in account_ids)
    began=time.monotonic()
    pending=None
    receipts=0
    query_errors=0
    while True:
        remaining=timeout-(time.monotonic()-began)
        if remaining<=0:
            break
        try:
            pending=int(runtime.sql("USE llm_gateway_lab; SELECT COUNT(*) FROM requests FORCE INDEX(requests_recovery) "
                                    "WHERE state IN ('reserved','dispatched','pending_usage') AND account_id IN ("+ids+");",
                                    timeout=min(3,remaining)))
        except (subprocess.TimeoutExpired,RuntimeError):
            pending=None
            query_errors+=1
        receipts=0
        for path in (runtime.ROOT/"lab/a/spool").glob("*.json"):
            try:value=json.loads(path.read_text())
            except (OSError,ValueError):continue
            receipts+=int(value.get("account_id") in account_ids)
        elapsed=time.monotonic()-began
        if (pending==0 and receipts==0) or elapsed>=timeout:
            break
        time.sleep(min(.5,max(0,timeout-elapsed)))
    return {"elapsed_s":round(time.monotonic()-began,3),"budget_s":timeout,"unresolved_requests":pending,
            "receipt_files":receipts,"query_errors":query_errors}


def main(mode,duration,rps,account_count,vus,admission_mode="combined",comparison_axis="admission"):
    if not (10<=duration<=86400 and 1<=rps<=100 and 1<=account_count<=100 and 1<=vus<=64):
        raise ValueError("duration 10..86400, RPS 1..100, accounts 1..100, VUs 1..64")
    api=API(runtime.LAB_A)
    run_dir=runtime.ROOT/"checks"/mode/time.strftime("%Y%m%dT%H%M%SZ",time.gmtime())
    run_dir.mkdir(parents=True,exist_ok=True);run_dir.chmod(0o700)
    report_path=runtime.ROOT/"checks"/({"perf":"performance.json","soak":"soak.json","compare":"compare.json"}[mode])
    report={"state":"running","started":time.time(),"namespace":api.prefix,"run_dir":str(run_dir),"generator":"official k6 constant-arrival-rate",
            "scope":"protocol fixture, isolated llm_gateway_lab database, one gateway with 32 inflight / 16 DB connections","admission_mode":admission_mode,"cases":[]}
    process=None
    def save():atomic_json(run_dir/"report.json",report);atomic_json(report_path,report)
    def metric(data,name,value,default=0):return data.get("metrics",{}).get(name,{}).get("values",{}).get(value,default)
    save()
    try:
        k6=runtime.ROOT/"toolchain/k6/k6"
        if not k6.exists():raise RuntimeError("Install the official tool with bash run.sh install-k6")
        report["k6"]=json.loads((runtime.ROOT/"config/k6-receipt.json").read_text())
        report["database_settings"]=runtime.sql("SELECT VERSION(),@@innodb_flush_method,@@innodb_flush_log_at_trx_commit,@@innodb_buffer_pool_size,@@innodb_log_file_size;")
        (run_dir/"k6-config.json").write_text("{}")
        scenarios=[{"name":"baseline-10rps-4accounts","rps":10,"accounts":4,"vus":16},
                   {"name":"20rps-10accounts-16vus","rps":20,"accounts":10,"vus":16},
                   {"name":"20rps-10accounts-32vus","rps":20,"accounts":10,"vus":32},
                   {"name":"20rps-1account-32vus","rps":20,"accounts":1,"vus":32},
                   {"name":"profile-20rps-10accounts","rps":20,"accounts":10,"vus":32,"profile":True}] if mode=="perf" else [
                   {"name":"soak","rps":rps,"accounts":account_count,"vus":vus}]
        if mode=="compare":
            report["comparison_axis"]=comparison_axis
            if comparison_axis=="request-id":
                scenarios=[{"name":name,"request_id_version":version,"admission_mode":admission_mode,"rps":rps,"accounts":account_count,"vus":vus}
                           for name,version in (("A1-uuid4",4),("B1-uuid7",7),("B2-uuid7",7),("A2-uuid4",4))]
                report["comparison_protocol"]="same binary, UUID4/UUID7/UUID7/UUID4; fresh gateway each case; identical load, admission and durability; shared-host load recorded"
            else:
                report["admission_mode"]="ABBA: split/combined/combined/split"
                scenarios=[{"name":name,"admission_mode":variant,"rps":rps,"accounts":account_count,"vus":vus}
                           for name,variant in (("A1-split","split"),("B1-combined","combined"),("B2-combined","combined"),("A2-split","split"))]
                report["comparison_protocol"]="same binary, split/combined/combined/split; fresh gateway each case; identical load and durability; shared-host load recorded"
        for index,scenario in enumerate(scenarios):
            case_dir=run_dir/scenario["name"];case_dir.mkdir();case_dir.chmod(0o700)
            report.update(stage=scenario["name"],current={**scenario,"duration_s":duration},completed=0);save()
            selected_mode=scenario.get("admission_mode",admission_mode)
            if mode=="compare":
                runtime.stop(["lab-a"])
                config_path=runtime.ROOT/"config/lab-a.env"
                config=runtime.read_env(config_path);config["LLMGW_ADMISSION_MODE"]=selected_mode
                if "request_id_version" in scenario:config["LLMGW_REQUEST_ID_VERSION"]=scenario["request_id_version"]
                runtime.save_env(config_path,config)
                release=runtime.immutable_binary("gateway")
                scenario["gateway_binary"]=release.name
                runtime.launch("lab-a",[release],"768M","200%",config_path)
                runtime.wait_http(runtime.LAB_A+"/health/ready")
            clients=[]
            for number in range(scenario["accounts"]):
                account,key,_=api.account(f"load-{index}-{number}",quota=1000000000,rpm=1000000)
                clients.append({"account_id":account["id"],"key":key})
            provider=api.provider("perf-"+str(index),input_micro_usd_per_million_tokens=0,output_micro_usd_per_million_tokens=0)
            inputs=case_dir/"clients.json";atomic_json(inputs,{"clients":clients})
            def warm(number):
                value=api.chat(clients[number%len(clients)]["key"],provider["model_alias"])
                if value[0]!=200:raise AssertionError("warmup failed: "+str(value[0]))
            with ThreadPoolExecutor(max_workers=16) as pool:list(pool.map(warm,range(16)))
            before_used=sum(api.usage(client["key"])["account"]["used_tokens"] for client in clients)
            before_calls=stats()["calls"].get("normal",0)
            database_before=database_counters()
            before=metric_snapshot(api)
            if before.get("admission_mode")!=selected_mode:raise AssertionError("Gateway admission mode does not match scenario")
            if before.get("request_id_version") not in (4,7):raise AssertionError("Gateway request ID strategy is missing")
            if "request_id_version" in scenario and before["request_id_version"]!=scenario["request_id_version"]:raise AssertionError("Gateway request ID strategy does not match scenario")
            report["request_id_version"]=before["request_id_version"]
            case={**scenario,"admission_mode":selected_mode,"duration_s":duration,"state":"running","started":time.time(),"samples":[before]}
            case["request_id_version"]=before["request_id_version"]
            config=runtime.read_env(runtime.ROOT/"config/lab-a.env")
            case["request_budgets"]={"database_read_ms":int(config.get("LLMGW_DATABASE_READ_TIMEOUT_MS",1000)),
                                     "admission_ms":int(config.get("LLMGW_ADMISSION_TIMEOUT_MS",5000)),
                                     "rate_limit_ms":int(config.get("LLMGW_RATE_LIMIT_TIMEOUT_MS",3000)),
                                     "overall_seconds":int(config["LLMGW_REQUEST_TIMEOUT_SECONDS"])}
            case["database_pool_limits"]={"total":int(config.get("LLMGW_DB_CONNECTIONS",32)),
                                           "metadata":int(config.get("LLMGW_METADATA_DB_CONNECTIONS",0))}
            pid=runtime.ctl("show",runtime.UNITS["lab-a"],"-p","MainPID","--value").stdout.strip()
            case["gateway_executable"]=str((Path("/proc")/pid/"exe").resolve(strict=True))
            lab_log=runtime.ROOT/"logs/lab-a.log"
            log_offset=lab_log.stat().st_size
            summary_path=case_dir/"k6-summary.json"
            env={name:os.environ[name] for name in ("PATH","TMPDIR","NO_PROXY","no_proxy","LANG","HOME") if name in os.environ}
            env.update(GOMAXPROCS="2",GOMEMLIMIT="512MiB",XDG_CACHE_HOME=str(runtime.ROOT/"cache/k6"),
                       K6_NO_USAGE_REPORT="true",LLMGW_CLIENTS=str(inputs),LLMGW_TARGET=runtime.LAB_A,
                       LLMGW_ALIAS=provider["model_alias"],LLMGW_CASE=api.prefix+"-"+str(index),
                       LLMGW_RATE=str(scenario["rps"]),LLMGW_DURATION=str(duration),LLMGW_VUS=str(scenario["vus"]),LLMGW_SUMMARY=str(summary_path))
            began=time.monotonic();next_sample=5;profile_future=None
            with (case_dir/"k6.log").open("w") as output, (case_dir/"gateway-samples.jsonl").open("w") as samples, ThreadPoolExecutor(max_workers=1) as profiler:
                process=subprocess.Popen([str(k6),"run","--quiet","--no-usage-report","--config",str(run_dir/"k6-config.json"),str(runtime.SOURCE/"scripts/load_k6.js")],
                                         env=env,stdout=output,stderr=subprocess.STDOUT,cwd=runtime.SOURCE)
                with HostMonitor(case_dir/"host-samples.jsonl",process.pid) as monitor:
                    samples.write(json.dumps(before)+"\n");samples.flush()
                    while process.poll() is None:
                        elapsed=time.monotonic()-began
                        if elapsed>=next_sample:
                            value=metric_snapshot(api);value["elapsed_s"]=round(elapsed,3);case["samples"].append(value)
                            completed=sum(value["requests"].values())-sum(before["requests"].values())
                            report.update(completed=int(completed),elapsed_s=round(time.time()-report["started"],3),
                                          current_sample=value["resources"],current_db_pool=value["db_pool"],
                                          current_metadata_db_pool=value.get("metadata_db_pool",{}),current_rate_limit=value.get("rate_limit",{}),
                                          current_protection_rejections=value.get("protection_rejections",{}),
                                          host_monitor=dict(monitor.summary))
                            samples.write(json.dumps(value)+"\n");samples.flush();save();next_sample+=5
                        if scenario.get("profile") and profile_future is None and elapsed>=3:
                            profile_future=profiler.submit(capture,api,"profile",case_dir/"cpu.pprof",5)
                        time.sleep(.2)
                case["host_monitor"]=monitor.summary
                exit_code=process.returncode;process=None
                if profile_future:case["cpu_profile"]=profile_future.result()
            if exit_code not in (0,99):raise AssertionError(f"k6 execution failed with {exit_code}; see {case_dir/'k6.log'}")
            if not summary_path.exists():raise AssertionError("k6 summary missing")
            data=json.loads(summary_path.read_text())
            eventually(lambda:metric_snapshot(api)["resources"]["llmgw_inflight"]==0,timeout=15)
            report["stage"]="settlement-recovery";save()
            case["settlement_recovery"]=await_settlement([client["account_id"] for client in clients])
            after=metric_snapshot(api)
            case["samples"].append(after)
            with (case_dir/"gateway-samples.jsonl").open("a") as samples:samples.write(json.dumps(after)+"\n")
            database_after=database_counters()
            database_delta={name:database_after[name]-value for name,value in database_before.items()}
            snapshot=ledger_snapshot([client["account_id"] for client in clients],"llm_gateway_lab",with_requests=True,validate=False)
            settled=sum(account["used"] for account in snapshot)-before_used
            count=int(metric(data,"http_reqs","count"));accepted=int(metric(data,"accepted_responses","count"));dropped=int(metric(data,"dropped_iterations","count"))
            actual_calls=stats()["calls"].get("normal",0)-before_calls
            # Provider usage and client delivery are separate facts: a network
            # failure can happen after a valid provider receipt was committed.
            receipt_sum=sum(account["request_tokens"] for account in snapshot)
            check_ledger=all(account["used"]==account["ledger_used"]==account["request_tokens"] and account["reserved"]==account["ledger_reserved"] and account["reserved"]>=0 for account in snapshot)
            no_unresolved=all(account["reserved"]==0 and account["unresolved_requests"]==0 for account in snapshot) and case["settlement_recovery"]["receipt_files"]==0
            stages=stage_delta(before,after)
            rejections={name:int(value-before.get("protection_rejections",{}).get(name,0))
                        for name,value in after.get("protection_rejections",{}).items()}
            rate_before=before.get("rate_limit",{});rate_after=after.get("rate_limit",{})
            rate_delta={name:int(rate_after.get(name,0)-rate_before.get(name,0)) for name in ("retries","cache_hits")}
            rate_delta["attempts"]={name:int(value-rate_before.get("attempts",{}).get(name,0)) for name,value in rate_after.get("attempts",{}).items()}
            limits=case["database_pool_limits"]
            pool_limits_match=(after["db_pool"].get("max_open_connections")==limits["total"]-limits["metadata"] and
                               after.get("metadata_db_pool",{}).get("max_open_connections",0)==limits["metadata"])
            request_commits=sum(value["count"] for phase,value in stages.items() if phase in ("reserve_commit","admit_commit","dispatch_commit","settle_commit"))
            expected_commits_per_call=2 if selected_mode=="combined" else 3
            checks={"ledger_exact":check_ledger,"no_duplicate_upstream":actual_calls<=count,
                    "no_unresolved_reservations":no_unresolved,
                    "all_started_requests_completed":accepted==count,
                    "no_dropped_iterations":dropped==0,
                    "p95_under_1000ms":metric(data,"http_req_duration","p(95)")<1000,
                    "p99_under_2000ms":metric(data,"http_req_duration","p(99)")<2000,
                    "host_sampling_completed":monitor.summary["samples"]>=max(1,int(duration*.9)) and monitor.summary["errors"]==0 and monitor.summary.get("stopped",False),
                    "database_sampling_available":monitor.summary["database_samples"]>monitor.summary["database_errors"],
                    "db_pool_metrics_available":bool(after["db_pool"]),
                    "database_pool_limits_match":pool_limits_match,
                    "rate_retries_bounded":sum(rate_delta["attempts"].values())<=2*stages.get("rate_limit",{}).get("count",0),
                    "rate_cache_hits_match_retries":rate_delta["cache_hits"]<=rate_delta["retries"],
                    "no_database_deadlocks":database_delta.get("Innodb_deadlocks",0)==0,
                    "request_commit_count_exact":request_commits==expected_commits_per_call*count,
                    "arrival_rate_matches_plan":abs(count+dropped-scenario["rps"]*duration)<=1,
                    "inflight_released":after["resources"]["llmgw_inflight"]==0,
                    "rss_growth_under_64mib":after["resources"]["process_resident_memory_bytes"]-before["resources"]["process_resident_memory_bytes"]<=64*1024**2,
                    "goroutine_growth_under_20":after["resources"]["go_goroutines"]-before["resources"]["go_goroutines"]<=20}
            case.update(state="validation_failed" if not check_ledger or not no_unresolved else "passed" if exit_code==0 and all(checks.values()) else "slo_failed",k6_exit=exit_code,checks=checks,
                        http_requests=count,successful=accepted,dropped_iterations=dropped,upstream_calls=actual_calls,settled_tokens=settled,delivered_tokens=accepted*12,
                        durable_receipt_tokens_including_warmup=receipt_sum,
                        achieved_rps=round(count/duration,3),p50_ms=metric(data,"http_req_duration","p(50)"),p95_ms=metric(data,"http_req_duration","p(95)"),p99_ms=metric(data,"http_req_duration","p(99)"),
                        client_blocked_mean_ms=metric(data,"http_req_blocked","avg"),client_connection_mean_ms=metric(data,"http_req_connecting","avg"),
                        stages=stages,ledger=snapshot,database_counter_delta=database_delta,protection_rejections=rejections,rate_limit=rate_delta,
                        request_commit_count=request_commits,request_commits_per_call=round(request_commits/count,3) if count else None,
                        request_commits_per_upstream_call=round(request_commits/actual_calls,3) if actual_calls else None,
                        mean_ms=metric(data,"http_req_duration","avg"))
            case["physical_reads_per_upstream_call"]=database_delta.get("Innodb_buffer_pool_reads",0)/actual_calls if actual_calls else None
            case["slow_commits"]=slow_commit_events(lab_log,log_offset,case_dir/"slow-commits.jsonl")
            case["latency_diagnostics"]=analyze_case(case_dir)
            atomic_json(case_dir/"latency-diagnostics.json",case["latency_diagnostics"])
            if scenario.get("profile"):
                case["heap_profile"]=capture(api,"heap",case_dir/"heap.pprof")
                case["goroutine_profile"]=capture(api,"goroutine",case_dir/"goroutine.pprof")
                command=[str(runtime.ROOT/"toolchain/go/bin/go"),"tool","pprof","-top","-nodecount=15",str(case_dir/"cpu.pprof")]
                with (case_dir/"cpu-top.txt").open("w") as output:subprocess.run(command,stdout=output,stderr=subprocess.STDOUT,env={**os.environ,"GOTOOLCHAIN":"local"},check=True)
            atomic_json(case_dir/"measurements.json",case)
            report["cases"].append(case);report["cases_completed"]=len(report["cases"]);save()
            if not check_ledger:raise AssertionError("account/ledger/request usage mismatch; details saved in measurements.json")
            if not no_unresolved:raise AssertionError("unresolved reservations or receipts remain after recovery budget; details saved in measurements.json")
            print(scenario["name"],case["state"],"requests",count,"dropped",dropped,"p95_ms",round(case["p95_ms"],2),flush=True)
            api.json("PATCH","/admin/providers/"+provider["id"],{"enabled":False},admin=True)
        if mode=="compare":
            report["variant_summary"]={}
            for variant in (("uuid4","uuid7") if comparison_axis=="request-id" else ("split","combined")):
                cases=[case for case in report["cases"] if (case["request_id_version"]==int(variant[-1]) if comparison_axis=="request-id" else case["admission_mode"]==variant)]
                count=sum(case["http_requests"] for case in cases)
                calls=sum(case["upstream_calls"] for case in cases)
                reads=sum(case["database_counter_delta"].get("Innodb_buffer_pool_reads",0) for case in cases)
                report["variant_summary"][variant]={"http_requests":count,"successful":sum(case["successful"] for case in cases),
                    "dropped_iterations":sum(case["dropped_iterations"] for case in cases),
                    "mean_ms":sum(case["mean_ms"]*case["http_requests"] for case in cases)/count if count else None,
                    "request_commits_per_call":sum(case["request_commit_count"] for case in cases)/count if count else None,
                    "case_p95_ms":[case["p95_ms"] for case in cases],"physical_reads":reads,"upstream_calls":calls,
                    "physical_reads_per_upstream_call":reads/calls if calls else None}
            if comparison_axis=="request-id":
                old=report["variant_summary"]["uuid4"]["physical_reads_per_upstream_call"]
                new=report["variant_summary"]["uuid7"]["physical_reads_per_upstream_call"]
                report["physical_read_reduction_fraction"]=1-new/old if old and new is not None else None
        report.update(state="passed" if all(case["state"]=="passed" for case in report["cases"]) else "completed_with_slo_failures",stage="finished",finished=time.time(),completed=sum(case["http_requests"] for case in report["cases"]))
    except runtime.TaskStopped:
        report.update(state="stopped",error="explicit stop request")
    except Exception as exc:
        report.update(state="failed",error=f"{type(exc).__name__}: {exc}");traceback.print_exc()
    finally:
        if process is not None and process.poll() is None:
            process.terminate()
            try:process.wait(timeout=8)
            except subprocess.TimeoutExpired:process.kill();process.wait()
        try:api.cleanup()
        except Exception as exc:report["cleanup_error"]=str(exc);report["state"]="failed"
        runtime.stop(["lab-a","lab-b","fixture"])
        report["elapsed_s"]=round(time.time()-report["started"],3);save()
    return 0 if report["state"]=="passed" else 1


if __name__=="__main__":
    runtime.install_stop_handler()
    parser=argparse.ArgumentParser();parser.add_argument("--mode",choices=["perf","soak","compare"],default="perf")
    parser.add_argument("--duration",type=int,default=15);parser.add_argument("--rps",type=int,default=10)
    parser.add_argument("--accounts",type=int,default=4);parser.add_argument("--vus",type=int,default=32)
    parser.add_argument("--admission-mode",choices=["split","combined"],default="combined")
    parser.add_argument("--compare-axis",choices=["admission","request-id"],default="admission")
    args=parser.parse_args();raise SystemExit(main(args.mode,args.duration,args.rps,args.accounts,args.vus,args.admission_mode,args.compare_axis))
