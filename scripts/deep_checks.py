"""Isolated, deterministic backend experiments with real HTTP/SQL/TCP faults."""
from concurrent.futures import ThreadPoolExecutor
import json
import threading
import http.client
import urllib.parse
import time
import traceback

from client import API, eventually, first_event, ledger_snapshot, stats
from lab_tools import metric_snapshot, checkpoint, arrived, release_checkpoint, receipt, kill_lab, restart_lab, lab_sql, slow_connection, capture
import runtime

api=API(runtime.LAB_A)
peer=API(runtime.LAB_B)
run_dir=runtime.ROOT/"checks/deep"/time.strftime("%Y%m%dT%H%M%SZ",time.gmtime())
run_dir.mkdir(parents=True,exist_ok=True);run_dir.chmod(0o700)
ADMISSION_MODE=runtime.read_env(runtime.ROOT/"config/lab-a.env").get("LLMGW_ADMISSION_MODE","combined")
RESERVATION_POINT="after_admit" if ADMISSION_MODE=="combined" else "after_reserve"
report={"state":"running","started":time.time(),"namespace":api.prefix,"run_dir":str(run_dir),"database":"llm_gateway_lab","admission_mode":ADMISSION_MODE,"cases":[]}


def save():
    text=json.dumps(report,ensure_ascii=False,indent=2)
    (run_dir/"report.json").write_text(text)
    (runtime.ROOT/"checks/deep.json").write_text(text)


def check(value,message):
    if not value:raise AssertionError(message)


def batch(count,work):
    barrier=threading.Barrier(count)
    def call(index):barrier.wait();return work(index)
    with ThreadPoolExecutor(max_workers=count) as pool:return list(pool.map(call,range(count)))


def guards():
    check(api.call("GET","/admin/testing/state")[0]==401,"fault controls have no admin guard")
    check(api.call("GET","/debug/pprof/heap")[0]==401,"profile has no admin guard")
    check(api.call("GET","/debug/pprof/profile?seconds=999",admin=True)[0]==400,"unbounded CPU profile")
    with runtime.HTTP.open("http://127.0.0.1:29480/health/live",timeout=3) as response:check(response.status==200,"normal API stopped")
    return {"admin_required":True,"profile_max_seconds":20,"normal_api_alive":True}


def idempotency_100():
    account,key,_=api.account("idem-100")
    p=api.provider("idem-100","slow")
    before=stats()["calls"].get("slow",0)
    responses=batch(100,lambda n:api.chat(key,p["model_alias"],idem="same-request",base=runtime.LAB_A if n%2==0 else runtime.LAB_B))
    codes=[row[0] for row in responses]
    check(all(code in (200,409) for code in codes),"unexpected duplicate admission status: "+repr(codes))
    identities={headers.get("x-request-id") for _,headers,_ in responses}
    check(len(identities)==1 and None not in identities,"duplicate request records created")
    check(stats()["calls"].get("slow",0)-before==1,"more than one upstream execution")
    code,headers,_=api.chat(key,p["model_alias"],idem="same-request",base=runtime.LAB_B)
    check(code==200 and headers.get("x-idempotency-replayed")=="true","response cannot replay across replicas")
    check(api.chat(key,p["model_alias"],idem="same-request",messages=[{"role":"user","content":"changed"}])[0]==409,"request hash conflict bypassed")
    total=lab_sql(f"SELECT COUNT(*) FROM ledger WHERE account_id='{account['id']}' AND event_type='settle';")
    check(total=="1" and api.usage(key)["account"]["used_tokens"]==12,"double settlement")
    return {"clients":100,"replicas":2,"statuses":{str(c):codes.count(c) for c in set(codes)},"upstream_calls":1,"settlements":1,"used_tokens":12}


def quota_100():
    _,key,_=api.account("quota-100",quota=2000)
    p=api.provider("quota-100")
    checkpoint(api,RESERVATION_POINT,"held-reservation")
    with ThreadPoolExecutor(max_workers=1) as held:
        original=held.submit(api.chat,key,p["model_alias"],idem="held-reservation")
        arrived(api,RESERVATION_POINT)
        reserved=api.usage(key)["account"]["reserved_tokens"]
        check(1000<reserved<2000,"unexpected reservation estimate")
        try:
            responses=batch(100,lambda n:api.chat(key,p["model_alias"],base=runtime.LAB_A if n%2==0 else runtime.LAB_B))
            check(all(row[0]==402 for row in responses),"quota admitted a conflicting reservation")
        finally:release_checkpoint(api,RESERVATION_POINT)
        check(original.result(timeout=10)[0]==200,"held request failed")
    account=api.usage(key)["account"]
    check(account["reserved_tokens"]==0 and account["used_tokens"]==12,"quota not released after settlement")
    return {"clients_rejected":100,"status":402,"peak_held_reservation":reserved,"final_used":12,"final_reserved":0}


def debt_policy():
    _,key,_=api.account("debt-policy",quota=3000)
    p=api.provider("debt-policy","overusage")
    check(api.chat(key,p["model_alias"])[0]==200,"over-estimate usage response failed")
    account=api.usage(key)["account"]
    check(account["used_tokens"]==6001 and account["available_tokens"]==-3001,"actual usage clipped or discarded")
    check(api.chat(key,p["model_alias"],base=runtime.LAB_B)[0]==402,"debt not enforced on other replica")
    return {"quota":3000,"actual_usage":6001,"available":-3001,"next_request_status":402,"policy":"estimate-based admission; actual usage is authoritative"}


def disconnect_reserved():
    _,key,_=api.account("disconnect-reserved")
    p=api.provider("disconnect-reserved")
    checkpoint(api,RESERVATION_POINT,"disconnect-reserved")
    target=urllib.parse.urlsplit(api.base)
    connection=http.client.HTTPConnection(target.hostname,target.port,timeout=10)
    before=stats()["calls"].get("normal",0)
    owner=runtime.ctl("show",runtime.UNITS["lab-a"],"-p","MainPID","--value").stdout.strip()
    try:
        connection.request("POST","/v1/chat/completions",json.dumps(api.body(p["model_alias"])),
                           {"Content-Type":"application/json","Authorization":"Bearer "+key,"Idempotency-Key":"disconnect-reserved"})
        gate=arrived(api,RESERVATION_POINT);identity=gate["request_id"]
        connection.close()
        started=time.monotonic();receipt(peer,key,identity,"released",timeout=5)
        elapsed=time.monotonic()-started
        check(api.usage(key)["account"]["reserved_tokens"]==0,"live owner's cancelled reservation remained held")
        check(stats()["calls"].get("normal",0)==before,"cancelled request reached the model")
        check(runtime.ctl("show",runtime.UNITS["lab-a"],"-p","MainPID","--value").stdout.strip()==owner,"release depended on owner restart")
    finally:
        connection.close();release_checkpoint(api,RESERVATION_POINT)
    return {"reservation_released_s":round(elapsed,3),"owner_remained_alive":True,"upstream_calls":0}


def distributed_rpm():
    if time.time()%60>55:time.sleep(60-time.time()%60+.05)
    _,key,_=api.account("rpm-100",rpm=10)
    p=api.provider("rpm-100")
    started_window=int(time.time()//60)
    responses=batch(100,lambda n:api.chat(key,p["model_alias"],base=runtime.LAB_A if n%2==0 else runtime.LAB_B))
    codes=[row[0] for row in responses]
    check(int(time.time()//60)==started_window,"test crossed a fixed-window boundary")
    check(codes.count(200)==10 and codes.count(429)==90,"distributed RPM violation: "+repr(codes))
    check(api.usage(key)["account"]["used_tokens"]==120,"rejected request was charged")
    return {"replicas":2,"clients":100,"limit_rpm":10,"accepted":10,"rejected":90}


def original_call(key,alias,idem,stream=False):
    try:return api.chat(key,alias,stream=stream,idem=idem)
    except Exception as exc:return (0,{},type(exc).__name__)


def crash_at(point,expected,stream=False,scenario="normal"):
    _,key,_=api.account("crash-"+point)
    p=api.provider("crash-"+point,scenario)
    identifier="crash-"+point
    before=stats()["calls"].get(scenario,0)
    checkpoint(api,point,identifier)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future=pool.submit(original_call,key,p["model_alias"],identifier,stream)
        gate=arrived(api,point)
        identity=gate["request_id"]
        kill_lab()
        original_status=future.result(timeout=8)[0]
    value=receipt(peer,key,identity,expected,timeout=18)
    held=peer.usage(key)["account"]["reserved_tokens"]
    if expected=="pending_usage":
        check(held>0 and value.get("usage") is None,"unknown usage was released or fabricated")
        if point=="before_settle":
            peer.json("POST",f"/admin/requests/{identity}/resolve",{"usage":{"prompt_tokens":5,"completion_tokens":7,"total_tokens":12},"reason":"fixture completed before kill; reconciled with controlled upstream receipt"},admin=True)
        else:peer.refund(identity)
    restart_lab()
    calls=stats()["calls"].get(scenario,0)-before
    if point in ("after_reserve","after_admit","after_dispatch"):check(calls==0,"request reached upstream before dispatch checkpoint")
    else:check(calls==1,"unexpected upstream attempt count")
    replay=False
    if expected=="settled":
        status,headers,_=api.chat(key,p["model_alias"],idem=identifier)
        replay=status==200 and headers.get("x-idempotency-replayed")=="true"
        check(replay and stats()["calls"].get(scenario,0)-before==1,"committed response repeated upstream execution")
    return {"checkpoint":point,"request_id":identity,"client_status":original_status,"recovered_state":expected,"held_before_resolution":held,"upstream_calls":calls,"replayed":replay}


def crash_matrix():
    return {"checkpoints":[crash_at(RESERVATION_POINT,"pending_usage" if ADMISSION_MODE=="combined" else "released"),crash_at("after_dispatch","pending_usage"),
                           crash_at("after_first_event","pending_usage",True,"hold"),crash_at("before_settle","pending_usage"),
                           crash_at("after_settle","settled")]}


def durable_spool_crash():
    _,key,_=api.account("spool-crash")
    p=api.provider("spool-crash")
    checkpoint(api,"before_settle","spool-crash");checkpoint(api,"after_spool","spool-crash")
    before=stats()["calls"].get("normal",0)
    with ThreadPoolExecutor(max_workers=1) as pool:
        future=pool.submit(original_call,key,p["model_alias"],"spool-crash")
        gate=arrived(api,"before_settle");identity=gate["request_id"]
        path=runtime.ROOT/"lab/a/spool"/(identity+".json")
        try:
            runtime.stop(["mysql"])
            release_checkpoint(api,"before_settle")
            arrived(api,"after_spool",timeout=12)
            check(path.exists(),"durable receipt not found")
            data=path.read_bytes();saved=json.loads(data)
            check(saved["usage"]["total_tokens"]==12 and path.stat().st_mode&0o077==0,"invalid durable receipt")
            kill_lab();future.result(timeout=8)
        finally:runtime.database_start()
    restart_lab();receipt(peer,key,identity,"settled")
    eventually(lambda:not path.exists(),timeout=12)
    path.write_bytes(data);path.chmod(0o600)
    eventually(lambda:not path.exists(),timeout=12)
    account=api.usage(key)["account"]
    check(account["used_tokens"]==12 and account["reserved_tokens"]==0,"durable replay double charged")
    check(stats()["calls"].get("normal",0)-before==1,"durable recovery repeated upstream")
    return {"request_id":identity,"faults":["actual database stop","SIGKILL after receipt fsync"],"receipt_replayed_twice":True,"used":12,"reserved":0,"upstream_calls":1}


def stream_resources():
    _,key,_=api.account("streams")
    p=api.provider("streams","paced")
    baseline_a=metric_snapshot(api);baseline_b=metric_snapshot(peer)
    complete=batch(20,lambda n:api.chat(key,p["model_alias"],stream=True,base=runtime.LAB_A if n%2==0 else runtime.LAB_B))
    check(all(row[0]==200 and b"[DONE]" in row[2] for row in complete),"paced stream completion failed")
    cancelled_before=stats()["cancelled"].get("paced",0)
    def disconnect(index):
        response=api.stream(key,p["model_alias"],base=runtime.LAB_A if index%2==0 else runtime.LAB_B)
        first_event(response);identity=response.headers["X-Request-ID"]
        time.sleep(.03);response.close();return identity
    identities=batch(20,disconnect)
    eventually(lambda:stats()["cancelled"].get("paced",0)-cancelled_before==20,timeout=8,message="upstream cancellation not propagated")
    for identity in identities:
        receipt(api,key,identity,"pending_usage");api.refund(identity)
    eventually(lambda:metric_snapshot(api)["resources"]["llmgw_inflight"]==0 and metric_snapshot(peer)["resources"]["llmgw_inflight"]==0,timeout=10)
    final_a=metric_snapshot(api);final_b=metric_snapshot(peer)
    check(api.usage(key)["account"]["used_tokens"]==240,"cancelled streams incorrectly settled as completed")
    profiles=[capture(api,"goroutine",run_dir/"goroutines-after-streams.pprof"),capture(api,"heap",run_dir/"heap-after-streams.pprof")]
    return {"completed":20,"cancelled":20,"upstream_cancellations":20,"used_tokens":240,"baselines":[baseline_a,baseline_b],"after":[final_a,final_b],"profiles":profiles}


def slow_readers():
    _,key,_=api.account("slow-readers")
    flood=api.provider("slow-flood","flood")
    normal=api.provider("slow-normal","large")
    handles=[];ids=[]
    try:
        for p,stream in [(flood,True),(flood,True),(normal,False)]:
            connection,response=slow_connection(api,key,p["model_alias"],stream=stream)
            handles.append((connection,response));ids.append(response.headers["X-Request-ID"])
        started=time.monotonic()
        eventually(lambda:metric_snapshot(api)["resources"]["llmgw_inflight"]==0,timeout=8,message="slow reader retained an inflight slot")
        elapsed=time.monotonic()-started
    finally:
        for connection,response in handles:response.close();connection.close()
    states=[]
    for identity in ids:
        value=api.receipt(identity,key);states.append(value["state"])
        if value["state"]=="pending_usage":api.refund(identity)
        else:check(value["state"]=="settled","slow reader ended in unexpected state")
    return {"slow_sockets":3,"receive_buffer_requested":4096,"all_slots_released_after_s":round(elapsed,3),"states":states,"write_deadline_s":2}


def half_open_probe():
    _,key,_=api.account("half-open")
    bad=api.provider("half-open-bad","fail503",alias=api.prefix+"-half-open",priority=10)
    api.provider("half-open-good",alias=bad["model_alias"])
    before=stats()["calls"].get("fail503",0)
    for _ in range(3):check(api.chat(key,bad["model_alias"])[0]==200,"initial fallback failed")
    check(stats()["calls"].get("fail503",0)-before==3,"circuit did not trip")
    time.sleep(10.1)
    checkpoint(api,"after_dispatch","half-open-probe")
    with ThreadPoolExecutor(max_workers=1) as pool:
        original=pool.submit(api.chat,key,bad["model_alias"],idem="half-open-probe")
        gate=arrived(api,"after_dispatch")
        value=api.receipt(gate["request_id"],key)
        check(value["provider_id"]==bad["id"],"checkpoint missed half-open probe")
        try:
            responses=batch(20,lambda _:api.chat(key,bad["model_alias"]))
            check(all(row[0]==200 for row in responses),"half-open contention broke fallback")
            check(stats()["calls"].get("fail503",0)-before==3,"more than one half-open slot was granted")
        finally:release_checkpoint(api,"after_dispatch")
        check(original.result(timeout=10)[0]==200,"probe fallback failed")
    check(stats()["calls"].get("fail503",0)-before==4,"probe attempt count")
    return {"scope":"one gateway instance","contending_clients":20,"simultaneous_half_open_probes":1,"fallback_responses":20}


def invariants():
    rows=ledger_snapshot(api.accounts,"llm_gateway_lab")
    check(all(row["reserved"]==0 for row in rows),"unresolved test reservations")
    races=list((runtime.ROOT/"logs").glob("lab-race.*"));check(not races,"race detector reported a conflict")
    return {"accounts":len(rows),"account_equals_ledger":True,"unresolved_reserved":0,"race_reports":0}


CASES=[guards,idempotency_100,quota_100,debt_policy,disconnect_reserved,distributed_rpm,crash_matrix,durable_spool_crash,stream_resources,slow_readers,half_open_probe,invariants]


if __name__=="__main__":
    runtime.install_stop_handler();report["total"]=len(CASES);save()
    try:
        for case in CASES:
            report["stage"]=case.__name__;save();started=time.monotonic()
            details=case();report["cases"].append({"name":case.__name__,"state":"passed","elapsed_s":round(time.monotonic()-started,3),"details":details})
            report["cases_completed"]=len(report["cases"]);save();print(case.__name__,"PASS",flush=True)
        report.update(state="passed",stage="finished",passed=len(CASES))
    except runtime.TaskStopped:
        report.update(state="stopped",error="explicit stop request")
    except Exception as exc:
        report.update(state="failed",error=f"{type(exc).__name__}: {exc}");traceback.print_exc()
    finally:
        try:
            if not runtime.active("mysql"):runtime.database_start()
            restart_lab();api.cleanup()
        except Exception as exc:report["cleanup_error"]=str(exc);report["state"]="failed"
        runtime.stop(["lab-a","lab-b","fixture"])
        report["elapsed_s"]=round(time.time()-report["started"],3);save()
    raise SystemExit(0 if report["state"]=="passed" else 1)
