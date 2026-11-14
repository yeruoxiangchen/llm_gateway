"""Shared isolated-lab checks, measurements and controlled TCP readers."""
from __future__ import annotations

import http.client
import json
import re
import socket
import time
import urllib.parse
from pathlib import Path

from client import API, eventually, HTTP, stats
import runtime


def metric_snapshot(api, base=None):
    code, _, raw=api.call("GET","/metrics",admin=True,base=base)
    if code!=200: raise AssertionError("Metrics unavailable: "+str(code))
    counters={};stages={}
    resources={};db_pool={};metadata_db_pool={};admission_mode=None;protection_rejections={};request_id_version=None
    rate_limit={"retries":0,"cache_hits":0,"attempts":{}}
    names={"go_goroutines","process_resident_memory_bytes","process_open_fds","llmgw_inflight","process_cpu_seconds_total"}
    for line in raw.decode().splitlines():
        if not line or line.startswith("#"):continue
        name,value=line.split()[:2]
        number=float(value)
        admission=re.fullmatch(r'llmgw_admission_info\{mode="(split|combined)"\}',name)
        if admission and number==1:admission_mode=admission[1]
        identity=re.fullmatch(r'llmgw_request_id_info\{version="([47])"\}',name)
        if identity and number==1:request_id_version=int(identity[1])
        rejection=re.fullmatch(r'llmgw_protection_rejections_total\{reason="([a-z_]+)"\}',name)
        if rejection:protection_rejections[rejection[1]]=number
        if name in names:resources[name]=number
        pool=re.fullmatch(r'go_sql_([a-z_]+)\{db_name="(gateway|gateway_metadata)"\}',name)
        if pool:(db_pool if pool[2]=="gateway" else metadata_db_pool)[pool[1]]=number
        if name=="llmgw_rate_limit_retries_total":rate_limit["retries"]=number
        if name=="llmgw_rate_limit_cache_hits_total":rate_limit["cache_hits"]=number
        attempt=re.fullmatch(r'llmgw_rate_limit_attempts_total\{result="([a-z_]+)"\}',name)
        if attempt:rate_limit["attempts"][attempt[1]]=number
        if name.startswith("llmgw_stage_seconds_"):
            match=re.fullmatch(r'llmgw_stage_seconds_(sum|count)\{stage="([^"]+)"\}',name)
            if match:stages.setdefault(match[2],{})[match[1]]=number
        if name.startswith("llmgw_requests_total{"):counters[name]=number
    return {"at":time.time(),"resources":resources,"db_pool":db_pool,"admission_mode":admission_mode,"stages":stages,"requests":counters,
            "protection_rejections":protection_rejections,"request_id_version":request_id_version,
            "metadata_db_pool":metadata_db_pool,"rate_limit":rate_limit}


def stage_delta(before,after):
    result={}
    for name,value in after["stages"].items():
        old=before["stages"].get(name,{})
        count=value.get("count",0)-old.get("count",0)
        seconds=value.get("sum",0)-old.get("sum",0)
        if count>0:result[name]={"count":int(count),"total_ms":round(seconds*1000,3),"mean_ms":round(seconds*1000/count,3)}
    return result


def database_counters():
    raw=runtime.sql("SHOW GLOBAL STATUS WHERE Variable_name IN ('Innodb_row_lock_time','Innodb_row_lock_waits','Innodb_os_log_fsyncs','Innodb_log_waits','Innodb_deadlocks','Innodb_buffer_pool_reads','Innodb_buffer_pool_read_requests');")
    return {line.split("\t")[0]:int(line.split("\t")[1]) for line in raw.splitlines()}


def lab_sql(query):
    return runtime.sql("USE llm_gateway_lab; "+query)


def checkpoint(api,point,key):
    return api.json("POST","/admin/testing/arm",{"point":point,"idempotency_key":key},admin=True,expected=201)[0]


def arrived(api,point,timeout=20):
    def check():
        gates=api.json("GET","/admin/testing/state",admin=True)[0]
        gate=gates.get(point,{})
        return gate if gate.get("arrived") else None
    return eventually(check,timeout,"checkpoint not reached: "+point)


def release_checkpoint(api,point):
    return api.json("POST","/admin/testing/release",{"point":point},admin=True)[0]


def receipt(api,key,identity,state,timeout=20):
    def check():
        value=api.receipt(identity,key)
        return value if value["state"]==state else None
    return eventually(check,timeout,"request did not reach "+state+": "+identity)


def restart_lab(name="a",race=True):
    runtime.launch("lab-"+name,[runtime.immutable_binary("gateway-race" if race else "gateway")],"1G" if race else "768M","200%",runtime.ROOT/"config"/f"lab-{name}.env")
    runtime.wait_http((runtime.LAB_A if name=="a" else runtime.LAB_B)+"/health/ready")


def kill_lab(name="a"):
    result=runtime.ctl("kill","--signal=KILL",runtime.UNITS["lab-"+name])
    if result.returncode:raise RuntimeError("Could not terminate isolated lab process")
    eventually(lambda:not runtime.active("lab-"+name),5)


def slow_connection(api,key,alias,stream=False,idem=None):
    """Restrict only this socket's receive window; no host sysctl changes."""
    target=urllib.parse.urlsplit(api.base)
    connection=http.client.HTTPConnection(target.hostname,target.port,timeout=10)
    channel=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    channel.setsockopt(socket.SOL_SOCKET,socket.SO_RCVBUF,4096)
    if hasattr(socket,"TCP_WINDOW_CLAMP"):
        channel.setsockopt(socket.IPPROTO_TCP,socket.TCP_WINDOW_CLAMP,4096)
    channel.settimeout(10)
    channel.connect((target.hostname,target.port))
    connection.sock=channel
    headers={"Authorization":"Bearer "+key,"Content-Type":"application/json"}
    if idem is not None:headers["Idempotency-Key"]=idem
    connection.request("POST","/v1/chat/completions",json.dumps(api.body(alias,stream)),headers)
    response=connection.getresponse()
    if response.status!=200:
        connection.close();raise AssertionError("slow client admission status "+str(response.status))
    return connection,response


def capture(api,kind,path,seconds=None):
    url="/debug/pprof/"+kind+("?seconds="+str(seconds) if seconds else "")
    code,_,data=api.call("GET",url,admin=True,timeout=30)
    if code!=200:raise AssertionError("Profile failed: "+str(code))
    path=Path(path);path.write_bytes(data);path.chmod(0o600)
    return {"kind":kind,"bytes":len(data),"path":str(path)}
