"""Reproduce and verify cached-response backpressure with a real TCP client."""
import argparse
import json
import time

from client import API, eventually, stats
from lab_tools import metric_snapshot, slow_connection
import runtime


def main(phase):
    api=API(runtime.LAB_A)
    out=runtime.ROOT/"checks"/("slow-reader-"+phase+".json")
    report={"phase":phase,"state":"running","started":time.time()}
    connection=response=None
    try:
        _,key,_=api.account("slow-replay-probe")
        p=api.provider("slow-replay-probe","large")
        code,headers,raw=api.chat(key,p["model_alias"],idem="cached-large")
        if code!=200 or len(raw)<3*1024*1024:raise AssertionError("large cached response not prepared")
        calls=stats()["calls"].get("large",0)
        baseline=metric_snapshot(api)
        connection,response=slow_connection(api,key,p["model_alias"],idem="cached-large")
        report.update(request_id=headers["x-request-id"],response_bytes=len(raw),receive_buffer=connection.sock.getsockopt(1,8),baseline=baseline)
        samples=[]
        for _ in range(24):
            snapshot=metric_snapshot(api)
            samples.append(snapshot)
            if snapshot["resources"]["llmgw_inflight"]==0:break
            time.sleep(.25)
        bounded=samples[-1]["resources"]["llmgw_inflight"]==0
        report.update(bounded_return=bounded,samples=samples,upstream_extra_calls=stats()["calls"].get("large",0)-calls)
        response.close();connection.close();connection=response=None
        eventually(lambda:metric_snapshot(api)["resources"]["llmgw_inflight"]==0,timeout=10)
        report["account"]=api.usage(key)["account"]
        if report["account"]["used_tokens"]!=12 or report["upstream_extra_calls"]!=0:raise AssertionError("replay caused another upstream charge")
        expected=phase=="after"
        if bounded!=expected:raise AssertionError(f"expected bounded_return={expected}, observed {bounded}")
        report["state"]="passed" if expected else "reproduced"
    except Exception as exc:
        report.update(state="failed",error=f"{type(exc).__name__}: {exc}")
    finally:
        if response:response.close()
        if connection:connection.close()
        api.cleanup()
        report["elapsed_s"]=round(time.time()-report["started"],3)
        out.write_text(json.dumps(report,ensure_ascii=False,indent=2))
    print(json.dumps({k:v for k,v in report.items() if k not in ("samples","baseline")},ensure_ascii=False,indent=2))
    return 0 if report["state"] in ("reproduced","passed") else 1


if __name__=="__main__":
    parser=argparse.ArgumentParser();parser.add_argument("phase",choices=["before","after"])
    raise SystemExit(main(parser.parse_args().phase))
