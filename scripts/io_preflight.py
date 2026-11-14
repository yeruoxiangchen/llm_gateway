"""Record a bounded quiet-I/O start condition for shared-host load tests."""
import argparse
import json
from pathlib import Path
import time

import runtime


def read_pressure():
    fields=next(line.split() for line in Path("/proc/pressure/io").read_text().splitlines() if line.startswith("full "))
    return {name:float(value) for name,value in (field.split("=",1) for field in fields[1:])}


def main(wait_seconds,quiet_seconds,maximum):
    if not 5<=quiet_seconds<=wait_seconds<=600 or not 0<maximum<=10:
        raise ValueError("Use 5 <= quiet seconds <= wait seconds <= 600, full-I/O percent 0..10")
    folder=runtime.ROOT/"checks/io-preflight"
    folder.mkdir(parents=True,exist_ok=True)
    path=folder/(time.strftime("%Y%m%dT%H%M%SZ",time.gmtime())+".json")
    report={"state":"waiting","started":time.time(),"budget_s":wait_seconds,"quiet_required_s":quiet_seconds,
            "max_full_pct":maximum,"samples":[],"scope":"host PSI only; the storage remains shared"}
    began=previous_at=time.monotonic();previous=read_pressure();quiet_from=None
    while True:
        now=time.monotonic();value=read_pressure()
        interval_pct=max(0,value["total"]-previous["total"])/(now-previous_at)/10000
        quiet=value["avg10"]<=maximum and interval_pct<=maximum
        if quiet and quiet_from is None:quiet_from=now
        if not quiet:quiet_from=None
        report["samples"].append({"at":time.time(),"full_avg10_pct":value["avg10"],"full_interval_pct":round(interval_pct,4)})
        elapsed=now-began
        if quiet_from is not None and now-quiet_from>=quiet_seconds:report["state"]="quiet"
        elif elapsed>=wait_seconds:report["state"]="busy"
        report.update(elapsed_s=round(elapsed,3),quiet_observed_s=round(now-quiet_from,3) if quiet_from is not None else 0)
        temp=path.with_suffix(".tmp");temp.write_text(json.dumps(report,indent=2));temp.chmod(0o600);temp.replace(path)
        if report["state"]!="waiting":break
        previous_at,previous=now,value
        time.sleep(min(5,max(0,wait_seconds-elapsed)))
    print(json.dumps({k:v for k,v in report.items() if k!="samples"},ensure_ascii=False),"report="+str(path),flush=True)
    return 0 if report["state"]=="quiet" else 2


if __name__=="__main__":
    parser=argparse.ArgumentParser()
    parser.add_argument("--wait-seconds",type=int,default=180)
    parser.add_argument("--quiet-seconds",type=int,default=30)
    parser.add_argument("--max-full-percent",type=float,default=1)
    args=parser.parse_args()
    raise SystemExit(main(args.wait_seconds,args.quiet_seconds,args.max_full_percent))
