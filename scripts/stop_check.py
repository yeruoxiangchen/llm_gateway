"""Verify that stopping the long-test service revokes keys and frees resources."""
import json
import time

from client import eventually
from lab_tools import lab_sql
import runtime


started=time.time();report={"state":"running","started":started}
try:
    runtime.dispatch("soak",duration=60,rps=5,concurrency=16,accounts=2)
    path=runtime.ROOT/"checks/soak.json"
    def running():
        if not path.exists():return None
        data=json.loads(path.read_text())
        return data if data.get("state")=="running" and data.get("started",0)>started and data.get("completed",0)>=10 else None
    value=eventually(running,timeout=45,message="soak did not begin handling requests")
    prefix=value["namespace"]
    result=runtime.ctl("stop",runtime.UNITS["soak"])
    if result.returncode:raise AssertionError("service stop failed")
    after=json.loads(path.read_text())
    if after["state"]!="stopped" or not after.get("service_cleanup_completed"):raise AssertionError("stop report or cleanup missing")
    for name in ("soak","lab-a","lab-b","fixture"):
        if runtime.active(name):raise AssertionError("resource still running: "+name)
    counts=lab_sql("SELECT COUNT(*) FROM providers WHERE enabled=TRUE AND name LIKE '"+prefix+"-%'; "
                   "SELECT COUNT(*) FROM api_keys k JOIN accounts a ON a.id=k.account_id WHERE k.active=TRUE AND a.name LIKE '"+prefix+"-%'; "
                   "SELECT COALESCE(SUM(reserved_tokens),0) FROM accounts WHERE name LIKE '"+prefix+"-%';")
    if counts.splitlines()!=["0","0","0"]:raise AssertionError("test routes, keys or reservation remain: "+counts)
    runtime.wait_http("http://127.0.0.1:29480/health/ready",timeout=5)
    report.update(state="passed",namespace=prefix,explicit_stop=True,processes_released=True,active_test_keys=0,active_test_routes=0,reserved_tokens=0,normal_api_ready=True)
except Exception as exc:
    report.update(state="failed",error=f"{type(exc).__name__}: {exc}")
finally:
    report["elapsed_s"]=round(time.time()-started,3)
    (runtime.ROOT/"checks/stop-check.json").write_text(json.dumps(report,ensure_ascii=False,indent=2))
print(json.dumps(report,ensure_ascii=False,indent=2))
raise SystemExit(0 if report["state"]=="passed" else 1)
