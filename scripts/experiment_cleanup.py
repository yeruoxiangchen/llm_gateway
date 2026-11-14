"""systemd ExecStopPost: reclaim lab resources after success, stop or crash."""
import json
import re
import sys
import time

import runtime


def main(name):
    filename={"deep":"deep.json","perf":"performance.json","soak":"soak.json","benchmark":"benchmark.json","compare":"compare.json","tools":"tools.json","real-load":"real-load.json"}[name]
    path=runtime.ROOT/"checks"/filename
    try:
        if not runtime.active("mysql"):runtime.database_start()
        if path.exists():
            value=json.loads(path.read_text())
            prefix=value.get("namespace","")
            if re.fullmatch(r"[a-f0-9]{10}",prefix):
                database="llm_gateway" if name in ("benchmark","tools","real-load") else "llm_gateway_lab"
                runtime.sql("USE "+database+"; UPDATE providers SET enabled=FALSE WHERE name LIKE '"+prefix+"-%'; "
                            "UPDATE api_keys k JOIN accounts a ON a.id=k.account_id SET k.active=FALSE WHERE a.name LIKE '"+prefix+"-%';")
            if value.get("state")=="running":
                value.update(state="stopped",error="runner exited before completing its report",finished=time.time())
            value["service_cleanup_completed"]=True
            text=json.dumps(value,ensure_ascii=False,indent=2);path.write_text(text)
            if value.get("run_dir"):
                run_dir=runtime.ROOT/"checks"/name
                target=__import__('pathlib').Path(value["run_dir"])
                if target.is_relative_to(runtime.ROOT/"checks") and target.exists():(target/"report.json").write_text(text)
    finally:
        if name not in ("benchmark","tools","real-load"):runtime.stop(["lab-a","lab-b","fixture"])


if __name__=="__main__":main(sys.argv[1])
