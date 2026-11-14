"""Run race-enabled Go tests against the independent integration database."""
import json
import os
import subprocess
import time

import runtime

values = runtime.read_env()
env = dict(os.environ)
env["LLMGW_TEST_DSN"] = values["LLMGW_DSN"].replace("/llm_gateway?", "/llm_gateway_test?")
env["LLMGW_TEST_REDIS_ADDRESS"] = values["LLMGW_REDIS_ADDRESS"]
env["LLMGW_TEST_REDIS_PASSWORD"] = values["LLMGW_REDIS_PASSWORD"]
env["GORACE"] = "halt_on_error=1"
path = runtime.ROOT / "checks/go-test.jsonl"
started = time.time()
with path.open("w") as output:
    result = subprocess.run(["go", "test", "-race", "-count=1", "-json", "./..."], cwd=runtime.SOURCE, env=env, stdout=output, stderr=subprocess.STDOUT)
passed, failed, skipped = [], [], []
for line in path.read_text().splitlines():
    try:
        event = json.loads(line)
    except ValueError:
        continue
    if event.get("Test"):
        target = {"pass": passed, "fail": failed, "skip": skipped}.get(event.get("Action"))
        if target is not None:
            target.append(event["Test"])
summary = {"state": "passed" if result.returncode == 0 and not skipped else "failed", "elapsed_s": round(time.time()-started, 3), "race_enabled": True,
           "database": "real isolated MariaDB", "passed": passed, "failed": failed, "skipped": skipped, "raw_log": str(path)}
(runtime.ROOT / "checks/go-test.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2))
print(json.dumps(summary, ensure_ascii=False, indent=2))
raise SystemExit(0 if summary["state"] == "passed" else 1)
