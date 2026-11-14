"""Isolated services, dependency setup and explicit verification entry points."""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import secrets
import shlex
import shutil
import socket
import subprocess
import tarfile
import time
import urllib.request
import fcntl
import signal

ROOT = Path(os.environ.get("LLMGW_DATA_ROOT", "/data/zjr/llm_gateway"))
SOURCE = Path(__file__).resolve().parents[1]
NATIVE = ROOT / "toolchain/native/usr"
UNITS = {name: f"llmgw-{name}.service" for name in ["mysql", "bootstrap", "redis", "api", "peer", "fixture", "verify", "stability", "lab-a", "lab-b", "deep", "perf", "soak", "benchmark", "compare", "tools", "real-load"]}
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))
LAB_A = "http://127.0.0.1:29483"
LAB_B = "http://127.0.0.1:29484"
RUNNERS = ["verify", "stability", "deep", "perf", "soak", "benchmark", "compare", "tools", "real-load"]


class TaskStopped(Exception):
    pass


def install_stop_handler():
    def stop_signal(signum, frame):
        raise TaskStopped("explicit stop signal")
    signal.signal(signal.SIGTERM, stop_signal)
    signal.signal(signal.SIGINT, stop_signal)


def ctl(*args):
    return subprocess.run(["systemctl", "--user", *args], text=True, capture_output=True)


def active(name):
    return ctl("is-active", "--quiet", UNITS[name]).returncode == 0


def read_env(path=None):
    result = {}
    for line in (path or ROOT / "config/gateway.env").read_text().splitlines():
        if line and not line.startswith("#"):
            name, value = line.split("=", 1)
            result[name] = shlex.split(value)[0]
    return result


def save_env(path, values):
    path.write_text("".join(f"{name}={shlex.quote(str(value))}\n" for name, value in values.items()))
    path.chmod(0o600)


def prepare():
    for folder in ["tmp", "logs", "config", "run", "db", "redis", "spool", "checks", "bin", "peer/spool", "lab/a/spool", "lab/b/spool", "toolchain", "releases"]:
        (ROOT / folder).mkdir(parents=True, exist_ok=True)
    for folder in ["config", "run", "db", "spool", "peer", "lab", "lab/a", "lab/b"]:
        (ROOT / folder).chmod(0o700)
    path = ROOT / "config/gateway.env"
    if not path.exists():
        password = secrets.token_hex(24)
        values = {
            "LLMGW_ADDRESS": "127.0.0.1:29480", "LLMGW_DATA_ROOT": str(ROOT),
            "LLMGW_DB_PASSWORD": password, "LLMGW_DB_ROOT_PASSWORD": secrets.token_hex(24),
            "LLMGW_DSN": f"gateway:{password}@tcp(127.0.0.1:29406)/llm_gateway?parseTime=true&loc=UTC&timeout=3s&readTimeout=5s&writeTimeout=5s",
            "LLMGW_REDIS_ADDRESS": "127.0.0.1:29479", "LLMGW_REDIS_PASSWORD": secrets.token_hex(24),
            "LLMGW_ADMIN_TOKEN": secrets.token_urlsafe(32),
            "LLMGW_MASTER_KEY": base64.urlsafe_b64encode(secrets.token_bytes(32)).decode().rstrip("="),
            "LLMGW_MAX_INFLIGHT": 32, "LLMGW_REQUEST_TIMEOUT_SECONDS": 120,
            "LLMGW_DATABASE_READ_TIMEOUT_MS": 1000, "LLMGW_ADMISSION_TIMEOUT_MS": 5000,
            "LLMGW_STREAM_IDLE_SECONDS": 20, "LLMGW_LEASE_SECONDS": 45, "LLMGW_HEARTBEAT_SECONDS": 5,
        }
        save_env(path, values)
    values = read_env()
    values.setdefault("LLMGW_DATABASE_READ_TIMEOUT_MS", 1000)
    values.setdefault("LLMGW_ADMISSION_TIMEOUT_MS", 5000)
    values.setdefault("LLMGW_REQUEST_ID_VERSION", 4)
    values.setdefault("LLMGW_METADATA_DB_CONNECTIONS", 2)
    values.setdefault("LLMGW_RATE_LIMIT_TIMEOUT_MS", 3000)
    values.update({name: os.environ[name] for name in ["TMPDIR", "TORCH_HOME", "HF_HOME", "HF_HUB_CACHE", "TORCH_EXTENSIONS_DIR", "MPLCONFIGDIR"] if name in os.environ})
    values.update({"LD_LIBRARY_PATH": str(NATIVE / "lib/x86_64-linux-gnu"), "NO_PROXY": "127.0.0.1,localhost,::1", "no_proxy": "127.0.0.1,localhost,::1", "GOMAXPROCS": 4, "GOMEMLIMIT": "384MiB"})
    save_env(path, values)
    peer = dict(values)
    peer.update(LLMGW_ADDRESS="127.0.0.1:29482", LLMGW_DATA_ROOT=str(ROOT / "peer"),
                LLMGW_MAX_INFLIGHT=4,
                LLMGW_REQUEST_TIMEOUT_SECONDS=3, LLMGW_STREAM_IDLE_SECONDS=1,
                LLMGW_LEASE_SECONDS=8, LLMGW_HEARTBEAT_SECONDS=2,
                GORACE=f"halt_on_error=1 log_path={ROOT}/logs/race")
    save_env(ROOT / "config/peer.env", peer)
    for name, port in [("a", 29483), ("b", 29484)]:
        lab = dict(values)
        lab.update(LLMGW_ADDRESS=f"127.0.0.1:{port}", LLMGW_DATA_ROOT=str(ROOT / "lab" / name),
                   LLMGW_DSN=values["LLMGW_DSN"].replace("/llm_gateway?", "/llm_gateway_lab?"),
                   LLMGW_MAX_INFLIGHT=128, LLMGW_DB_CONNECTIONS=16,
                   LLMGW_REQUEST_TIMEOUT_SECONDS=30, LLMGW_STREAM_IDLE_SECONDS=2,
                   LLMGW_LEASE_SECONDS=8, LLMGW_HEARTBEAT_SECONDS=1,
                   LLMGW_DIAGNOSTICS="true", LLMGW_FAULT_INJECTION="true", LLMGW_REQUEST_TRACES="true",
                   GORACE=f"halt_on_error=1 log_path={ROOT}/logs/lab-race")
        save_env(ROOT / "config" / f"lab-{name}.env", lab)
    client = ROOT / "config/root.cnf"
    client.write_text(f"[client]\nuser=root\npassword={values['LLMGW_DB_ROOT_PASSWORD']}\nsocket={ROOT}/run/mysql.sock\ndefault-character-set=utf8mb4\n")
    client.chmod(0o600)
    (ROOT / "config/mariadb.cnf").write_text(f"""[mysqld]
basedir={NATIVE}
datadir={ROOT}/db
socket={ROOT}/run/mysql.sock
pid-file={ROOT}/run/mysql.pid
port=29406
bind-address=127.0.0.1
skip-name-resolve
character-set-server=utf8mb4
collation-server=utf8mb4_unicode_ci
default-time-zone=+00:00
innodb-buffer-pool-size=128M
innodb-log-file-size=32M
innodb-flush-log-at-trx-commit=1
max-connections=80
max-allowed-packet=16M
performance-schema=OFF
tmpdir={ROOT}/tmp
log-error={ROOT}/logs/mariadb.log
""")
    redis = ROOT / "config/redis.conf"
    redis.write_text(f"bind 127.0.0.1\nprotected-mode yes\nport 29479\nrequirepass {values['LLMGW_REDIS_PASSWORD']}\ndir {ROOT}/redis\nmaxmemory 64mb\nmaxmemory-policy noeviction\nappendonly yes\nappendfsync everysec\nlogfile {ROOT}/logs/redis-server.log\n")
    redis.chmod(0o600)


def setup():
    prepare()
    if not (ROOT / "toolchain/go/bin/go").exists():
        version = "go1.27.4"
        with HTTP.open("https://go.dev/dl/?mode=json&include=all", timeout=30) as response:
            releases = json.load(response)
        release = next(r for r in releases if r["version"] == version)
        asset = next(a for a in release["files"] if a["os"] == "linux" and a["arch"] == "amd64" and a["kind"] == "archive")
        archive = ROOT / "tmp" / asset["filename"]
        with HTTP.open("https://go.dev/dl/" + asset["filename"], timeout=90) as response, archive.open("wb") as target:
            shutil.copyfileobj(response, target, 1024 * 1024)
        if hashlib.sha256(archive.read_bytes()).hexdigest() != asset["sha256"]:
            raise RuntimeError("Go archive checksum mismatch")
        with tarfile.open(archive) as bundle:
            bundle.extractall(ROOT / "toolchain", filter="data")
    if not (NATIVE / "sbin/mariadbd").exists():
        cached = Path("/data/zjr/smart_office/toolchain/native/usr")
        if not (cached / "sbin/mariadbd").exists():
            raise RuntimeError("Portable MariaDB/Redis binaries are required; see README")
        print("Copying cached native binaries into independent gateway toolchain", flush=True)
        shutil.copytree(cached, NATIVE, symlinks=True, dirs_exist_ok=True)
    print("Independent runtime prepared", ROOT, flush=True)


def launch(name, command, memory, cpu="100%", envfile=None):
    if active(name):
        return
    ports = {"mysql": 29406, "redis": 29479, "api": 29480, "peer": 29482, "fixture": 29481, "lab-a":29483, "lab-b":29484}
    if name in ports:
        with socket.socket() as guard:
            guard.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                guard.bind(("127.0.0.1", ports[name]))
            except OSError as exc:
                raise RuntimeError(f"Port {ports[name]} is occupied outside {UNITS[name]}") from exc
    ctl("reset-failed", UNITS[name])
    command = [str(value) for value in command]
    args = ["systemd-run", "--user", "--collect", "--unit=" + UNITS[name],
            "--property=Restart=no", "--property=MemoryMax=" + memory, "--property=CPUQuota=" + cpu,
            "--property=TasksMax=256", "--property=TimeoutStopSec=15", "--property=WorkingDirectory=" + str(SOURCE),
            "--property=UMask=0077",
            "--property=EnvironmentFile=" + str(envfile or ROOT / "config/gateway.env"),
            "--property=StandardOutput=append:" + str(ROOT / "logs" / (name + ".log")),
            "--property=StandardError=append:" + str(ROOT / "logs" / (name + ".log"))]
    if name in ("deep","perf","soak","benchmark","compare","tools","real-load"):
        args += ["--property=TimeoutStopSec=60", "--property=ExecStopPost=/home/zjr/anaconda3/bin/python -B " + str(SOURCE/"scripts/experiment_cleanup.py") + " " + name]
    subprocess.run(args + command, check=True)
    print(name, UNITS[name], flush=True)


def stop(names):
    for name in names:
        ctl("stop", UNITS[name])
        print(name, "stopped", flush=True)


def native_env():
    return {**os.environ, "LD_LIBRARY_PATH": str(NATIVE / "lib/x86_64-linux-gnu"), "TMPDIR": str(ROOT / "tmp")}


def sql(query, initial=False, timeout=None):
    command = [str(NATIVE / "bin/mariadb")]
    if initial:
        command += ["--no-defaults", "--user=root", "--socket=" + str(ROOT / "run/mysql.sock")]
    else:
        command += ["--defaults-file=" + str(ROOT / "config/root.cnf")]
    result = subprocess.run(command + ["--batch", "--skip-column-names"], input=query, text=True, capture_output=True, env=native_env(), timeout=timeout)
    if result.returncode:
        raise RuntimeError("Gateway database command failed: " + result.stderr[-600:])
    return result.stdout.strip()


def wait_database(initial=False):
    deadline = time.monotonic() + 45
    while time.monotonic() < deadline:
        try:
            sql("SELECT 1", initial)
            return
        except RuntimeError:
            time.sleep(0.5)
    raise RuntimeError("Gateway database failed to start; see logs/mariadb.log")


def wait_http(url, timeout=45):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with HTTP.open(url, timeout=3) as response:
                if response.status == 200:
                    return
        except OSError:
            pass
        time.sleep(0.5)
    raise RuntimeError("Service did not become ready: " + url)


def database_start():
    launch("mysql", [NATIVE / "sbin/mariadbd", "--defaults-file=" + str(ROOT / "config/mariadb.cnf")], "512M")
    wait_database()


def init():
    prepare()
    if not (ROOT / "db/mysql").exists():
        subprocess.run([str(NATIVE / "bin/mariadb-install-db"), "--no-defaults", "--basedir=" + str(NATIVE),
                        "--datadir=" + str(ROOT / "db"), "--auth-root-authentication-method=normal", "--skip-test-db"],
                       env=native_env(), check=True, stdout=(ROOT / "logs/database-init.log").open("w"), stderr=subprocess.STDOUT)
    if not (ROOT / "config/database-initialized").exists():
        launch("bootstrap", [NATIVE / "sbin/mariadbd", "--defaults-file=" + str(ROOT / "config/mariadb.cnf"), "--skip-networking"], "512M")
        try:
            wait_database(initial=True)
            values = read_env()
            sql(f"ALTER USER 'root'@'localhost' IDENTIFIED BY '{values['LLMGW_DB_ROOT_PASSWORD']}';", initial=True)
            sql(f"CREATE DATABASE IF NOT EXISTS llm_gateway CHARACTER SET utf8mb4; CREATE DATABASE IF NOT EXISTS llm_gateway_test CHARACTER SET utf8mb4; "
                f"CREATE USER IF NOT EXISTS 'gateway'@'127.0.0.1' IDENTIFIED BY '{values['LLMGW_DB_PASSWORD']}'; "
                "GRANT ALL ON llm_gateway.* TO 'gateway'@'127.0.0.1'; GRANT ALL ON llm_gateway_test.* TO 'gateway'@'127.0.0.1'; FLUSH PRIVILEGES;")
            (ROOT / "config/database-initialized").write_text("initialized\n")
        finally:
            stop(["bootstrap"])
    database_start()
    sql("CREATE DATABASE IF NOT EXISTS llm_gateway_lab CHARACTER SET utf8mb4; GRANT ALL ON llm_gateway_lab.* TO 'gateway'@'127.0.0.1';")
    launch("redis", [NATIVE / "bin/redis-server", ROOT / "config/redis.conf"], "128M")


def immutable_binary(name):
    built = ROOT / "bin" / name
    identity = hashlib.sha256(built.read_bytes()).hexdigest()[:20]
    release = ROOT / "releases" / (name + "-" + identity)
    if not release.exists():
        shutil.copyfile(built, release)
        release.chmod(0o700)
    return release


def start(peer=False, fixture=False):
    init()
    if not active("api"):
        launch("api", [immutable_binary("gateway")], "768M", "200%")
    wait_http("http://127.0.0.1:29480/health/ready")
    if fixture:
        launch("fixture", [immutable_binary("upstream-fixture")], "128M")
        wait_http("http://127.0.0.1:29481/health")
    if peer:
        launch("peer", [immutable_binary("gateway-race")], "768M", "200%", ROOT / "config/peer.env")
        wait_http("http://127.0.0.1:29482/health/ready")


def start_lab(race=False, both=True, admission_mode="combined", request_id_version=None):
    init()
    launch("fixture", [immutable_binary("upstream-fixture")], "128M")
    wait_http("http://127.0.0.1:29481/health")
    for name in (["a", "b"] if both else ["a"]):
        path = ROOT / "config" / f"lab-{name}.env"
        values = read_env(path)
        # The 100-client quota/idempotency cases exercise business races below
        # the entry limit. Dedicated protection tests use a two-request limit.
        values["LLMGW_MAX_INFLIGHT"] = 128 if race else 32
        values["LLMGW_REQUEST_TRACES"] = "true" if race else "false"
        values["LLMGW_ADMISSION_MODE"] = admission_mode
        if request_id_version is not None:values["LLMGW_REQUEST_ID_VERSION"]=request_id_version
        save_env(path, values)
        launch("lab-"+name, [immutable_binary("gateway-race" if race else "gateway")], "1G" if race else "768M", "200%", path)
        wait_http((LAB_A if name=="a" else LAB_B)+"/health/ready")


def status():
    for name in UNITS:
        if name == "bootstrap":
            continue
        output = ctl("show", UNITS[name], "-p", "LoadState", "-p", "ActiveState", "-p", "Result", "-p", "MainPID", "-p", "MemoryCurrent").stdout.strip()
        details = dict(line.split("=",1) for line in output.splitlines() if "=" in line)
        if details.get("LoadState")=="not-found":
            print(name,"已退出/未启动；测试结论见下方报告")
        else:
            print(name,output.replace("\n"," | "))
    for name in ["go-test", "verification", "stability", "deep", "performance", "soak", "benchmark", "compare", "tools", "real-load"]:
        path = ROOT / "checks" / (name + ".json")
        if path.exists():
            report = json.loads(path.read_text())
            fields = ["state", "stage", "passed", "elapsed_s", "error", "completed", "planned_calls", "successful", "skipped", "achieved_rps", "p95_ms", "p99_ms", "run_dir", "cases_completed"]
            summary = {field: report[field] for field in fields if field in report}
            if isinstance(summary.get("passed"), list): summary["passed"] = len(summary["passed"])
            print(name, json.dumps(summary, ensure_ascii=False), "report=" + str(path))
            if name in ("performance","soak","compare") and report.get("cases"):
                cases=report["cases"]
                emitted=sum(case.get("http_requests",0) for case in cases)
                succeeded=sum(case.get("successful",0) for case in cases)
                dropped=sum(case.get("dropped_iterations",0) for case in cases)
                ledger_ok=all(case.get("checks",{}).get("ledger_exact",False) for case in cases)
                print(f"  已发出={emitted} | 成功={succeeded} | 调用失败(含超时)={emitted-succeeded} | 未发出迭代={dropped} | 账本一致={ledger_ok}")
                if any("no_unresolved_reservations" in case.get("checks",{}) for case in cases):
                    held=sum(row["reserved"] for case in cases for row in case.get("ledger",[]))
                    unresolved=sum(row.get("unresolved_requests",0) for case in cases for row in case.get("ledger",[]))
                    pending_receipts=sum(case.get("settlement_recovery",{}).get("receipt_files",0) for case in cases)
                    print(f"  预占Token={held} | 未结清请求={unresolved} | 待重放回执={pending_receipts}")
                if name=="soak":
                    for case in cases:
                        print(f"  P95={case.get('p95_ms',0):.2f}ms (目标<1000ms) | P99={case.get('p99_ms',0):.2f}ms (目标<2000ms)")
                        if any(case.get("protection_rejections",{}).values()):
                            print("  入口/数据库预算拒绝:",json.dumps(case["protection_rejections"],ensure_ascii=False))
                if name=="compare":
                    for case in cases:print(f"  {case['name']} | {case['state']} | 提交/请求={case.get('request_commits_per_call')} | P95={case.get('p95_ms',0):.2f}ms | 丢弃={case.get('dropped_iterations',0)}")
            if name=="benchmark" and report.get("scores"):
                for dataset,arms in report["scores"].items():
                    print("  "+dataset+" | "+" | ".join(f"{arm}: 已评{value['completed']}/{value['total']}, 正确{value['correct']}"+(f" ({value['accuracy_pct']}%)" if value['accuracy_pct'] is not None else "") for arm,value in arms.items()))
                if report.get("current") and report.get("state")=="running":print("  当前:",report["current"])
    print("自动重启: 关闭；故障验收仅控制 llmgw-* 自有服务")
    print("API http://127.0.0.1:29480/v1 | runtime", ROOT)


def dispatch(action, duration=60, rps=10, concurrency=16, accounts=4, samples=20, seed=20261113, admission_mode="combined",request_id_version=None,comparison_axis="admission"):
    if action in ("verify", "stability", "deepen", "perf", "soak", "benchmark", "compare", "tools", "real-load"):
        lock_path=ROOT/"run/test-launch.lock"
        with lock_path.open("a") as lock:
            fcntl.flock(lock,fcntl.LOCK_EX)
            busy=[name for name in RUNNERS if ctl("show",UNITS[name],"-p","ActiveState","--value").stdout.strip() in ("active","activating","deactivating","reloading")]
            if busy: raise RuntimeError("Another experiment is active: "+", ".join(busy))
            return launch_experiment(action,duration,rps,concurrency,accounts,samples,seed,admission_mode,request_id_version,comparison_axis)
    if action == "setup": setup()
    elif action == "init": init()
    elif action == "start": start()
    elif action == "stop": stop(["real-load", "tools", "compare", "benchmark", "soak", "perf", "deep", "stability", "verify", "lab-a", "lab-b", "peer", "api", "fixture", "redis", "mysql", "bootstrap"])
    elif action == "status": status()


def launch_experiment(action, duration, rps, concurrency, accounts, samples=20, seed=20261113, admission_mode="combined",request_id_version=None,comparison_axis="admission"):
    if action == "verify":
        start(peer=True, fixture=True)
        launch("verify", ["/home/zjr/anaconda3/bin/python", "-B", SOURCE / "scripts/acceptance.py"], "512M", "200%")
    elif action == "stability":
        start(fixture=True)
        launch("stability", ["/home/zjr/anaconda3/bin/python", "-B", SOURCE / "scripts/stability.py", "--duration", str(duration),
                             "--rps", str(rps), "--concurrency", str(concurrency), "--accounts", str(accounts)], "512M", "200%")
    elif action == "benchmark":
        if not 1<=samples<=100:raise ValueError("Use 1..100 samples per dataset")
        start()
        launch("benchmark",["/home/zjr/anaconda3/bin/python","-B",SOURCE/"scripts/benchmark.py","--samples",str(samples),"--seed",str(seed)],"512M","100%")
    elif action in ("tools", "real-load"):
        if not 3<=samples<=100:raise ValueError("Use 3..100 samples")
        start()
        script = "tool_benchmark.py" if action == "tools" else "real_load.py"
        launch(action,["/home/zjr/anaconda3/bin/python","-B",SOURCE/"scripts"/script,"--samples",str(samples),"--seed",str(seed)],"3G" if action == "real-load" else "512M","100%")
    elif action in ("deepen", "perf", "soak", "compare"):
        stop(["lab-a", "lab-b"])
        start_lab(race=action=="deepen", both=action=="deepen",admission_mode=admission_mode,request_id_version=request_id_version)
        if action=="deepen":
            launch("deep", ["/home/zjr/anaconda3/bin/python", "-B", SOURCE / "scripts/deep_checks.py"], "768M", "200%")
        else:
            launch(action, ["/home/zjr/anaconda3/bin/python", "-B", SOURCE / "scripts/performance.py", "--mode", action,
                   "--duration",str(duration),"--rps",str(rps),"--accounts",str(accounts),"--vus",str(concurrency),"--admission-mode",admission_mode,"--compare-axis",comparison_axis], "768M", "200%")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=["setup", "init", "start", "stop", "status", "verify", "stability", "deepen", "perf", "soak", "benchmark", "compare", "tools", "real-load"])
    parser.add_argument("--duration", type=int, default=60)
    parser.add_argument("--rps", type=int, default=10)
    parser.add_argument("--concurrency", type=int, default=16)
    parser.add_argument("--accounts", type=int, default=4)
    parser.add_argument("--samples", type=int, default=20)
    parser.add_argument("--seed", type=int, default=20261113)
    parser.add_argument("--admission-mode",choices=["split","combined"],default="combined")
    parser.add_argument("--request-id-version",type=int,choices=[4,7],default=None,help="UUID strategy for isolated deep/perf/soak/compare instances")
    parser.add_argument("--compare-axis",choices=["admission","request-id"],default="admission")
    args = parser.parse_args()
    dispatch(args.action, args.duration, args.rps, args.concurrency, args.accounts, args.samples, args.seed, args.admission_mode,args.request_id_version,args.compare_axis)
