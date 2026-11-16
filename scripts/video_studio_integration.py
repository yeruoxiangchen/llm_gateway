"""Connect the video workstation and reconcile real model-call receipts.

Existing browser journeys exercise editing and localization. Credentials and
configuration backups remain private under the owning projects' data roots.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
from urllib.parse import urlsplit, urlunsplit

from dotenv import dotenv_values, set_key

from enterprise_agent_integration import HTTP, write
import runtime


SOURCE = runtime.SOURCE.parent / "camcat_studio"
ROOT = Path("/data/zjr/camcat_studio")
GATEWAY = "http://127.0.0.1:29480"
CONNECTION = ROOT / "config/go-gateway-client.json"


def admin_client():
    return HTTP(GATEWAY, runtime.read_env()["LLMGW_ADMIN_TOKEN"])


def check_idle():
    sql = (
        "SELECT count(*) FROM jobs WHERE status::text IN ('QUEUED','RUNNING') "
        "UNION ALL SELECT count(*) FROM graph_runs WHERE status::text IN ('running','RUNNING')"
    )
    result = subprocess.run(
        ["bash", "scripts/studio.sh", "compose", "exec", "-T", "postgres", "psql",
         "-U", "camcat", "-d", "camcat", "-Atc", sql],
        cwd=SOURCE, capture_output=True, text=True, check=True,
    )
    if any(int(row) for row in result.stdout.splitlines() if row.strip()):
        raise RuntimeError("Video workstation has active jobs; deployment was not changed")


def host_url(value):
    url = urlsplit(value)
    if url.hostname == "10.0.2.2":
        url = url._replace(netloc=f"127.0.0.1:{url.port}")
    return urlunsplit(url).rstrip("/") + "/v1"


def connect(folder):
    check_idle()
    if CONNECTION.exists():
        state = json.loads(CONNECTION.read_text())
    else:
        original = (SOURCE / ".env").read_text()
        existing = dict(dotenv_values(SOURCE / ".env"))
        if existing["CAMCAT_LLM_BASE_URL"].endswith(":29480"):
            raise RuntimeError("Gateway route exists without its saved connection record")
        admin = admin_client()
        account = admin.call("POST", "/admin/accounts", {
            "name": "video-workstation", "quota_tokens": 2000000,
        })
        issued = admin.call("POST", f"/admin/accounts/{account['id']}/keys", {
            "name": "video-workstation-server", "requests_per_minute": 120,
        })
        providers = []
        for role, alias in [("LLM", "video-studio-planning"),
                            ("TRANSLATION", "video-studio-translation")]:
            provider = admin.call("POST", "/admin/providers", {
                "name": alias, "model_alias": alias,
                "upstream_model": existing[f"CAMCAT_{role}_MODEL"],
                "base_url": host_url(existing[f"CAMCAT_{role}_BASE_URL"]),
                "api_key": existing.get(f"CAMCAT_{role}_API_KEY") or existing["CAMCAT_LLM_API_KEY"],
                "priority": 10, "enabled": True,
                "input_micro_usd_per_million_tokens": 0,
                "output_micro_usd_per_million_tokens": 0,
            })
            providers.append({"role": role, "id": provider["id"], "alias": alias,
                              "upstream_model": provider["upstream_model"]})
        state = {"account_id": account["id"], "key_id": issued["key"]["id"],
                 "api_key": issued["secret"], "providers": providers}
        write(folder / "application.env.before", original, private=True)
        write(CONNECTION, state, private=True)
    for role in ("LLM", "TRANSLATION"):
        provider = next(p for p in state["providers"] if p["role"] == role)
        for suffix, value in {"BASE_URL": "http://10.0.2.2:29480",
                              "API_KEY": state["api_key"], "MODEL": provider["alias"]}.items():
            set_key(SOURCE / ".env", f"CAMCAT_{role}_{suffix}", value)
    for name, value in {
        "CAMCAT_LLM_HEALTH_PATH": "/v1/models",
        "CAMCAT_LLM_MAX_OUTPUT_TOKENS": "4096",
        "CAMCAT_GATEWAY_RECEIPTS_DIR": "/var/lib/camcat/gateway-receipts",
    }.items():
        set_key(SOURCE / ".env", name, value)
    (SOURCE / ".env").chmod(0o600)
    public = {k: v for k, v in state.items() if k != "api_key"}
    write(folder / "connection.json", public)
    print(json.dumps({"state": "configured", **public}))


def verify(folder):
    state = json.loads(CONNECTION.read_text())
    admin = admin_client()
    requests = []
    path = f"/admin/requests?account_id={state['account_id']}&limit=100"
    while path:
        page = admin.call("GET", path)
        requests.extend(page["data"])
        cursor = page.get("next_cursor")
        path = (f"/admin/requests?account_id={state['account_id']}&limit=100&cursor={cursor}"
                if cursor else None)
    by_id = {row["id"]: row for row in requests}
    application = {}
    for path in (ROOT / "runtime/gateway-receipts").glob("*.json"):
        row = json.loads(path.read_text())
        if row["gateway_request_id"] in by_id:
            application[row["gateway_request_id"]] = row
    if not by_id or set(by_id) != set(application):
        raise RuntimeError("Application and gateway request IDs differ")
    for identity, row in by_id.items():
        if row.get("http_status") != 200 or row.get("usage") != application[identity]["usage"]:
            raise RuntimeError("A model response or token receipt differs")
    application_tokens = sum(row["usage"]["total_tokens"] for row in application.values())
    gateway_tokens = sum(row["usage"]["total_tokens"] for row in by_id.values())
    account = admin.call("GET", f"/admin/accounts/{state['account_id']}")
    if not (application_tokens == gateway_tokens == account["used_tokens"]
            and account["reserved_tokens"] == 0):
        raise RuntimeError("Application, gateway and account token counts differ")
    browser = json.loads((folder / "browser-report.json").read_text())
    stats = browser["stats"]
    if stats["unexpected"] or stats["skipped"] or stats["expected"] != 2:
        raise RuntimeError("Both real browser journeys must pass without skips")
    report = {
        "state": "passed", "account_id": state["account_id"], "calls": len(by_id),
        "application_tokens": application_tokens, "gateway_tokens": gateway_tokens,
        "account_tokens": account["used_tokens"], "reserved_tokens": account["reserved_tokens"],
        "request_ids_match": True, "per_request_usage_matches": True,
        "browser": stats, "requests": list(by_id.values()),
        "application_receipts": list(application.values()), "providers": state["providers"],
        "scope": "Editing planning and subtitle translation via Go gateway; specialized media services remain separate",
    }
    write(folder / "report.json", report)
    print(json.dumps({k: v for k, v in report.items()
                      if k not in {"requests", "application_receipts", "providers"}}))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=["connect", "verify"])
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=True)
    if args.phase == "connect":
        connect(args.output)
    else:
        verify(args.output)


if __name__ == "__main__":
    main()
