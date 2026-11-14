"""Small standard-library client shared by demos and black-box acceptance."""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.request
import uuid

import runtime

BASE = "http://127.0.0.1:29480"
PEER = "http://127.0.0.1:29482"
FIXTURE = "http://127.0.0.1:29481"
HTTP = urllib.request.build_opener(urllib.request.ProxyHandler({}))


class API:
    def __init__(self, base=BASE):
        self.admin = runtime.read_env()["LLMGW_ADMIN_TOKEN"]
        self.base = base
        self.prefix = uuid.uuid4().hex[:10]
        self.accounts = []
        self.provider_ids = []
        self.key_ids = []

    def call(self, method, path, data=None, key=None, admin=False, base=None, headers=None, timeout=20):
        request_headers = {"Content-Type": "application/json", **(headers or {})}
        if admin or key:
            request_headers["Authorization"] = "Bearer " + (self.admin if admin else key)
        payload = json.dumps(data, ensure_ascii=False).encode() if data is not None else None
        request = urllib.request.Request((base or self.base) + path, data=payload, headers=request_headers, method=method)
        try:
            response = HTTP.open(request, timeout=timeout)
        except urllib.error.HTTPError as exc:
            response = exc
        with response:
            return response.status, {k.lower(): v for k, v in response.headers.items()}, response.read()

    def json(self, method, path, data=None, expected=200, **kwargs):
        status, headers, raw = self.call(method, path, data, **kwargs)
        if status != expected:
            raise AssertionError(f"{method} {path}: expected {expected}, got {status}: {raw[:400]!r}")
        return json.loads(raw), headers

    def account(self, name, quota=1000000, rpm=100000):
        account, _ = self.json("POST", "/admin/accounts", {"name": self.prefix + "-" + name, "quota_tokens": quota}, admin=True, expected=201)
        credential, _ = self.json("POST", f"/admin/accounts/{account['id']}/keys", {"name": name, "requests_per_minute": rpm}, admin=True, expected=201)
        self.accounts.append(account["id"])
        self.key_ids.append(credential["key"]["id"])
        return account, credential["secret"], credential["key"]

    def provider(self, name, scenario="normal", alias=None, priority=0, **changes):
        body = {"name": self.prefix + "-" + name, "model_alias": alias or self.prefix + "-" + name,
                "upstream_model": "fixture-model", "base_url": FIXTURE + "/" + scenario + "/v1",
                "api_key": "fixture-secret", "priority": priority, "enabled": True,
                "input_micro_usd_per_million_tokens": 1000000,
                "output_micro_usd_per_million_tokens": 2000000, **changes}
        provider, _ = self.json("POST", "/admin/providers", body, admin=True, expected=201)
        self.provider_ids.append(provider["id"])
        return provider

    def cleanup(self):
        """Disable this test run's routes/keys while retaining its audit ledger."""
        for identity in self.provider_ids:
            self.json("PATCH", "/admin/providers/"+identity, {"enabled": False}, admin=True)
        for identity in self.key_ids:
            code, _, _ = self.call("DELETE", "/admin/keys/"+identity, admin=True)
            if code not in (200, 404):
                raise RuntimeError(f"could not revoke test key {identity}: {code}")

    @staticmethod
    def body(alias, stream=False, **changes):
        return {"model": alias, "messages": [{"role": "user", "content": "hello"}], "max_tokens": 16, "stream": stream, **changes}

    def chat(self, key, alias, stream=False, idem=None, base=None, **changes):
        headers = {"Idempotency-Key": idem} if idem is not None else {}
        return self.call("POST", "/v1/chat/completions", self.body(alias, stream, **changes), key=key, base=base, headers=headers)

    def stream(self, key, alias, base=None, idem=None):
        headers = {"Content-Type": "application/json", "Authorization": "Bearer " + key}
        if idem is not None:
            headers["Idempotency-Key"] = idem
        request = urllib.request.Request((base or self.base) + "/v1/chat/completions", data=json.dumps(self.body(alias, True)).encode(), headers=headers)
        return HTTP.open(request, timeout=25)

    def receipt(self, request_id, key):
        return self.json("GET", "/v1/requests/" + request_id, key=key)[0]

    def usage(self, key):
        return self.json("GET", "/v1/usage", key=key)[0]

    def refund(self, request_id):
        return self.json("POST", f"/admin/requests/{request_id}/resolve", {"refund": True, "reason": "controlled fault acceptance: no billable delivery"}, admin=True)[0]


def eventually(check, timeout=15, message="condition did not become true"):
    end = time.monotonic() + timeout
    while time.monotonic() < end:
        value = check()
        if value:
            return value
        time.sleep(0.15)
    raise AssertionError(message)


def stats():
    with HTTP.open(FIXTURE + "/stats", timeout=3) as response:
        return json.load(response)


def release():
    with HTTP.open(urllib.request.Request(FIXTURE + "/release", data=b"", method="POST"), timeout=3) as response:
        return json.load(response)


def first_event(response):
    lines = []
    while True:
        line = response.readline()
        if not line:
            raise AssertionError("stream ended before its first event")
        lines.append(line)
        if line == b"\n":
            return b"".join(lines)


def ledger_snapshot(account_ids, database="llm_gateway", with_requests=False, validate=True):
    if database not in ("llm_gateway", "llm_gateway_lab"):
        raise ValueError("Unknown test database")
    for identity in account_ids:
        uuid.UUID(identity)
    if not account_ids:return []
    where = ",".join("'" + value + "'" for value in account_ids)
    extra_columns=",COALESCE(q.tokens,0),COALESCE(q.unresolved,0)" if with_requests else ""
    extra_join=(f"LEFT JOIN (SELECT account_id,SUM(total_tokens) tokens,SUM(state NOT IN ('settled','released')) unresolved "
                f"FROM requests FORCE INDEX(requests_account) WHERE account_id IN ({where}) GROUP BY account_id) q ON q.account_id=a.id") if with_requests else ""
    raw = runtime.sql(f"""USE {database};
SELECT a.id,a.used_tokens,a.reserved_tokens,COALESCE(l.used,0),COALESCE(l.reserved,0){extra_columns}
FROM accounts a LEFT JOIN
(SELECT r.account_id,SUM(e.used_delta) used,SUM(e.reserved_delta) reserved
 FROM requests r FORCE INDEX(requests_account) STRAIGHT_JOIN ledger e ON e.request_id=r.id
 WHERE r.account_id IN ({where}) GROUP BY r.account_id) l
ON a.id=l.account_id {extra_join} WHERE a.id IN ({where});
""")
    rows = []
    for line in raw.splitlines():
        fields=line.split("\t")
        identity, used, reserved, ledger_used, ledger_reserved = fields[:5]
        row = {"account_id": identity, "used": int(used), "reserved": int(reserved), "ledger_used": int(ledger_used), "ledger_reserved": int(ledger_reserved)}
        if with_requests:row.update(request_tokens=int(fields[5]),unresolved_requests=int(fields[6]))
        if validate and (row["used"] != row["ledger_used"] or row["reserved"] != row["ledger_reserved"] or row["reserved"] < 0):
            raise AssertionError("account/ledger drift: " + repr(row))
        rows.append(row)
    if len(rows) != len(account_ids):
        raise AssertionError("account snapshot omitted accounts")
    return rows
