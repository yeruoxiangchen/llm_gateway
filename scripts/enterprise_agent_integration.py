"""Connect the running enterprise Agent and verify its real HTTP business flow.

Uses its existing model and native multi-agent implementation. Credentials stay
under /data with mode 0600; reports contain only test data and credential IDs.
"""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import http.cookiejar
import json
import os
from pathlib import Path
import secrets
import shlex
import sqlite3
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

import runtime

KB_SOURCE = runtime.SOURCE.parent / 'enterprise_kb_agent'
KB_ROOT = Path('/data/zjr/enterprise_kb_agent')
GATEWAY = 'http://127.0.0.1:29480'
APPLICATION = 'http://127.0.0.1:8768'


def write(path, value, private=False):
    path.parent.mkdir(parents=True, exist_ok=True)
    text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, indent=2)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600 if private else 0o644)
    with os.fdopen(fd, 'w') as handle:
        handle.write(text)
    if private:
        path.chmod(0o600)


def profile(path):
    values = {}
    for line in path.read_text().splitlines():
        words = shlex.split(line, comments=True)
        if words and words[0] == 'export':
            words = words[1:]
        for word in words:
            if '=' in word:
                key, value = word.split('=', 1)
                values[key] = value
    return values


class HTTP:
    def __init__(self, base, token=None, cookies=False):
        self.base, self.token = base, token
        self.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}),
            *([urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar())] if cookies else []))

    def call(self, method, path, value=None, raw=None, content_type=None, expected=None):
        headers = {'X-KB-Client': 'web'}
        if self.token:
            headers['Authorization'] = 'Bearer ' + self.token
        if value is not None:
            raw = json.dumps(value, ensure_ascii=False).encode()
            content_type = 'application/json'
        if content_type:
            headers['Content-Type'] = content_type
        req = urllib.request.Request(self.base + path, data=raw, headers=headers, method=method)
        try:
            with self.opener.open(req, timeout=20) as response:
                status, data = response.status, response.read()
        except urllib.error.HTTPError as exc:
            status, data = exc.code, exc.read()
        if (expected is not None and status != expected) or (expected is None and status >= 400):
            # Authentication values and raw upstream responses stay out of logs.
            raise RuntimeError(f'{method} {path.split("?")[0]} returned HTTP {status}')
        return json.loads(data) if data else {}


def active_runs():
    with sqlite3.connect(f'file:{KB_ROOT}/knowledge.sqlite3?mode=ro', uri=True) as db:
        return db.execute("SELECT count(*) FROM runs WHERE status IN ('queued','running')").fetchone()[0]


def restart_application(folder):
    for action in ('stop', 'start'):
        with (folder / 'application-reload.log').open('a') as log:
            subprocess.run(['bash', str(KB_SOURCE / 'scripts/run.sh'), action],
                cwd=KB_SOURCE, stdout=log, stderr=subprocess.STDOUT, check=True)


def connect(folder, admin):
    config = KB_ROOT / 'config/gateway-client.json'
    if config.exists():
        state = json.loads(config.read_text())
        current = profile(KB_ROOT / 'app.env')
        if current.get('KB_LLM_BASE_URL') != GATEWAY + '/v1' or current.get('KB_LLM_API_KEY') != state['api_key']:
            raise RuntimeError('Saved gateway client differs from current app.env; inspect before reconnecting')
        return state
    if active_runs():
        raise RuntimeError('Application has queued/running requests; configuration was not changed')
    app_path = KB_ROOT / 'app.env'
    original = app_path.read_text()
    before_hash = hashlib.sha256(original.encode()).hexdigest()
    effective = {}
    for name in ('model.env', 'private.env', 'judge.env', 'app.env'):
        path = KB_ROOT / name
        if path.exists():
            effective.update(profile(path))
    if effective.get('KB_AGENT_POLICY') != 'business_native_v1':
        raise RuntimeError('The live Agent policy changed; inspect its current execution core')
    upstream_url, model = effective['KB_LLM_BASE_URL'], effective['KB_LLM_MODEL']
    if upstream_url == GATEWAY + '/v1':
        raise RuntimeError('Application is already proxied without a saved client record')
    matches = [p for p in admin.call('GET', '/admin/providers')['data'] if p['model_alias'] == model and p['enabled']]
    if matches:
        raise RuntimeError('Model alias already has enabled channels; inspect routing before provisioning')
    account = admin.call('POST', '/admin/accounts', {'name': 'enterprise-kb-agent', 'quota_tokens': 1000000})
    issued = admin.call('POST', f"/admin/accounts/{account['id']}/keys",
                        {'name': 'enterprise-server', 'requests_per_minute': 120})
    provider = admin.call('POST', '/admin/providers', {'name': 'enterprise-kb-agent', 'model_alias': model,
        'upstream_model': model, 'base_url': upstream_url, 'api_key': effective.get('KB_LLM_API_KEY', ''),
        'priority': 10, 'enabled': True, 'input_micro_usd_per_million_tokens': 0,
        'output_micro_usd_per_million_tokens': 0})
    state = {'account_id': account['id'], 'key_id': issued['key']['id'], 'api_key': issued['secret'],
        'provider_id': provider['id'], 'model': model, 'upstream_url': upstream_url,
        'gateway_url': GATEWAY + '/v1', 'agent_policy': effective['KB_AGENT_POLICY']}
    write(config, state, private=True)
    write(folder / 'app.env.before', original, private=True)
    updates = {'KB_LLM_BASE_URL': GATEWAY + '/v1', 'KB_LLM_API_KEY': state['api_key'],
        'KB_TOKENIZE_API_KEY': effective.get('KB_TOKENIZE_API_KEY', effective.get('KB_LLM_API_KEY', ''))}
    lines, found = [], set()
    for line in original.splitlines():
        words = shlex.split(line, comments=True)
        token = words[1] if words and words[0] == 'export' and len(words)>1 else words[0] if words else ''
        key = token.split('=', 1)[0]
        if key in updates:
            lines.append(f'export {key}={shlex.quote(updates[key])}')
            found.add(key)
        else:
            lines.append(line)
    lines += [f'export {key}={shlex.quote(value)}' for key, value in updates.items() if key not in found]
    if active_runs() or hashlib.sha256(app_path.read_bytes()).hexdigest() != before_hash:
        raise RuntimeError('Application activity/configuration changed during setup; no profile update applied')
    temporary = app_path.with_name('app.env.gateway-' + str(os.getpid()))
    write(temporary, '\n'.join(lines) + '\n', private=True)
    os.replace(temporary, app_path)
    try:
        restart_application(folder)
    except BaseException:
        write(app_path, original, private=True)
        restart_application(folder)
        print('Application reload failed; restored its previous direct-model configuration', flush=True)
        raise
    return state


def upload_document(client, base):
    boundary = 'gw-' + uuid.uuid4().hex
    text = ('网关联调测试资料。Windows 11 上 VPN 返回 HTTP 401 Authentication failed 时，'
        '先检查账号是否过期，再续期认证并重新登录。若仍失败，记录操作系统、错误码和故障现象，'
        '由技术支持处理。禁止公开密码、Token 或访问密钥。')
    raw = (f'--{boundary}\r\nContent-Disposition: form-data; name="base_id"\r\n\r\n{base}\r\n'
        f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="VPN_gateway_check.md"\r\n'
        f'Content-Type: text/markdown\r\n\r\n{text}\r\n--{boundary}--\r\n').encode()
    return client.call('POST', '/api/documents', raw=raw, content_type='multipart/form-data; boundary=' + boundary)


def wait_run(client, ident, folder):
    deadline = time.monotonic() + 600
    while time.monotonic() < deadline:
        run = client.call('GET', '/api/runs/' + ident)
        if run['status'] not in ('queued', 'running'):
            write(folder / (ident + '.json'), run)
            return run
        time.sleep(2)
    client.call('POST', '/api/runs/' + ident + '/cancel')
    raise RuntimeError('Integration task exceeded 10 minutes; cancellation requested')


def verify(folder, state, admin):
    started = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')
    account_before = admin.call('GET', '/admin/accounts/' + state['account_id'])
    credentials = (KB_ROOT / 'credentials.txt').read_text().splitlines()
    login = {line.split('：', 1)[0]: line.split('：', 1)[1] for line in credentials if '：' in line}
    manager, member = HTTP(APPLICATION, cookies=True), HTTP(APPLICATION, cookies=True)
    manager.call('POST', '/api/login', {'username': login['账号'], 'password': login['密码']})
    username, password = 'gwcheck_' + uuid.uuid4().hex[:10], secrets.token_urlsafe(24)
    manager.call('POST', '/api/users', {'username': username, 'password': password, 'role': 'member'})
    member.call('POST', '/api/login', {'username': username, 'password': password})
    me = member.call('GET', '/api/me')
    if me['agent_policy'] != state['agent_policy'] or me['model'] != state['model']:
        raise RuntimeError('Live application changed model or Agent policy')
    base = manager.call('POST', '/api/bases', {'name': '网关联调VPN资料-' + username,
                        'description': '独立联调测试资料', 'visibility': 'all', 'domain': 'technical'})['id']
    document = upload_document(manager, base)
    for _ in range(60):
        current = manager.call('GET', '/api/documents/' + document['id'])
        if current['status'] == 'ready': break
        if current['status'] == 'error': raise RuntimeError('Test document ingestion failed')
        time.sleep(1)
    else: raise RuntimeError('Test document did not become ready')
    question = ('这是一项网关联调测试：我在 Windows 11 上登录 VPN，出现 HTTP 401 Authentication failed。'
        '请先检索所选知识库，说明处理建议并引用来源；然后创建标题为“网关联调-VPN登录故障”的技术支持工单草稿，'
        '描述写明操作系统、错误码和现象，等待我本人确认提交。')
    submitted = member.call('POST', '/api/chat', {'question': question, 'base_ids': [base], 'knowledge_grounded': True})
    print(json.dumps({'stage': 'business_task_started', 'run_id': submitted['run_id']}, ensure_ascii=False), flush=True)
    run = wait_run(member, submitted['run_id'], folder)
    if run['status'] != 'answered' or not run['citations']:
        raise RuntimeError('Knowledge + draft task did not finish with a cited answer; see saved run')
    tickets = member.call('GET', '/api/tickets')
    tickets = [row for row in tickets if row['source_run'] == run['id']]
    if len(tickets) != 1 or tickets[0]['status'] != 'draft':
        raise RuntimeError('Expected exactly one unsubmitted draft')
    ticket = tickets[0]
    if not all(text in ticket['description'] for text in ('VPN', '401', 'Windows')):
        raise RuntimeError('Draft description lost user-provided facts')
    ticket = member.call('POST', '/api/tickets/' + ticket['id'] + '/action', {'action': 'submit', 'version': ticket['version']})
    ticket = manager.call('POST', '/api/tickets/' + ticket['id'] + '/action', {'action': 'accept', 'version': ticket['version']})
    ticket = manager.call('POST', '/api/tickets/' + ticket['id'] + '/action', {'action': 'resolve', 'version': ticket['version'],
                        'note': '网关联调验证完成，此为测试工单。'})
    follow = member.call('POST', '/api/chat', {'question': '查询刚才这个测试工单的当前状态和处理备注。',
        'conversation_id': submitted['conversation_id'], 'base_ids': [base]})
    follow_run = wait_run(member, follow['run_id'], folder)
    if follow_run['status'] != 'answered' or not follow_run.get('answer'):
        raise RuntimeError('Ticket status follow-up failed')
    with sqlite3.connect(f'file:{KB_ROOT}/knowledge.sqlite3?mode=ro', uri=True) as db:
        db.row_factory = sqlite3.Row
        raw_runs = [dict(db.execute('SELECT id,state,answer,status FROM runs WHERE id=?', (ident,)).fetchone())
                    for ident in (run['id'], follow_run['id'])]
        events = [dict(row) for row in db.execute('SELECT run_id,kind,payload FROM events WHERE run_id IN (?,?) ORDER BY id',
                    (run['id'], follow_run['id']))]
        for row in events: row['payload'] = json.loads(row['payload'])
    delegations = [row for row in events if row['kind'] == 'delegation_bound']
    if not delegations: raise RuntimeError('Task did not use the current multi-agent delegation path')
    request_query = urllib.parse.urlencode({'account_id': state['account_id'], 'since': started, 'limit': 200})
    requests = admin.call('GET', '/admin/requests?' + request_query)
    if requests['has_more']: raise RuntimeError('Unexpectedly exceeded the bounded request page')
    records = requests['data']
    account_after = admin.call('GET', '/admin/accounts/' + state['account_id'])
    usages = [json.loads(row['state'])['business']['usage'] for row in raw_runs]
    app_tokens = sum(u['prompt_tokens'] + u['completion_tokens'] for u in usages)
    gateway_tokens = sum(r['usage']['total_tokens'] for r in records if r.get('usage'))
    checks = {'current_native_multi_agent': me['agent_policy'] == 'business_native_v1',
        'actual_delegation': bool(delegations), 'cited_answer': bool(run['citations']),
        'user_confirmation_then_admin_resolution': ticket['status'] == 'resolved',
        'followup_answered': follow_run['status'] == 'answered',
        'model_calls_recorded': bool(records), 'one_configured_upstream': all(r.get('provider_id') == state['provider_id'] for r in records),
        'requests_completed': all(r['state'] == 'settled' and r['outcome'] == 'succeeded' for r in records),
        'complete_application_usage': all(u['usage_complete'] for u in usages),
        'token_usage_matches': app_tokens == gateway_tokens == account_after['used_tokens'] - account_before['used_tokens'],
        'no_held_quota': account_after['reserved_tokens'] == 0}
    report = {'state': 'passed' if all(checks.values()) else 'failed', 'checks': checks,
        'scope': 'Real deployed application + native multi-agent + existing Qwen3-8B + Go gateway; dedicated test document and test ticket',
        'application_url': APPLICATION, 'gateway_url': GATEWAY, 'model': state['model'],
        'agent_policy': me['agent_policy'], 'account_id': state['account_id'], 'provider_id': state['provider_id'],
        'run_ids': [run['id'], follow_run['id']], 'ticket_id': ticket['id'], 'base_id': base,
        'gateway_requests': records, 'application_usage': usages, 'application_tokens': app_tokens,
        'gateway_tokens': gateway_tokens, 'account_used_delta': account_after['used_tokens'] - account_before['used_tokens'],
        'held_tokens': account_after['reserved_tokens'], 'delegations': delegations,
        'answer': run['answer'], 'followup_answer': follow_run['answer']}
    write(folder / 'report.json', report)
    write(folder / 'application-events.json', events)
    write(folder / 'report.md', '# 企业多 Agent 接入网关验证\n\n'
        + '\n'.join(f'- {key}: {value}' for key, value in checks.items())
        + f'\n\n应用/网关/账户 Token：{app_tokens} / {gateway_tokens} / {report["account_used_delta"]}\n'
        + '\n初次回答：\n' + run['answer'] + '\n\n状态追问：\n' + follow_run['answer'] + '\n')
    print(json.dumps({key: report[key] for key in ('state', 'checks', 'run_ids', 'application_tokens', 'gateway_tokens')}, ensure_ascii=False), flush=True)
    if not all(checks.values()): raise RuntimeError('Integration result failed one or more checks; see report.json')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--output', required=True)
    args = parser.parse_args()
    folder = Path(args.output).resolve()
    if not folder.is_relative_to(runtime.ROOT / 'checks'): parser.error('Use a gateway checks directory under /data')
    folder.mkdir(parents=True, exist_ok=True)
    if (folder / 'started.json').exists(): parser.error('Use a fresh output directory; failed runs are retained')
    write(folder / 'started.json', {'pid': os.getpid(), 'started_at': time.time()})
    try:
        admin = HTTP(GATEWAY, runtime.read_env()['LLMGW_ADMIN_TOKEN'])
        state = connect(folder, admin)
        write(folder / 'connection.json', {key: value for key, value in state.items() if key != 'api_key'})
        verify(folder, state, admin)
    except BaseException as exc:
        write(folder / 'failure.json', {'type': type(exc).__name__, 'error': str(exc), 'time': time.time()})
        raise


if __name__ == '__main__':
    main()
