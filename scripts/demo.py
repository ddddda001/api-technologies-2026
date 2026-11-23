import json
import os
import re
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


HORIZONS = ('1d', '1m', '6m', '1y')
CHANNELS = ('email', 'push')
METRICS = ('cpuPercent', 'ramPercent', 'diskPercent')
SERVER_ID = re.compile(r'\bsrv-[a-zA-Z0-9]+\b')


def load_environment():
    path = Path('.env')
    if not path.is_file():
        return
    for line in path.read_text().splitlines():
        key, separator, value = line.partition('=')
        key = key.strip()
        if not separator or not key or key.startswith('#'):
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
            value = value[1:-1]
        os.environ.setdefault(key, value)


def redact(value):
    if isinstance(value, dict):
        return {
            key: '<redacted>' if key in {'accessToken', 'password', 'token'} else redact(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [redact(item) for item in value]
    return value


def call(method, url, headers=None, *, payload=None, params=None, quiet=False):
    if params:
        url += '?' + urlencode(params)
    request_headers = dict(headers or {})
    body = None
    if payload is not None:
        request_headers['Content-Type'] = 'application/json'
        body = json.dumps(payload).encode()
    request = Request(url, method=method, headers=request_headers, data=body)
    if not quiet:
        print(f'{method} {url}', flush=True)
    try:
        with urlopen(request, timeout=6) as response:
            data = json.loads(response.read())
            if not quiet:
                print(json.dumps(redact(data), ensure_ascii=False), flush=True)
            return data
    except HTTPError as error:
        print(f'HTTP {error.code} {error.read().decode(errors="replace")}', file=sys.stderr)
        raise


def wait_for(check, description, timeout=180, interval=3):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if result := check():
                return result
        except (URLError, TimeoutError, ConnectionError):
            pass
        time.sleep(interval)
    raise RuntimeError(f'Timed out waiting for {description}')


def inbox_snapshot(notify, headers):
    data = call('GET', f'{notify}/inbox', headers, params={'limit': 1000}, quiet=True)
    messages = {}
    counts = dict.fromkeys(CHANNELS, 0)
    for item in data['items']:
        body = item.get('body') or ''
        match = SERVER_ID.search(body)
        if '[alert=' in body and match and item['channel'] in CHANNELS:
            messages.setdefault(match[0], set()).add(item['channel'])
            counts[item['channel']] += 1
    print(f'GET {notify}/inbox → {counts}', flush=True)
    return messages, counts


def main():
    load_environment()
    api = os.getenv('DEMO_API_URL', f"http://127.0.0.1:{os.getenv('API_PORT', '8080')}").rstrip('/')
    notify = os.getenv('NOTIFY_URL', 'https://api.external-services.dataprosvet.ru/notify').rstrip('/')
    result = {'startedAt': datetime.now(timezone.utc).isoformat(), 'apiUrl': api, 'notificationUrl': notify}
    try:
        notify_headers = {'Authorization': f"Bearer {os.environ['API_KEY']}"}
        if scenario := os.getenv('MOCK_SCENARIO'):
            notify_headers['X-Mock-Scenario'] = scenario
        wait_for(lambda: call('GET', f'{api}/health', quiet=True), 'API health')
        credentials = {
            'email': os.getenv('API_LOGIN_EMAIL', 'engineer@example.com'),
            'password': os.environ['API_LOGIN_PASSWORD'],
        }
        token = call('POST', f'{api}/v1/auth/login', payload=credentials)['accessToken']
        headers = {'Authorization': f'Bearer {token}'}

        def read(path, params=None, quiet=False):
            return call('GET', f'{api}/v1{path}', headers, params=params, quiet=quiet)

        servers = read('/servers')
        server_ids = [server['id'] for server in servers]
        assert len(server_ids) == 40, f'Expected 40 monitored servers, got {len(server_ids)}'
        result['servers'] = len(server_ids)

        def fresh_states():
            states = [read(f'/servers/{sid}/current-state', quiet=True) for sid in server_ids]
            return states if sum(not state['stale'] for state in states) >= 38 else None

        wait_for(fresh_states, 'current metrics')
        states = [read(f'/servers/{sid}/current-state') for sid in server_ids]
        result['freshServers'] = sum(not state['stale'] for state in states)
        result['staleServers'] = sorted(state['serverId'] for state in states if state['stale'])
        assert result['staleServers'] == ['srv-013', 'srv-034'], result['staleServers']

        end = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        start = end - timedelta(days=30)
        window = {'from': start.isoformat(), 'to': end.isoformat(), 'resolution': 'daily'}
        history = read('/servers/srv-001/metrics', window)
        assert len(history['points']) == 30
        result['historySrv001'] = {'points': len(history['points']), 'first': history['points'][0], 'last': history['points'][-1]}

        def forecasts_ready():
            return all(read('/servers/srv-023/forecasts', {'horizon': horizon}, quiet=True)['status'] == 'ready' for horizon in HORIZONS)

        wait_for(forecasts_ready, 'four forecasting horizons')
        forecasts = [read('/servers/srv-023/forecasts', {'horizon': horizon}) for horizon in HORIZONS]
        result['forecastsSrv023'] = forecasts

        thresholds = dict.fromkeys(METRICS, 80)
        expected = set()
        for sid in server_ids:
            for horizon in ('1d', '1m'):
                prediction = read(f'/servers/{sid}/forecasts', {'horizon': horizon}, quiet=True)
                if prediction['status'] == 'ready' and any(prediction[metric] >= 80 for metric in METRICS):
                    expected.add(sid)
            call('PUT', f'{api}/v1/servers/{sid}/thresholds', headers, payload=thresholds)
        result['expectedAlertServers'] = sorted(expected)

        def all_alerts():
            messages, counts = inbox_snapshot(notify, notify_headers)
            return (messages, counts) if all(messages.get(sid) == set(CHANNELS) for sid in expected) else None

        messages, counts = wait_for(all_alerts, 'email and push alerts')
        result['notifiedServers'] = sorted(messages)
        assert set(messages) == expected, f'Unexpected alert servers: {sorted(set(messages) - expected)}'
        result['notifications'] = counts
        time.sleep(float(os.getenv('DEMO_ALERT_RECHECK_SECONDS', '18')))
        _, next_counts = inbox_snapshot(notify, notify_headers)
        result['notificationsAfterRecheck'] = next_counts
        assert next_counts == counts, 'Notifications were duplicated in a later worker cycle'
        result['ok'] = True
        return 0
    except (AssertionError, KeyError, RuntimeError, ValueError, OSError) as error:
        result['ok'] = False
        result['error'] = str(error)
        print(str(error), file=sys.stderr)
        return 1
    finally:
        result['finishedAt'] = datetime.now(timezone.utc).isoformat()
        Path('demo-result.json').write_text(json.dumps(result, indent=2, ensure_ascii=False) + '\n')


if __name__ == '__main__':
    raise SystemExit(main())
