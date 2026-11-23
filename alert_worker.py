import logging
import os
import time
from datetime import timedelta, timezone
from email.utils import parsedate_to_datetime
from uuid import uuid4

import psycopg
import requests

from service_common import database, heartbeat, utc_now


log = logging.getLogger(__name__)
FIELDS = {
    'email': ('email_done', 'email_attempts', 'email_retry_at'),
    'push': ('push_done', 'push_attempts', 'push_retry_at'),
}


def breached_thresholds(conn):
    return conn.execute(
        '''
        SELECT f.server_id, f.horizon, m.metric, m.forecast, m.threshold
        FROM server_forecasts f
        JOIN servers s ON s.id = f.server_id AND s.deleted_at IS NULL
        JOIN server_thresholds t ON t.server_id = f.server_id
        CROSS JOIN LATERAL (VALUES
            ('cpu', f.cpu_percent, t.cpu_percent),
            ('ram', f.ram_percent, t.ram_percent),
            ('disk', f.disk_percent, t.disk_percent)
        ) AS m(metric, forecast, threshold)
        WHERE f.mode = 'peak' AND f.horizon IN ('1d', '1m') AND m.forecast >= m.threshold
        ''',
    ).fetchall()


def sync_events(conn):
    active = {(row['server_id'], row['horizon'], row['metric']): row for row in breached_thresholds(conn)}
    existing = {
        (row['server_id'], row['horizon'], row['metric']): row['id']
        for row in conn.execute('SELECT id, server_id, horizon, metric FROM alert_events WHERE resolved_at IS NULL')
    }
    for key, event in existing.items():
        if key not in active:
            conn.execute('UPDATE alert_events SET resolved_at = now() WHERE id = %s', (event,))
    for key, threshold in active.items():
        if key not in existing:
            conn.execute(
                '''
                INSERT INTO alert_events (id, server_id, horizon, metric, forecast, threshold)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT DO NOTHING
                ''',
                (uuid4(), *key, threshold['forecast'], threshold['threshold']),
            )
    conn.commit()


def notify_headers():
    headers = {'Authorization': f"Bearer {os.environ['API_KEY']}"}
    if scenario := os.getenv('MOCK_SCENARIO'):
        headers['X-Mock-Scenario'] = scenario
    return headers


def retry_delay(response, attempts):
    delay = min(300, 10 * 2 ** min(attempts, 5))
    if response is not None and (header := response.headers.get('Retry-After')):
        try:
            return max(delay, int(header))
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(header)
                return max(delay, (retry_at.astimezone(timezone.utc) - utc_now()).total_seconds())
            except (TypeError, ValueError, OverflowError):
                pass
    return delay


def verify_inbox(root, opened_at):
    response = requests.get(
        f'{root}/inbox', headers=notify_headers(),
        params={'since': opened_at.isoformat(), 'limit': 1000}, timeout=5,
    )
    response.raise_for_status()
    return response.json()['items']


def send_notification(root, event, channel):
    server = event['server_id']
    title = f"Capacity alert for {server}: {event['metric']} {event['horizon']}"
    event_id = str(event['id'])
    body = (
        f"{server} {event['metric']} {event['horizon']}: "
        f"{event['forecast']:.2f}% >= {event['threshold']:.2f}% [alert={event_id}]"
    )
    headers = {**notify_headers(), 'Idempotency-Key': f'{event_id}:{channel}'}
    if channel == 'email':
        payload = {
            'personalizations': [{'to': [{'email': os.getenv('ALERT_EMAIL', 'engineer@example.com')}]}],
            'from': {'email': 'capacity-forecast@example.com'},
            'subject': title, 'content': [{'type': 'text/plain', 'value': body}],
            'custom_args': {'alertId': event_id, 'serverId': server},
        }
        url = f'{root}/sendgrid/v3/mail/send'
    else:
        notification = {
            'notification': {'title': title, 'body': body},
            'data': {'alertId': event_id, 'serverId': server},
        }
        if token := os.getenv('ALERT_PUSH_TOKEN'):
            notification['token'] = token
        else:
            notification['topic'] = os.getenv('ALERT_PUSH_TOPIC', 'capacity-forecast')
        payload = {'message': notification}
        url = f'{root}/fcm/v1/projects/capacity-forecast/messages:send'

    try:
        response = requests.post(url, headers=headers, json=payload, timeout=5)
    except requests.RequestException as error:
        log.warning('Notification request failed for %s/%s: %s', event_id, channel, error)
        return False, None
    if response.status_code == (202 if channel == 'email' else 200):
        return True, response
    if channel == 'push' and response.status_code == 404 and 'UNREGISTERED' in response.text:
        log.info('Push token is unregistered for alert %s', event_id)
        return True, response
    log.warning('Notification %s/%s returned HTTP %s', event_id, channel, response.status_code)
    return False, response


def send_pending(conn, root):
    rows = conn.execute(
        '''
        SELECT e.* FROM alert_events e
        JOIN servers s ON s.id = e.server_id AND s.deleted_at IS NULL
        WHERE e.resolved_at IS NULL
          AND ((NOT email_done AND (email_retry_at IS NULL OR email_retry_at <= now()))
            OR (NOT push_done AND (push_retry_at IS NULL OR push_retry_at <= now())))
        ORDER BY opened_at
        ''',
    ).fetchall()
    if not rows:
        conn.commit()
        return
    try:
        inbox = verify_inbox(root, rows[0]['opened_at'] - timedelta(seconds=1))
    except (requests.RequestException, ValueError, KeyError) as error:
        log.warning('Could not check the notification inbox: %s', error)
        conn.commit()
        return
    for event in rows:
        marker = f"[alert={event['id']}]"
        for channel, (done, attempts, retry_at) in FIELDS.items():
            if event[done] or (event[retry_at] and event[retry_at] > utc_now()):
                continue
            delivered = any(item['channel'] == channel and marker in (item.get('body') or '') for item in inbox)
            response = None
            if not delivered:
                delivered, response = send_notification(root, event, channel)
            if delivered:
                conn.execute(f'UPDATE alert_events SET {done} = true, {retry_at} = NULL WHERE id = %s', (event['id'],))
            else:
                delay = retry_delay(response, event[attempts])
                conn.execute(
                    f'UPDATE alert_events SET {attempts} = {attempts} + 1, {retry_at} = %s WHERE id = %s',
                    (utc_now() + timedelta(seconds=delay), event['id']),
                )
            conn.commit()


def run_once():
    root = os.getenv('NOTIFY_URL', 'https://api.external-services.dataprosvet.ru/notify').rstrip('/')
    with database() as conn:
        locked = conn.execute('SELECT pg_try_advisory_lock(38041426) AS locked').fetchone()['locked']
        if not locked:
            return
        sync_events(conn)
        send_pending(conn, root)


def main():
    logging.basicConfig(level=logging.INFO)
    interval = float(os.getenv('ALERT_INTERVAL_SECONDS', '15'))
    while True:
        heartbeat()
        try:
            run_once()
        except psycopg.Error as error:
            log.warning('Could not update alerts: %s', error)
        time.sleep(interval)


if __name__ == '__main__':
    main()
