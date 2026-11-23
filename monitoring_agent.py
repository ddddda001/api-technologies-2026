import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor

import requests

from forecast_prototype import get_with_retry, url_servers
from service_common import heartbeat


log = logging.getLogger(__name__)


def send_sample(server_id):
    response = get_with_retry(f'{url_servers}/{server_id}/metrics/current', None)
    if response is None:
        return False

    current = response.json()
    sample = {
        'serverId': server_id,
        'timestamp': current['measuredAt'],
        'cpuPercent': current['cpuPercent'],
        'ramPercent': current['ramPercent'],
        'diskPercent': current['diskPercent'],
    }
    try:
        reply = requests.post(
            os.getenv('COLLECTOR_URL', 'http://collector:8081/v1/metrics'),
            headers={'X-Agent-Api-Key': os.environ['AGENT_API_KEY']},
            json=sample,
            timeout=7,
        )
        if reply.status_code == 202:
            return True
        log.warning('Collector returned %s for %s', reply.status_code, server_id)
    except requests.RequestException as error:
        log.warning('Collector request failed for %s: %s', server_id, error)
    return False


def poll_once(pool):
    response = get_with_retry(url_servers, None)
    if response is None:
        return

    servers = response.json()
    pending = {pool.submit(send_sample, server['serverId']): server['serverId'] for server in servers}
    accepted = 0
    for future, server_id in pending.items():
        try:
            accepted += future.result()
        except (ValueError, KeyError) as error:
            log.warning('Invalid monitoring response for %s: %s', server_id, error)
    log.info('Collector accepted %s/%s current samples', accepted, len(servers))


def main():
    logging.basicConfig(level=logging.INFO)
    interval = float(os.getenv('MONITORING_INTERVAL_SECONDS', '60'))
    with ThreadPoolExecutor(max_workers=40) as pool:
        while True:
            started = time.monotonic()
            heartbeat()
            try:
                poll_once(pool)
            except (ValueError, KeyError) as error:
                log.warning('Could not read the server inventory: %s', error)
            time.sleep(max(0, interval - (time.monotonic() - started)))


if __name__ == '__main__':
    main()
