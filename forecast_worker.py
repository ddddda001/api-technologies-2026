import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

import grpc
import psycopg

from forecast_pb2 import RequestPredict
from forecast_pb2_grpc import ForecastServiceStub
from service_common import HORIZONS, database, heartbeat, utc_now


log = logging.getLogger(__name__)


def refresh_daily_metrics(conn, today):
    yesterday = today - timedelta(days=1)
    conn.execute(
        '''
        INSERT INTO server_daily_metrics (server_id, day, cpu_percent, ram_percent, disk_percent)
        SELECT server_id, %s, avg(cpu_percent), avg(ram_percent), avg(disk_percent)
        FROM server_live_metrics
        WHERE measured_at >= %s::date AT TIME ZONE 'UTC'
          AND measured_at < %s::date AT TIME ZONE 'UTC'
        GROUP BY server_id
        HAVING count(DISTINCT date_trunc('minute', measured_at)) = 1440
        ON CONFLICT (server_id, day) DO NOTHING
        ''',
        (yesterday, yesterday, today),
    )


def forecast_server(stub, server_id, horizons):
    results = []
    retry = False
    for horizon in horizons:
        for mode in ('mean', 'peak'):
            request = RequestPredict(serverID=server_id, horizon=horizon, mode=mode)
            try:
                reply = stub.Request(request, timeout=6)
            except grpc.RpcError as error:
                if error.code() != grpc.StatusCode.FAILED_PRECONDITION:
                    log.warning('Forecast failed for %s %s/%s: %s', server_id, horizon, mode, error.code())
                    retry = True
                continue
            results.append((server_id, horizon, mode, reply.values_cpu, reply.values_ram, reply.values_disk))
    return results, retry


def refresh_forecasts(stub, today, horizons):
    with database() as conn:
        refresh_daily_metrics(conn, today)
        ids = [row['id'] for row in conn.execute('SELECT id FROM servers WHERE deleted_at IS NULL')]

    results = []
    retry = False
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = [pool.submit(forecast_server, stub, server_id, horizons) for server_id in ids]
        for future in futures:
            forecasts, failed = future.result()
            results.extend(forecasts)
            retry = retry or failed
    with database() as conn, conn.cursor() as cur:
        cur.executemany(
            '''
            INSERT INTO server_forecasts
                (server_id, horizon, mode, cpu_percent, ram_percent, disk_percent)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (server_id, horizon, mode) DO UPDATE SET
                cpu_percent = EXCLUDED.cpu_percent,
                ram_percent = EXCLUDED.ram_percent,
                disk_percent = EXCLUDED.disk_percent,
                generated_at = now()
            ''',
            results,
        )
    log.info('Stored %s forecasts for %s servers', len(results), len(ids))
    return not retry


def main():
    logging.basicConfig(level=logging.INFO)
    interval = float(os.getenv('FORECAST_INTERVAL_SECONDS', '60'))
    address = os.getenv('FORECAST_ADDRESS', 'forecast-service:50051')
    last_daily_run = None
    with grpc.insecure_channel(address) as channel:
        stub = ForecastServiceStub(channel)
        while True:
            started = time.monotonic()
            heartbeat()
            today = utc_now().date()
            horizons = HORIZONS if today != last_daily_run else ('1d',)
            try:
                if refresh_forecasts(stub, today, horizons):
                    last_daily_run = today
            except psycopg.Error as error:
                log.warning('Could not store forecasts: %s', error)
            time.sleep(max(0, interval - (time.monotonic() - started)))


if __name__ == '__main__':
    main()
