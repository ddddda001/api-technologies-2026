import os
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import redis
from psycopg.rows import dict_row


HORIZONS = ('1d', '1m', '6m', '1y')
METRICS = ('cpu', 'ram', 'disk')


def database():
    return psycopg.connect(os.environ['DATABASE_URL'], connect_timeout=3, row_factory=dict_row)


def redis_client():
    return redis.Redis.from_url(
        os.getenv('REDIS_URL', 'redis://redis:6379/0'),
        decode_responses=True,
        socket_connect_timeout=1,
        socket_timeout=1,
    )


def utc_now():
    return datetime.now(timezone.utc)


def kafka_topic():
    return os.getenv('KAFKA_TOPIC', 'server-metrics')


def kafka_address():
    return os.getenv('KAFKA_BOOTSTRAP', 'kafka:9092')


def heartbeat():
    Path('/tmp/capacity-heartbeat').touch()
