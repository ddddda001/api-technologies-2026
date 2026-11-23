import base64
import binascii
import hmac
import json
import os
import secrets
from datetime import datetime, timedelta
from typing import Literal
from uuid import uuid4

import jwt
import psycopg
from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Query, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import AwareDatetime, BaseModel, Field
from redis.exceptions import RedisError
from starlette.exceptions import HTTPException as StarletteHTTPException

from service_common import database, redis_client, utc_now


app = FastAPI(title='CapacityForecast API')
cache = redis_client()
token_secret = os.getenv('JWT_SECRET') or secrets.token_urlsafe(48)
token_lifetime = 3600


def fail(status, code, message):
    raise HTTPException(status, detail={'code': code, 'message': message})


@app.exception_handler(StarletteHTTPException)
async def http_error(_request, error):
    detail = error.detail
    if not isinstance(detail, dict):
        detail = {'code': 'http_error', 'message': str(detail)}
    return JSONResponse(status_code=error.status_code, content={'error': detail})


@app.exception_handler(RequestValidationError)
async def validation_error(_request, error):
    return JSONResponse(status_code=400, content={
        'error': {'code': 'validation_error', 'message': error.errors()[0]['msg']},
    })


@app.exception_handler(psycopg.OperationalError)
async def database_error(_request, _error):
    return JSONResponse(status_code=503, content={
        'error': {'code': 'unavailable', 'message': 'Database is unavailable'},
    })


class Login(BaseModel):
    email: str = Field(pattern=r'^[^@\s]+@[^@\s]+\.[^@\s]+$')
    password: str = Field(min_length=8)


class SystemInput(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    description: str | None = Field(default=None, max_length=1000)


class ServerInput(BaseModel):
    hostname: str = Field(min_length=1, max_length=255)
    isId: str = Field(min_length=1)


class Thresholds(BaseModel):
    cpuPercent: float = Field(ge=1, le=100, allow_inf_nan=False)
    ramPercent: float = Field(ge=1, le=100, allow_inf_nan=False)
    diskPercent: float = Field(ge=1, le=100, allow_inf_nan=False)


@app.get('/health')
def health():
    with database() as conn:
        conn.execute('SELECT 1')
    try:
        cache.ping()
    except RedisError:
        fail(503, 'unavailable', 'Redis is unavailable')
    return {'status': 'ok'}


@app.post('/v1/auth/login')
def login(body: Login):
    email = os.getenv('API_LOGIN_EMAIL', 'engineer@example.com')
    password = os.getenv('API_LOGIN_PASSWORD', '')
    if (not password
        or not hmac.compare_digest(body.email.encode(), email.encode())
        or not hmac.compare_digest(body.password.encode(), password.encode())):
        fail(401, 'invalid_credentials', 'Invalid email or password')
    now = utc_now()
    token = jwt.encode(
        {'sub': email, 'iat': now, 'exp': now + timedelta(seconds=token_lifetime)},
        token_secret, algorithm='HS256',
    )
    return {'accessToken': token, 'tokenType': 'Bearer', 'expiresInSeconds': token_lifetime}


def authenticate(authorization: str = Header(default='')):
    prefix, _, token = authorization.partition(' ')
    if prefix.lower() != 'bearer' or not token:
        fail(401, 'unauthorized', 'Bearer token is required')
    try:
        jwt.decode(token, token_secret, algorithms=['HS256'], options={'require': ['sub', 'exp']})
    except jwt.InvalidTokenError:
        fail(401, 'unauthorized', 'Invalid or expired token')


api = APIRouter(prefix='/v1', dependencies=[Depends(authenticate)])
SERVER_FIELDS = 'id, hostname, is_id AS "isId", created_at AS "createdAt"'
METRIC_KEYS = {'cpu_percent': 'cpuPercent', 'ram_percent': 'ramPercent', 'disk_percent': 'diskPercent'}


def metric_fields(row):
    return {public_key: row[db_key] for db_key, public_key in METRIC_KEYS.items()}


def get_system(conn, system_id):
    row = conn.execute('SELECT * FROM information_systems WHERE id = %s', (system_id,)).fetchone()
    if row is None:
        fail(404, 'not_found', 'Information system not found')
    return row


def get_server(conn, server_id):
    row = conn.execute(
        f'SELECT {SERVER_FIELDS} FROM servers WHERE id = %s AND deleted_at IS NULL',
        (server_id,),
    ).fetchone()
    if row is None:
        fail(404, 'not_found', 'Server not found')
    return row


@api.get('/information-systems')
def list_systems():
    with database() as conn:
        return conn.execute('SELECT * FROM information_systems ORDER BY id').fetchall()


@api.post('/information-systems', status_code=201)
def create_system(body: SystemInput):
    system_id = f'is-{uuid4().hex[:12]}'
    with database() as conn:
        return conn.execute(
            'INSERT INTO information_systems (id, name, description) VALUES (%s, %s, %s) RETURNING *',
            (system_id, body.name, body.description),
        ).fetchone()


@api.get('/information-systems/{isId}')
def read_system(isId: str):
    with database() as conn:
        return get_system(conn, isId)


@api.patch('/information-systems/{isId}')
def update_system(isId: str, body: SystemInput):
    with database() as conn:
        get_system(conn, isId)
        if 'description' not in body.model_fields_set:
            return conn.execute(
                'UPDATE information_systems SET name = %s WHERE id = %s RETURNING *',
                (body.name, isId),
            ).fetchone()
        return conn.execute(
            'UPDATE information_systems SET name = %s, description = %s WHERE id = %s RETURNING *',
            (body.name, body.description, isId),
        ).fetchone()


@api.delete('/information-systems/{isId}', status_code=204)
def delete_system(isId: str):
    with database() as conn:
        if conn.execute(
            'SELECT 1 FROM information_systems WHERE id = %s FOR UPDATE', (isId,),
        ).fetchone() is None:
            fail(404, 'not_found', 'Information system not found')
        if conn.execute(
            'SELECT 1 FROM servers WHERE is_id = %s AND deleted_at IS NULL LIMIT 1', (isId,),
        ).fetchone():
            fail(409, 'is_has_servers', 'Information system still has servers')
        conn.execute('DELETE FROM servers WHERE is_id = %s AND deleted_at IS NOT NULL', (isId,))
        conn.execute('DELETE FROM information_systems WHERE id = %s', (isId,))
    return Response(status_code=204)


@api.get('/servers')
def list_servers(isId: str | None = None):
    with database() as conn:
        return conn.execute(
            f'SELECT {SERVER_FIELDS} FROM servers WHERE deleted_at IS NULL AND (%s::text IS NULL OR is_id = %s) ORDER BY id',
            (isId, isId),
        ).fetchall()


@api.post('/servers', status_code=201)
def create_server(body: ServerInput):
    server_id = f'srv-{uuid4().hex[:12]}'
    with database() as conn:
        get_system(conn, body.isId)
        return conn.execute(
            f'INSERT INTO servers (id, hostname, is_id) VALUES (%s, %s, %s) RETURNING {SERVER_FIELDS}',
            (server_id, body.hostname, body.isId),
        ).fetchone()


@api.get('/servers/{serverId}')
def read_server(serverId: str):
    with database() as conn:
        return get_server(conn, serverId)


@api.patch('/servers/{serverId}')
def update_server(serverId: str, body: ServerInput):
    with database() as conn:
        get_server(conn, serverId)
        get_system(conn, body.isId)
        return conn.execute(
            f'UPDATE servers SET hostname = %s, is_id = %s WHERE id = %s AND deleted_at IS NULL RETURNING {SERVER_FIELDS}',
            (body.hostname, body.isId, serverId),
        ).fetchone()


@api.delete('/servers/{serverId}', status_code=204)
def delete_server(serverId: str):
    with database() as conn:
        row = conn.execute(
            'UPDATE servers SET deleted_at = now() WHERE id = %s AND deleted_at IS NULL RETURNING id',
            (serverId,),
        ).fetchone()
        if row is None:
            fail(404, 'not_found', 'Server not found')
        for table in ('server_live_metrics', 'server_daily_metrics', 'server_forecasts', 'server_thresholds', 'alert_events'):
            conn.execute(f'DELETE FROM {table} WHERE server_id = %s', (serverId,))
    try:
        cache.delete(f'current:{serverId}')
    except RedisError:
        pass
    return Response(status_code=204)


@api.get('/servers/{serverId}/thresholds')
def read_thresholds(serverId: str):
    with database() as conn:
        get_server(conn, serverId)
        row = conn.execute('SELECT * FROM server_thresholds WHERE server_id = %s', (serverId,)).fetchone()
        return metric_fields(row) if row else dict.fromkeys(METRIC_KEYS.values(), 100)


@api.put('/servers/{serverId}/thresholds')
def update_thresholds(serverId: str, body: Thresholds):
    with database() as conn:
        get_server(conn, serverId)
        row = conn.execute(
            '''
            INSERT INTO server_thresholds (server_id, cpu_percent, ram_percent, disk_percent)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (server_id) DO UPDATE SET
                cpu_percent = EXCLUDED.cpu_percent,
                ram_percent = EXCLUDED.ram_percent,
                disk_percent = EXCLUDED.disk_percent
            RETURNING *
            ''',
            (serverId, body.cpuPercent, body.ramPercent, body.diskPercent),
        ).fetchone()
    return metric_fields(row)


@api.get('/servers/{serverId}/current-state')
def current_state(serverId: str):
    with database() as conn:
        get_server(conn, serverId)
        try:
            raw = cache.hget(f'current:{serverId}', 'sample')
        except RedisError:
            raw = None
        if raw:
            latest = json.loads(raw)
            measured_at = datetime.fromisoformat(latest['timestamp'])
            values = {key: latest[key] for key in METRIC_KEYS.values()}
        else:
            row = conn.execute(
                'SELECT * FROM server_live_metrics WHERE server_id = %s ORDER BY measured_at DESC LIMIT 1',
                (serverId,),
            ).fetchone()
            measured_at = row['measured_at'] if row else None
            values = metric_fields(row) if row else dict.fromkeys(METRIC_KEYS.values())
    return {
        'serverId': serverId, **values, 'measuredAt': measured_at,
        'stale': measured_at is None or utc_now() - measured_at > timedelta(minutes=2),
    }


def history_query(resolution, server_id, start, end):
    args = (server_id, start, end)
    if resolution == 'raw':
        return (
            '''
            SELECT measured_at AS timestamp, cpu_percent, ram_percent, disk_percent
            FROM server_live_metrics WHERE server_id = %s AND measured_at >= %s AND measured_at < %s
            ''', args,
        )
    if resolution == 'hourly':
        return (
            '''
            SELECT time_bucket('1 hour', measured_at) AS timestamp, avg(cpu_percent) AS cpu_percent,
                avg(ram_percent) AS ram_percent, avg(disk_percent) AS disk_percent
            FROM server_live_metrics WHERE server_id = %s AND measured_at >= %s AND measured_at < %s
            GROUP BY 1
            ''', args,
        )
    return (
        '''
        SELECT day::timestamp AT TIME ZONE 'UTC' AS timestamp, cpu_percent, ram_percent, disk_percent
        FROM server_daily_metrics WHERE server_id = %s
        UNION ALL
        SELECT time_bucket('1 day', measured_at) AS timestamp, avg(cpu_percent),
            avg(ram_percent), avg(disk_percent)
        FROM server_live_metrics AS live
        WHERE server_id = %s AND measured_at >= %s AND measured_at < %s
          AND NOT EXISTS (
              SELECT 1 FROM server_daily_metrics AS daily WHERE daily.server_id = live.server_id
                 AND daily.day = (live.measured_at AT TIME ZONE 'UTC')::date
          )
        GROUP BY 1
        ''', (server_id, *args),
    )


def parse_cursor(cursor, scope):
    if cursor is None:
        return None
    try:
        parsed = json.loads(base64.urlsafe_b64decode(cursor))
        if not isinstance(parsed, list) or len(parsed) != 2 or parsed[0] != scope:
            raise ValueError
        last = datetime.fromisoformat(parsed[1])
        if last.tzinfo is None:
            raise ValueError
        return last
    except (ValueError, TypeError, binascii.Error):
        fail(400, 'validation_error', 'Invalid cursor for this history query')


@api.get('/servers/{serverId}/metrics')
def metrics(
    serverId: str,
    start: AwareDatetime = Query(alias='from'),
    end: AwareDatetime = Query(alias='to'),
    resolution: Literal['raw', 'hourly', 'daily'] = 'hourly',
    cursor: str | None = None,
    limit: int = Query(default=500, ge=1, le=1000),
):
    if start >= end:
        fail(400, 'validation_error', 'from must be earlier than to')
    scope = [serverId, resolution, start.isoformat(), end.isoformat()]
    last = parse_cursor(cursor, scope)
    query, args = history_query(resolution, serverId, start, end)
    with database() as conn:
        get_server(conn, serverId)
        rows = conn.execute(
            f'''
            SELECT * FROM ({query}) AS points WHERE timestamp >= %s AND timestamp < %s
              AND (%s::timestamptz IS NULL OR timestamp > %s)
            ORDER BY timestamp LIMIT %s
            ''', (*args, start, end, last, last, limit + 1),
        ).fetchall()
    result = {
        'serverId': serverId, 'resolution': resolution,
        'points': [{'timestamp': row['timestamp'], **metric_fields(row)} for row in rows[:limit]],
    }
    if len(rows) > limit:
        payload = json.dumps([scope, rows[limit - 1]['timestamp'].isoformat()]).encode()
        result['nextCursor'] = base64.urlsafe_b64encode(payload).decode()
    return result


@api.get('/servers/{serverId}/forecasts')
def forecast(serverId: str, horizon: Literal['1d', '1m', '6m', '1y']):
    with database() as conn:
        get_server(conn, serverId)
        row = conn.execute(
            "SELECT * FROM server_forecasts WHERE server_id = %s AND horizon = %s AND mode = 'peak'",
            (serverId, horizon),
        ).fetchone()
    return {
        'serverId': serverId, 'horizon': horizon, 'status': 'ready' if row else 'pending',
        **(metric_fields(row) if row else dict.fromkeys(METRIC_KEYS.values())),
        'generatedAt': row['generated_at'] if row else None,
    }


@api.get('/information-systems/{isId}/summary')
def summary(isId: str):
    with database() as conn:
        get_system(conn, isId)
        row = conn.execute(
            '''
            SELECT count(DISTINCT s.id) AS "serverCount",
                count(DISTINCT s.id) FILTER (WHERE f.horizon = '1d' AND
                  (f.cpu_percent >= t.cpu_percent OR f.ram_percent >= t.ram_percent OR f.disk_percent >= t.disk_percent))
                  AS "serversOverThreshold1d",
                count(DISTINCT s.id) FILTER (WHERE f.horizon = '1m' AND
                  (f.cpu_percent >= t.cpu_percent OR f.ram_percent >= t.ram_percent OR f.disk_percent >= t.disk_percent))
                  AS "serversOverThreshold1m"
            FROM servers s
            LEFT JOIN server_forecasts f ON f.server_id = s.id AND f.mode = 'peak'
            LEFT JOIN server_thresholds t ON t.server_id = s.id
            WHERE s.is_id = %s AND s.deleted_at IS NULL
            ''', (isId,),
        ).fetchone()
    return {'isId': isId, **row, 'generatedAt': utc_now()}


app.include_router(api)
