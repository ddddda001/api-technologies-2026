import datetime as dt
import os
import grpc,forecast_pb2_grpc
import psycopg
from concurrent import futures
from forecast_pb2 import ForecastPredict
from forecast_pb2_grpc import ForecastServiceServicer
from forecast_prototype import get_art,predict_server_metrics

def read_daily_history(cursor, server_id, start_day, end_day):
    query = (
        "SELECT day, cpu_percent, ram_percent, disk_percent "
        "FROM server_daily_metrics "
        "WHERE server_id = %s AND day >= %s AND day < %s "
        "ORDER BY day"
    )
    cursor.execute(query, [server_id, start_day, end_day])
    data = cursor.fetchall()
    if len(data) != 30:
        return None
    points = []
    for day, cpu, ram, disk in data:
        point = {"ts": f"{day.isoformat()}T00:00:00Z","cpuPercent": cpu,"ramPercent": ram,"diskPercent": disk,}
        points.append(point)
    return {"points": points}

class ForecastService(ForecastServiceServicer):
    def __init__(self,art,database_url):
        self.art = art
        self.database_url = database_url

    def Request(self,request,context):
        horizon = request.horizon
        mode = request.mode
        if horizon not in ['1d','1m','6m','1y']:
            context.abort(grpc.StatusCode.INVALID_ARGUMENT, 'Incorrect Horizon')
        if mode not in ['peak','mean']:
                context.abort(grpc.StatusCode.INVALID_ARGUMENT, 'Invalid mode')
        server = {"serverId" :request.serverID}
        end_day = dt.datetime.now(dt.timezone.utc).date()
        start_day = end_day - dt.timedelta(days=30)
        try:
            with psycopg.connect(self.database_url, connect_timeout=5) as conn:
                with conn.cursor() as cur:
                    result = read_daily_history(cur, request.serverID, start_day, end_day)
        except psycopg.OperationalError:
            context.abort(grpc.StatusCode.UNAVAILABLE, 'Database is unavailable')
        if result is not None:
            data = predict_server_metrics(server = server,horizon = horizon, art=self.art, mode = mode, metrics = result)
        else:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, 'Insufficient history for a forecast')
        if len(data) == 3:
            return ForecastPredict(values_cpu=data['cpu'] ,values_ram=data['ram'],values_disk=data['disk'])
        else:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, 'There is no forecast for one of the metrics')

def serve():
    database_url = os.getenv('DATABASE_URL')
    if not database_url:
        raise RuntimeError('DATABASE_URL is not set')
    art = get_art()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    forecast_pb2_grpc.add_ForecastServiceServicer_to_server(ForecastService(art, database_url), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    server.wait_for_termination()

if __name__ == "__main__":
    serve()