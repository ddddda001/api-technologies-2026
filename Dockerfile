FROM docker.io/library/python:3.13-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app

COPY requirements.txt ./
RUN python -m pip install --no-cache-dir -r requirements.txt

COPY forecast_server.py forecast_client.py forecast_prototype.py forecast_pb2.py forecast_pb2_grpc.py load_daily_metrics.py ./
COPY alert_worker.py api_service.py forecast_worker.py metrics_collector.py metrics_consumer.py monitoring_agent.py service_common.py ./
COPY db/migrations/ db/migrations/
