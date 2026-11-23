import hmac
import os

from confluent_kafka import Producer
from fastapi import FastAPI, Header
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import AwareDatetime, BaseModel, Field, field_validator

from service_common import kafka_address, kafka_topic, utc_now


app = FastAPI(title='Metrics Collector')
producer = Producer({
    'bootstrap.servers': kafka_address(),
    'enable.idempotence': True,
    'message.timeout.ms': 4500,
})


class MetricSample(BaseModel):
    serverId: str = Field(min_length=1, max_length=255)
    timestamp: AwareDatetime
    cpuPercent: float = Field(ge=0, le=100, allow_inf_nan=False, strict=True)
    ramPercent: float = Field(ge=0, le=100, allow_inf_nan=False, strict=True)
    diskPercent: float = Field(ge=0, le=100, allow_inf_nan=False, strict=True)

    @field_validator('timestamp', mode='before')
    @classmethod
    def timestamp_is_a_string(cls, value):
        if not isinstance(value, str):
            raise ValueError('timestamp must be an ISO 8601 string')
        return value


def error(status, code, message):
    return JSONResponse(status_code=status, content={'error': {'code': code, 'message': message}})


@app.exception_handler(RequestValidationError)
async def validation_error(_request, _error):
    return error(400, 'validation_error', 'Invalid metric sample')


@app.get('/health')
def health():
    try:
        producer.list_topics(timeout=2)
    except Exception:
        return error(503, 'unavailable', 'Kafka is unavailable')
    return {'status': 'ok'}


@app.post('/v1/metrics', status_code=202)
def ingest(sample: MetricSample, x_agent_api_key: str = Header(default='')):
    key = os.environ.get('AGENT_API_KEY', '')
    if not key or not hmac.compare_digest(x_agent_api_key.encode(), key.encode()):
        return error(401, 'unauthorized', 'Invalid agent key')

    delivered = []
    try:
        producer.produce(
            kafka_topic(),
            key=sample.serverId,
            value=sample.model_dump_json(),
            on_delivery=lambda delivery_error, _: delivered.append(delivery_error),
        )
        producer.flush(5)
    except Exception:
        return error(503, 'unavailable', 'Could not deliver the sample to Kafka')

    if delivered != [None]:
        return error(503, 'unavailable', 'Kafka did not acknowledge the sample')
    return {'accepted': True, 'receivedAt': utc_now()}
