import json
import logging
from datetime import datetime

from confluent_kafka import Consumer, KafkaError, KafkaException

from service_common import database, heartbeat, kafka_address, kafka_topic, redis_client


log = logging.getLogger(__name__)
UPDATE_CACHE = """
local current = redis.call('HGET', KEYS[1], 'timestamp')
if not current or tonumber(ARGV[1]) >= tonumber(current) then
    redis.call('HSET', KEYS[1], 'timestamp', ARGV[1], 'sample', ARGV[2])
end
"""


def save_sample(cache, sample):
    server_id = sample['serverId']
    measured_at = datetime.fromisoformat(sample['timestamp'])
    with database() as conn:
        active = conn.execute(
            'SELECT 1 FROM servers WHERE id = %s AND deleted_at IS NULL FOR SHARE',
            (server_id,),
        ).fetchone()
        if active is None:
            log.info('Ignoring sample for unregistered server %s', server_id)
            return
        values = (server_id, measured_at, sample['cpuPercent'], sample['ramPercent'], sample['diskPercent'])
        stored = conn.execute(
            '''
            INSERT INTO server_live_metrics
                (server_id, measured_at, cpu_percent, ram_percent, disk_percent)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (server_id, measured_at) DO NOTHING
            RETURNING cpu_percent, ram_percent, disk_percent
            ''',
            values,
        ).fetchone()
        if stored is None:
            stored = conn.execute(
                'SELECT cpu_percent, ram_percent, disk_percent FROM server_live_metrics WHERE server_id = %s AND measured_at = %s',
                (server_id, measured_at),
            ).fetchone()
        sample = {
            'serverId': server_id, 'timestamp': measured_at.isoformat(),
            'cpuPercent': stored['cpu_percent'], 'ramPercent': stored['ram_percent'], 'diskPercent': stored['disk_percent'],
        }
    cache.eval(UPDATE_CACHE, 1, f'current:{server_id}', measured_at.timestamp(), json.dumps(sample))


def main():
    logging.basicConfig(level=logging.INFO)
    cache = redis_client()
    consumer = Consumer({
        'bootstrap.servers': kafka_address(),
        'group.id': 'capacity-metrics-consumer',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'allow.auto.create.topics': True,
    })
    consumer.subscribe([kafka_topic()])
    try:
        while True:
            heartbeat()
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error():
                if message.error().code() in (KafkaError.UNKNOWN_TOPIC_OR_PART, KafkaError._UNKNOWN_TOPIC):
                    continue
                raise KafkaException(message.error())
            sample = json.loads(message.value())
            save_sample(cache, sample)
            consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
