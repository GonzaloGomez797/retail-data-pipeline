import json
import os

from kafka import KafkaConsumer

from config import connection_options


def main():
    topic = os.getenv('AVIATION_KAFKA_TOPIC', 'aviationstack-topic')
    consumer = KafkaConsumer(
        topic,
        **connection_options(),
        group_id=None,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        consumer_timeout_ms=15_000,
    )
    try:
        for message in consumer:
            for flight in json.loads(message.value):
                print(json.dumps(flight, ensure_ascii=True))
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
