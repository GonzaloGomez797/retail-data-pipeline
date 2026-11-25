from kafka import KafkaConsumer

from config import TOPIC, connection_options


def main():
    consumer = KafkaConsumer(
        TOPIC,
        **connection_options(),
        group_id=None,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        consumer_timeout_ms=60_000,
    )
    try:
        for message in consumer:
            value = message.value.decode('utf-8', errors='replace')
            print(f'{message.topic} | partición {message.partition} | offset {message.offset} | {value}')
    finally:
        consumer.close()


if __name__ == '__main__':
    main()
