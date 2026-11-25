import socket

from kafka import KafkaProducer

from config import TOPIC, connection_options


def main():
    producer = KafkaProducer(**connection_options())
    try:
        key = socket.gethostname().encode('utf-8')
        pending = [
            producer.send(TOPIC, key=key, value=f'Mensaje #{index}'.encode('utf-8'))
            for index in range(100)
        ]
        for result in pending:
            metadata = result.get(timeout=30)
            print(f'{metadata.topic} | partición {metadata.partition} | offset {metadata.offset}')
    finally:
        producer.close()


if __name__ == '__main__':
    main()
