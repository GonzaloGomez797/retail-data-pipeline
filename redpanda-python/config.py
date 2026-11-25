import os
from pathlib import Path

from dotenv import load_dotenv


load_dotenv(Path(__file__).with_name('.env'))
TOPIC = os.getenv('KAFKA_TOPIC', 'demo-topic')


def connection_options():
    required = ('KAFKA_BOOTSTRAP_SERVERS', 'KAFKA_USERNAME', 'KAFKA_PASSWORD')
    missing = [name for name in required if not os.getenv(name)]
    if missing:
        raise SystemExit(f"Completa tu archivo .env. Faltan: {', '.join(missing)}")

    return {
        'bootstrap_servers': os.environ['KAFKA_BOOTSTRAP_SERVERS'],
        'security_protocol': 'SASL_SSL',
        'sasl_mechanism': os.getenv('KAFKA_SASL_MECHANISM', 'SCRAM-SHA-256'),
        'sasl_plain_username': os.environ['KAFKA_USERNAME'],
        'sasl_plain_password': os.environ['KAFKA_PASSWORD'],
    }
