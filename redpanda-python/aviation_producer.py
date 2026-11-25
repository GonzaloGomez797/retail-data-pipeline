import argparse
import json
import os
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

from config import connection_options


def fetch_flights(api_key, limit, arrival, active):
    params = {'access_key': api_key, 'limit': limit, 'offset': 0}
    if arrival:
        params['arr_iata'] = arrival
    if active:
        params['flight_status'] = 'active'

    try:
        with urlopen('https://api.aviationstack.com/v1/flights?' + urlencode(params), timeout=20) as response:
            payload = json.load(response)
    except HTTPError as error:
        raise SystemExit(f'Aviationstack devolvió HTTP {error.code}. Revisa tu clave y plan.') from None
    except (URLError, TimeoutError, ValueError):
        raise SystemExit('No se pudo obtener una respuesta JSON de Aviationstack.') from None

    if not isinstance(payload, dict) or not isinstance(payload.get('data'), list):
        raise SystemExit('Aviationstack no devolvió una lista de vuelos. Revisa tu clave y plan.')
    return payload['data']


def normalize(record):
    flight = record.get('flight') or {}
    live = record.get('live') or {}
    return {
        'vuelo': flight.get('iata') or flight.get('icao') or flight.get('number'),
        'aerolinea': (record.get('airline') or {}).get('name'),
        'fecha_vuelo': record.get('flight_date'),
        'estado': record.get('flight_status'),
        'origen': (record.get('departure') or {}).get('airport'),
        'destino': (record.get('arrival') or {}).get('airport'),
        'latitud': live.get('latitude'),
        'longitud': live.get('longitude'),
        'altitud': live.get('altitude'),
        'velocidad': live.get('speed_horizontal'),
        'timestamp': live.get('updated'),
    }


def main():
    parser = argparse.ArgumentParser(description='Obtiene una página de vuelos de Aviationstack.')
    parser.add_argument('--preview', action='store_true', help='Muestra los vuelos sin publicarlos.')
    parser.add_argument('--limit', type=int, choices=range(1, 101), metavar='1-100', default=5)
    parser.add_argument('--arrival', help='Código IATA del aeropuerto de llegada.')
    parser.add_argument('--active', action='store_true', help='Filtra por vuelos activos.')
    args = parser.parse_args()

    api_key = os.getenv('AVIATIONSTACK_ACCESS_KEY')
    if not api_key:
        raise SystemExit('Configura AVIATIONSTACK_ACCESS_KEY en .env.')
    options = None if args.preview else connection_options()
    flights = [
        normalize(record)
        for record in fetch_flights(api_key, args.limit, args.arrival, args.active)
    ]
    if args.preview:
        print(json.dumps(flights, ensure_ascii=True, indent=2))
        return
    if not flights:
        print('Aviationstack no devolvió vuelos para estos filtros.')
        return

    from kafka import KafkaProducer

    topic = os.getenv('AVIATION_KAFKA_TOPIC', 'aviationstack-topic')
    producer = KafkaProducer(**options)
    try:
        value = json.dumps(flights, ensure_ascii=False).encode('utf-8')
        metadata = producer.send(topic, value=value).get(timeout=30)
        print(f'{len(flights)} vuelos publicados en {topic}, partición {metadata.partition}, offset {metadata.offset}.')
    finally:
        producer.close()


if __name__ == '__main__':
    main()
