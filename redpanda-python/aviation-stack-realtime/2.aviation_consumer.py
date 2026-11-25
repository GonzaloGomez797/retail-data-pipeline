import os
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import connection_options

import json
import mysql.connector
from kafka import KafkaConsumer
from kafka.structs import OffsetAndMetadata, TopicPartition

consumer = KafkaConsumer(
  **connection_options(),
  auto_offset_reset="earliest",
  enable_auto_commit=False,
  consumer_timeout_ms=-1,
  group_id=os.getenv("AVIATION_KAFKA_GROUP", "grupo-vuelos")
)

consumer.subscribe([os.getenv("AVIATION_KAFKA_TOPIC", "aviationstack-topic")])

db_config = {
    "host": os.environ["AVIATION_MYSQL_HOST"],
    "user": os.environ["AVIATION_MYSQL_USER"],
    "password": os.environ["AVIATION_MYSQL_PASSWORD"],
    "database": os.environ["AVIATION_MYSQL_DATABASE"],
}

#Conexión a base de datos
db_connection = mysql.connector.connect(**db_config)
cursor = db_connection.cursor()

def insert_into_db(data):
    user = os.getenv("AVIATION_MYSQL_ROW_USER", "Gonzalo Gomez")
    timestamp = data.get("timestamp")
    if timestamp:
        timestamp = datetime.fromisoformat(timestamp)
        if timestamp.tzinfo is not None:
            timestamp = timestamp.astimezone(timezone.utc).replace(tzinfo=None)

    insert_query = """
        INSERT INTO vuelos (vuelo,aerolinea,fecha_vuelo,origen,destino,latitud,longitud,altitud,velocidad,`timestamp`,`user`)
        values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    cursor.execute(insert_query,(
        data['vuelo'],data['aerolinea'],data['fecha_vuelo'],
        data['origen'],data['destino'],data['latitud'],
        data['longitud'],data['altitud'],data['velocidad'],
        timestamp,user
        ))

for message in consumer:
    try:
        message_value = json.loads(message.value.decode("utf-8"))
        for  unit_message in  message_value:
            insert_into_db(unit_message)
        db_connection.commit()
    except Exception:
        db_connection.rollback()
        raise

    partition = TopicPartition(message.topic, message.partition)
    consumer.commit(offsets={partition: OffsetAndMetadata(message.offset + 1, "")})
    print(f"Guardados {len(message_value)} vuelos | Offset {message.offset}")
