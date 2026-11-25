import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import connection_options

import json
import mysql.connector
from kafka import KafkaConsumer

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
    user = 'JuanS'
    insert_query = """
        INSERT INTO vuelos (vuelo,aerolinea,fecha_vuelo,origen,destino,latitud,longitud,altitud,velocidad,timestamp,user)
        values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    cursor.execute(insert_query,(
        data['vuelo'],data['aerolinea'],data['fecha_vuelo'],
        data['origen'],data['destino'],data['latitud'],
        data['longitud'],data['altitud'],data['velocidad'],
        data['timestamp'],user
        ))

for message in consumer:
    try:
        message_value = json.loads(message.value.decode("utf-8"))
        for  unit_message in  message_value:
            insert_into_db(unit_message)
            print(f"Guardando vuelo {unit_message['vuelo']} en la base de datos")
        db_connection.commit()
        consumer.commit()
    except Exception:
        db_connection.rollback()
        raise
