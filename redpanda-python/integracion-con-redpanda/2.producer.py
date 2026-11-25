import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import TOPIC, connection_options

import socket
from kafka import KafkaProducer

producer = KafkaProducer(
  **connection_options(),
)
hostname = str.encode(socket.gethostname())
print(hostname)
def on_success(metadata):
  print(f"Sent to topic '{metadata.topic}' at offset {metadata.offset}")

def on_error(e):
  print(f"Error sending message: {e}")

# Produce 100 messages asynchronously
for i in range(100):
  msg = f"asynchronous message #{i}"
  future = producer.send(
    TOPIC,
    key=hostname,
    value=str.encode(msg)
  )
  future.add_callback(on_success) #registra un mensaje
  future.add_errback(on_error) #Se ejecutará si hay un error al enviar el mensaje
producer.flush() #Mensajes pendientes sean enviados
producer.close() #Cerrar el productor de forma lima (Liberar recursos y asegura que todos los mensajes fueron enviados)
