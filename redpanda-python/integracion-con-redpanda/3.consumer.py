import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from config import TOPIC, connection_options

from kafka import KafkaConsumer

consumer = KafkaConsumer(
  **connection_options(),
  auto_offset_reset="earliest",#latest
  enable_auto_commit=False,
  consumer_timeout_ms=-1
)
consumer.subscribe([TOPIC])

for message in consumer:
  topic_info = f"topic: {message.topic} ({message.partition}|{message.offset})"
  message_info = f"key: {message.key}, {message.value}"
  print(f"{topic_info}, {message_info}")
