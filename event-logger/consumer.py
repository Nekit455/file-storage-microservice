from kafka import KafkaConsumer
import json
import logging
import os
import time

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
TOPIC = 'file.uploaded'

# Ждём, пока Kafka станет доступна
max_retries = 20
for i in range(max_retries):
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            request_timeout_ms=5000,
            consumer_timeout_ms=1000
        )
        logger.info(f"✅ Connected to Kafka at {KAFKA_BOOTSTRAP_SERVERS}")
        break
    except Exception as e:
        logger.warning(f"Attempt {i+1}/{max_retries}: {e}")
        time.sleep(3)
else:
    logger.error("❌ Failed to connect to Kafka after retries")
    exit(1)

logger.info(f"🎧 Listening for {TOPIC} events...")

for message in consumer:
    event = message.value
    logger.info(f"📁 Event: {event.get('original_filename')} by {event.get('uploaded_by')} (id: {event.get('file_id')})")
