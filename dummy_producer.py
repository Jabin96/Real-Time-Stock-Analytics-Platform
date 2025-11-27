import time
import json
import random
from kafka import KafkaProducer

KAFKA_PORT = '9093'
TOPIC_NAME = 'test_topic'

print(f"Connecting to Kafka on localhost:{KAFKA_PORT}")

try:
    producer = KafkaProducer(
        bootstrap_servers=[f'127.0.0.1:{KAFKA_PORT}'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8'),
        api_version=(0, 10, 1)
    )
    print("Connected to Kafka")
except Exception as e:
    print(f"Connection Failed: {e}")
    exit()

print(f"Sending data to topic: '{TOPIC_NAME}'")

while True:
    data = {
        'symbol': random.choice(['AAPL', 'GOOGL', 'MSFT']),
        'price': round(random.uniform(100, 200), 2),
        'timestamp': time.time()
    }
    
    producer.send(TOPIC_NAME, value=data)
    print(f"Sent: {data}")
    time.sleep(1)
