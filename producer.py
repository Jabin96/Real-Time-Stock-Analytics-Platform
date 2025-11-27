import time
import json
import pandas as pd
import os
import random
from kafka import KafkaProducer

KAFKA_BROKER = os.getenv('KAFKA_BROKER', '127.0.0.1:9093')
TOPIC_NAME = 'test_topic'
DATA_FILE = 'data/stock_data.csv'

SPEED = 0.2
ANOMALY_EVERY_SEC = 1800

# FAKE ANOMALY FEATURE (Plug-and-Play)
# Set to False to disable controlled anomaly generation
ENABLE_FAKE_ANOMALIES = True

print(f"Connecting to Kafka at {KAFKA_BROKER}")

producer = None
for i in range(15):
    try:
        producer = KafkaProducer(
            bootstrap_servers=[KAFKA_BROKER],
            value_serializer=lambda x: json.dumps(x).encode('utf-8'),
            api_version=(0, 10, 1)
        )
        print("Connected to Kafka")
        break
    except Exception as e:
        print(f"Waiting for Kafka... ({e})")
        time.sleep(2)

if not producer:
    print("Critical Error: Kafka Unreachable")
    exit()

try:
    df = pd.read_csv(DATA_FILE)
    print(f"Loaded {len(df)} rows")
except Exception as e:
    print(f"Error: {e}")
    exit()

trigger_index = int(ANOMALY_EVERY_SEC / SPEED)

print(f"Streaming Started")
print(f"Speed: {SPEED}s per tick")
if ENABLE_FAKE_ANOMALIES:
    print(f"Fake Anomalies: Every {ANOMALY_EVERY_SEC}s (~{trigger_index} records)")
else:
    print("Fake Anomalies: Disabled")

for index, row in df.iterrows():
    symbol = str(row['symbol'])
    price = float(row['price'])
    
    is_anomaly = False
    
    if ENABLE_FAKE_ANOMALIES and index > 0 and index % trigger_index == 0:
        price = price * 5.0
        is_anomaly = True
        time.sleep(random.uniform(0.1, 1.0))

    data = {
        'symbol': symbol,
        'price': round(price, 2),
        'timestamp': time.time()
    }
    
    producer.send(TOPIC_NAME, value=data)
    
    if is_anomaly:
        print(f"Generated fake anomaly: {symbol} ${price:.2f}")
    elif index % 50 == 0:
        print(f"[{index}] Sent: {symbol} ${price:.2f}")
    
    time.sleep(SPEED)

print("Stream finished")