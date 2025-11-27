import time
import json
import pandas as pd
from kafka import KafkaProducer

KAFKA_PORT = '9093'
TOPIC_NAME = 'test_topic'
DATA_FILE = 'data/stock_data.csv'  
SPEED = 0.5 

print(f"Connecting to Kafka on 127.0.0.1:{KAFKA_PORT}")

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

try:
    print(f"Loading data from {DATA_FILE}")
    df = pd.read_csv(DATA_FILE)
    print(f"Loaded {len(df)} rows. Starting stream")
except Exception as e:
    print(f"Error reading CSV: {e}")
    exit()

print(f"Streaming data to topic: '{TOPIC_NAME}'")

for index, row in df.iterrows():
    data = {
        'symbol': str(row['symbol']),
        'price': float(row['price']),
        'timestamp': time.time() 
    }
    
    producer.send(TOPIC_NAME, value=data)
    print(f"[{index+1}/{len(df)}] Sent: {data['symbol']} ${data['price']}")
    
    time.sleep(SPEED)

print("Stream finished")