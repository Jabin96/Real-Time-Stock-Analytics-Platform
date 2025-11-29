import time
import json
import pandas as pd
import os
import random
import socket
from kafka import KafkaProducer

KAFKA_BROKER = os.getenv('KAFKA_BROKER', '127.0.0.1:9093')
TOPIC_NAME = 'test_topic'
DATA_FILE = 'data/stock_data.csv'

SPEED = 0.2
ANOMALY_EVERY_SEC = 180

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
    
    # Determine which subset of stocks this producer should handle
    TOTAL_PRODUCERS = int(os.getenv('TOTAL_PRODUCERS', '1'))
    
    # Try to get index from Env Var first (Reliable)
    env_index = os.getenv('PRODUCER_INDEX')
    if env_index:
        producer_index = int(env_index)
    else:
        # Fallback to Hostname (Robust parsing for Docker Compose scaling)
        try:
            hostname = socket.gethostname()
            # Handle both hyphen (project-service-1) and underscore (project_service_1) separators
            if '-' in hostname:
                producer_index = int(hostname.split('-')[-1])
            elif '_' in hostname:
                producer_index = int(hostname.split('_')[-1])
            else:
                # Fallback if no separator found (unlikely in scaled mode)
                producer_index = 1
        except Exception as e:
            print(f"Could not determine producer index from hostname ({hostname}): {e}")
            producer_index = 1
        
    print(f"Producer Index: {producer_index} / {TOTAL_PRODUCERS}")
    
    all_symbols = sorted(df['symbol'].unique())
    
    # Only shard if there are multiple producers
    if TOTAL_PRODUCERS > 1:
        # Modulo arithmetic to distribute symbols across producers
        my_symbols = [s for i, s in enumerate(all_symbols) if i % TOTAL_PRODUCERS == (producer_index - 1)]
        print(f"Sharding enabled: Handling {len(my_symbols)} symbols out of {len(all_symbols)} total")
        
        # Filter the dataframe to only include assigned symbols
        df = df[df['symbol'].isin(my_symbols)]
        print(f"Filtered data to {len(df)} rows")
    else:
        # Single producer - handle ALL symbols
        print(f"Single producer mode: Handling all {len(all_symbols)} symbols")
        print(f"Total data rows: {len(df)}")

    
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
    
    # Fake Anomaly Injection
    # Injects a 5x price spike every ~3 minutes (based on ANOMALY_EVERY_SEC)
    if ENABLE_FAKE_ANOMALIES and index > 0 and index % trigger_index == 0:

        price = price * 5.0
        is_anomaly = True
        print(f"Generated fake anomaly for {symbol}: ${round(price, 2)}")

    data = {
        'symbol': symbol,
        'price': round(price, 2),
        'timestamp': time.time()  # Use current time for real-time appearance
    }
    
    producer.send(TOPIC_NAME, value=data)
    
    if is_anomaly:
        pass # Already printed above
    elif index % 50 == 0:
        print(f"[{index}] Sent: {symbol} ${price:.2f}")
    
    time.sleep(SPEED)

print("Stream finished")