import logging
import json
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, stddev, lit, current_timestamp, pandas_udf, PandasUDFType
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from pyspark.sql.window import Window

import os

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', '127.0.0.1:9093')
TOPIC_NAME = 'test_topic'
CHECKPOINT_DIR = "./outputs/checkpoints"
OUTPUT_DIR = "./outputs/streaming_data"
ANOMALY_DIR = "./outputs/anomalies"

spark_builder = SparkSession.builder \
    .appName("StockAnalyticsEngine") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.network.timeout", "600s") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .config("spark.rpc.askTimeout", "600s") \
    .config("spark.rpc.lookupTimeout", "600s") \
    .config("spark.core.connection.ack.wait.timeout", "600s") \
    .config("spark.sql.streaming.pollingDelay", "500") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.memory.fraction", "0.6") \
    .config("spark.memory.storageFraction", "0.3") \
    .master(os.getenv('SPARK_MASTER', 'local[*]'))

# Only set driver host if explicitly provided (needed for some network setups)
if os.getenv('SPARK_DRIVER_HOST'):
    spark_builder = spark_builder.config("spark.driver.host", os.getenv('SPARK_DRIVER_HOST'))
    spark_builder = spark_builder.config("spark.driver.bindAddress", "0.0.0.0")

spark = spark_builder.getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType() \
    .add("symbol", StringType()) \
    .add("price", DoubleType()) \
    .add("timestamp", DoubleType()) 

print(f"Listening to {TOPIC_NAME} at {KAFKA_BOOTSTRAP_SERVERS}")
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .option("kafka.group.id", "spark-consumer-group-v2") \
    .load()

parsed_df = raw_df.select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", col("timestamp").cast(TimestampType()))



# Define the State Schema (History of prices)
state_schema = StructType() \
    .add("history", StringType())  # JSON string of list of (timestamp, price)

# Define the Output Schema (Enriched data with stats)
output_schema = StructType() \
    .add("symbol", StringType()) \
    .add("price", DoubleType()) \
    .add("timestamp", TimestampType()) \
    .add("average_price", DoubleType()) \
    .add("std_dev", DoubleType()) \
    .add("z_score", DoubleType()) \
    .add("is_anomaly", StringType()) # "true" or "false"

def process_batch(key, pdf, state):
    # key is a tuple (symbol,)
    symbol = key[0]
    
    # Initialize or get state
    if state.exists:
        history_json = state.get[0]
        history = json.loads(history_json)
    else:
        history = []
    
    # Convert input pandas df to list of dicts
    new_data = []
    for pdf in pdf:
        for index, row in pdf.iterrows():
            new_data.append({
                'timestamp': row['timestamp'].timestamp(), # Store as float seconds
                'price': row['price']
            })
    
    # Append new data to history
    history.extend(new_data)
    
    # Sort by timestamp (should be sorted, but good to ensure)
    history.sort(key=lambda x: x['timestamp'])
    
    # Prune history: Keep only last 5 minutes (300 seconds)
    # We use the max timestamp in the current batch as "current time" reference
    # or just the latest in history.
    print(f"Processing batch for key {key}, size: {len(pdf)}", flush=True)

    if pdf.empty:
        return pd.DataFrame()



        
    latest_time = history[-1]['timestamp']
    cutoff_time = latest_time - 300 # 5 minutes window
    
    history = [x for x in history if x['timestamp'] >= cutoff_time]
    
    # Update state
    state.update((json.dumps(history),))
    
    # Calculate stats on the entire valid window (including new points).
    
    prices = [x['price'] for x in history]
    if len(prices) > 1:
        avg_price = sum(prices) / len(prices)
        variance = sum([((x - avg_price) ** 2) for x in prices]) / len(prices)
        std_dev = variance ** 0.5
    else:
        avg_price = prices[0]
        std_dev = 0.0
        
    # Generate Output Rows
    results = []
    for row in new_data:
        price = row['price']
        
        # Avoid division by zero and over-sensitivity
        # If std_dev is very small, we ignore anomalies unless the price change is massive
        if std_dev > 0.05:  # Minimum volatility required to detect anomalies
            z_score = (price - avg_price) / std_dev
        else:
            z_score = 0.0
            
        is_anomaly = abs(z_score) > 3  # Adjusted threshold to 3.0 (Industry Standard)
        
        results.append({
            'symbol': symbol,
            'price': price,
            'timestamp': pd.to_datetime(row['timestamp'], unit='s'),
            'average_price': round(avg_price, 2),
            'std_dev': round(std_dev, 2),
            'z_score': round(z_score, 2),
            'is_anomaly': str(is_anomaly).lower()
        })
        
    yield pd.DataFrame(results)

# Apply Stateful Processing
# Note: applyInPandasWithState requires a timeout config, usually GroupStateTimeout.ProcessingTimeTimeout
print("Starting Stateful Processing Stream...")

# timestamp is already TimestampType, but we convert to float inside the function.

processed_df = parsed_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy("symbol") \
    .applyInPandasWithState(
        process_batch,
        output_schema,
        state_schema,
        "Append",
        "NoTimeout"
    )

# Query: Write Enriched Data (All Ticks with Stats + Anomaly Flag)
# Dashboard reads anomalies directly from this stream via the is_anomaly column
query_raw = processed_df.writeStream \
    .format("csv") \
    .option("header", "true") \
    .option("path", f"{OUTPUT_DIR}/raw_ticks") \
    .option("checkpointLocation", f"{CHECKPOINT_DIR}/raw") \
    .outputMode("append") \
    .trigger(processingTime="2 seconds") \
    .start()

print("Analytics Engine Running")
print(f"Writing Enriched Data to: {OUTPUT_DIR}/raw_ticks")

query_raw.awaitTermination()