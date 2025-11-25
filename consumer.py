import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, stddev, abs, lit, current_timestamp
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType
from pyspark.sql.window import Window

# Configuration
KAFKA_PORT = '9093'
TOPIC_NAME = 'test_topic'
CHECKPOINT_DIR = "./outputs/checkpoints"
OUTPUT_DIR = "./outputs/streaming_data"
ANOMALY_DIR = "./outputs/anomalies"

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("StockAnalyticsEngine") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.driver.host", "127.0.0.1") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 2. Define Data Schema
schema = StructType() \
    .add("symbol", StringType()) \
    .add("price", DoubleType()) \
    .add("timestamp", DoubleType()) 

# 3. Read from Kafka Stream
print(f"Listening to {TOPIC_NAME} on port {KAFKA_PORT}...")
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"127.0.0.1:{KAFKA_PORT}") \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "latest") \
    .load()

# 4. Parse and Transform Data
parsed_df = raw_df.select(from_json(col("value").cast("string"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("timestamp", col("timestamp").cast(TimestampType()))

# ---------------------------------------------------------
# Core Analytics Logic: Process each batch
# ---------------------------------------------------------
def process_batch(batch_df, batch_id):
    if batch_df.count() == 0:
        return
        
    # --- A. Write Raw Data ---
    batch_df.write \
        .mode("append") \
        .csv(f"{OUTPUT_DIR}/raw_ticks", header=True)
        
    # --- B. Calculate 5-Min Moving Average ---
    # We group by symbol to get the average price in this batch
    moving_avg_df = batch_df.groupBy("symbol") \
        .agg(avg("price").alias("5_min_avg_price")) \
        .withColumn("timestamp", current_timestamp())
        
    # Write Averages to Console
    print(f"--- Batch {batch_id}: Moving Averages ---")
    moving_avg_df.show(truncate=False)

    # --- C. Anomaly Detection ---
    # Logic: Calculate Z-Score. If |Z| > 3, it's an anomaly.
    # Calculates stats on the current micro-batch
    
    window_spec = Window.partitionBy("symbol")
    
    anomaly_df = batch_df \
        .withColumn("mean", avg("price").over(window_spec)) \
        .withColumn("stddev", stddev("price").over(window_spec)) \
        .withColumn("z_score", (col("price") - col("mean")) / col("stddev")) \
        .filter(abs(col("z_score")) > 3) # The Threshold
        
    # Write anomalies if found
    if anomaly_df.count() > 0:
        print(f"ANOMALIES DETECTED in Batch {batch_id}!")
        anomaly_df.select("symbol", "price", "z_score").show()
        
        anomaly_df.select("symbol", "price", "timestamp", "z_score") \
            .write \
            .mode("append") \
            .csv(f"{ANOMALY_DIR}", header=True)

# 5. Start the Streaming Query
query = parsed_df.writeStream \
    .foreachBatch(process_batch) \
    .trigger(processingTime="5 seconds") \
    .start()

print("Analytics Engine Running...")
print("   -> Writing Raw Data to: outputs/streaming_data/raw_ticks")
print("   -> Writing Anomalies to: outputs/anomalies")
query.awaitTermination()