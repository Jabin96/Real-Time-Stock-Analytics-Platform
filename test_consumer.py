from pyspark.sql import SparkSession

# --- CONFIGURATION ---
KAFKA_PORT = '9093' # <--- CRITICAL: Your custom port
TOPIC_NAME = 'test_topic'
# ---------------------

# 1. Initialize Spark (This triggers the download of Kafka JARs on first run)
print("⏳ Starting Spark Session... (This may take a minute first time)")
spark = SparkSession.builder \
    .appName("TestConnectivity") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN") # Hide messy logs

print("✅ Spark Started! Listening for data...")

# 2. Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", f"127.0.0.1:{KAFKA_PORT}") \
    .option("subscribe", TOPIC_NAME) \
    .option("startingOffsets", "latest") \
    .load()

# 3. Simple Output to Console
query = df.selectExpr("CAST(value AS STRING)") \
    .writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()
