from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, avg, window
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import col

scala_version = '2.12'
spark_version = '3.1.2'

packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:2.8.0'
]

# Create a SparkSession
spark = SparkSession.builder \
    .master("local") \
    .appName("KafkaStreamProcessing") \
    .config("spark.jars.packages", ",".join(packages)) \
    .getOrCreate()

# Set the log level
spark.sparkContext.setLogLevel("ERROR")

# Define the Kafka parameters
kafka_params = {
    "kafka.bootstrap.servers": "localhost:9092",
    "subscribe": "gemini-data-streaming",
    "startingOffsets": "earliest",
    "enable_auto_commit": "True",
}

# Define the schema for your Kafka message value
schema = StructType([
    StructField("time", TimestampType(), True),
    StructField("ip", StringType(), True),
    StructField("score", DoubleType(), True)
])

# Create a Kafka source stream
kafka_stream = spark.readStream \
    .format("kafka") \
    .options(**kafka_params) \
    .load()

# Parse JSON data from Kafka message value
parsed_stream = kafka_stream.select(
    from_json(col("value").cast("string"), schema).alias("data"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp")
)

# Select additional columns for printing
parsedDf = parsed_stream.select(
    "data.time",
    "data.ip",
    "data.score",
    "topic",
    "partition",
    "offset",
    "timestamp"
)

# Count the occurrences of each IP address
ip_counts = parsedDf \
    .groupBy("ip").agg(count("*").alias("count"))

# Calculate the average score for each IP address
ip_avg_scores = parsedDf \
    .groupBy("ip").agg(avg("score").alias("avg_score"))

# Print the data to the console
query_ip_counts = ip_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

query_ip_avg_scores = ip_avg_scores.writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Start the query
query_ip_counts.awaitTermination()
query_ip_avg_scores.awaitTermination()
