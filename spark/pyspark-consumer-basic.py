from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
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

# Select additional columns for printing
parsedDf = kafka_stream.select(col("key").cast("string"), col("value").cast("string"), col("topic"), col("partition"), col("offset"), col("timestamp"))

# Print the data
query = parsedDf.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", "false") \
    .start()

# Start the query
query.awaitTermination()
