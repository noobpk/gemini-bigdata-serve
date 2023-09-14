from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import json
import pandas as pd

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

# Select data
parsedDf = kafka_stream.select(col("key").cast("string"), col("value").cast("string"), col("topic"), col("partition"), col("offset"), col("timestamp"))


temp_data = []
ip_counts = {}

def analyzer_frame(frame, tmp):
    # Extract the data from the DataFrame
    df = frame.collect()
    key = df[0]["key"]
    value = df[0]["value"]

    key_str = key if key else None
    value_str = value if value else None

    if key_str == 'time_series':
        new_data = json.loads(value_str)
        temp_data.append(new_data)
        df_temp = pd.DataFrame(temp_data)

        # Count the occurrences of each IP address in the DataFrame
        ip_counts = df_temp['ip'].value_counts().reset_index()
        ip_counts.columns = ['ip', 'count']

        # Calculate the average score for each IP address in the DataFrame
        ip_avg_scores = df_temp.groupby('ip')['score'].mean().reset_index()
        ip_avg_scores.columns = ['ip', 'avg_score']

        # Convert Pandas DataFrames to PySpark DataFrames
        ip_counts_df = spark.createDataFrame(ip_counts)
        ip_avg_scores_df = spark.createDataFrame(ip_avg_scores)

        # Join the two DataFrames on the "ip" column
        joined_df = ip_counts_df.join(ip_avg_scores_df, "ip", "inner")

        # Print the joined DataFrame
        joined_df.show()

# Create a streaming query
query = parsedDf.writeStream \
    .foreachBatch(analyzer_frame) \
    .outputMode("append") \
    .option("truncate", "false") \
    .start()

# Start the query
query.awaitTermination()
