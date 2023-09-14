from json import loads
from kafka import KafkaConsumer
from pymongo import MongoClient

# Define Kafka topic and MongoDB collection
kafka_topic = 'gemini-data-streaming'
kafka_bootstrap_servers = ['localhost:9092']
mongodb_uri = 'mongodb://localhost:27017/'
mongodb_database = 'geminidb'
mongodb_collection = 'ip_predict'

# Create a Kafka consumer
consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=kafka_bootstrap_servers,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda x: loads(x.decode('utf-8'))
)

# Create a MongoDB client and select the database and collection

mongo_client = MongoClient(mongodb_uri)
db = mongo_client[mongodb_database]
collection = db[mongodb_collection]


# Consume messages from Kafka and insert into MongoDB
for message in consumer:
    message_data = message.value
    collection.insert_one(message_data)
    print('{} added to {}'.format(message_data, mongodb_collection))
