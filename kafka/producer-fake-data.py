from kafka import KafkaProducer
import json
from time import sleep
from datetime import datetime
import random
from faker import Faker
from faker.providers import internet
from hashlib import sha256

fake = Faker()
fake.add_provider(internet)

producer = KafkaProducer(bootstrap_servers=['localhost:9092'], value_serializer=lambda v: json.dumps(v).encode('utf-8'))
for e in range(100):
    now = datetime.now()
    key = b'prediction_data'
    accuracy_value = random.uniform(0.0, 100.0)

    payload = fake.text()
    hash = sha256(payload.encode('utf-8')).hexdigest()
    ipaddress = fake.ipv4_private()
    payload = {
        'time': now.strftime('%Y-%m-%d %H:%M:%S'),
        'ipaddress': ipaddress,
        'payload' : payload,
        'score': accuracy_value,
        'hash' : hash,
        }
    producer.send('gemini-data-streaming', key=key, value=payload)
    print(payload)
    sleep(1)
