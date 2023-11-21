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

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)
for e in range(100):
    now = datetime.now()
    key = b"threat_metrix"
    accuracy_value = random.uniform(0.0, 100.0)

    origin_payload = fake.text()
    hash = sha256(origin_payload.encode("utf-8")).hexdigest()
    ipaddress = fake.ipv4_private()
    payload_threat_metrix = {
        "time": now.strftime("%Y-%m-%d %H:%M:%S"),
        "ipaddress": ipaddress,
        "origin_payload": origin_payload,
        "decode_payload": origin_payload,
        "score": accuracy_value,
        "hash": hash,
        "rbd_xss": random.choice([True, False]),
        "rbd_sqli": random.choice([True, False]),
        "rbd_unknown": random.choice([True, False]),
    }
    producer.send("gemini-data-streaming", key=key, value=payload_threat_metrix)
    print(payload_threat_metrix)
    sleep(1)
