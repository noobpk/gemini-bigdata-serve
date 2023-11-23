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

# List of IP addresses
ip_list = [
    "171.138.47.143",
    "245.176.84.85",
    "63.22.250.31",
    "43.22.164.214",
    "108.25.113.49",
    "72.213.227.176",
    "247.233.69.74",
    "52.240.53.31",
    "244.143.108.123",
    "240.29.138.195",
    "43.223.179.186",
    "82.218.255.182",
    "98.196.47.14",
    "126.72.146.141",
    "199.115.165.63",
    "128.73.149.27",
    "163.170.215.41",
    "210.69.88.237",
    "211.18.251.55",
    "245.220.139.179",
    "103.227.176.58",
    "230.160.251.175",
]

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

for _ in range(100):
    now = datetime.now()
    key = b"threat_metrix"
    accuracy_value = random.uniform(0.0, 100.0)

    origin_payload = fake.text()
    hash = sha256(origin_payload.encode("utf-8")).hexdigest()
    ipaddress = random.choice(ip_list)
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
