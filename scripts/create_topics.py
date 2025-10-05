# scripts/create_topics.py
import os
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# ---------- تنظیمات Kafka ----------
# اگر اسکریپت روی ویندوز اجرا میشه، از localhost استفاده کن
# اگر داخل Docker container اجرا میشه، از نام سرویس Docker
KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")  # default: localhost
KAFKA_PORT = os.getenv("KAFKA_PORT", "9092")       # default: 9092

BOOTSTRAP_SERVERS = f"{KAFKA_HOST}:{KAFKA_PORT}"
TOPIC_NAME = "new_tokens"
NUM_PARTITIONS = 1
REPLICATION_FACTOR = 1

# ---------- ساخت Kafka Admin ----------
admin_client = KafkaAdminClient(
    bootstrap_servers=BOOTSTRAP_SERVERS,
    client_id='collector_admin'
)

# ---------- ساخت Topic ----------
topic = NewTopic(
    name=TOPIC_NAME,
    num_partitions=NUM_PARTITIONS,
    replication_factor=REPLICATION_FACTOR
)

try:
    admin_client.create_topics([topic])
    print(f"Topic '{TOPIC_NAME}' created successfully on {BOOTSTRAP_SERVERS}")
except TopicAlreadyExistsError:
    print(f"Topic '{TOPIC_NAME}' already exists on {BOOTSTRAP_SERVERS}")
except Exception as e:
    print(f"Error creating topics: {e}")
