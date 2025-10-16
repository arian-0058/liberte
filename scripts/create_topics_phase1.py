#!/usr/bin/env python3
"""
Create Kafka topics for Phase 1 (new_tokens, filtered_tokens)
Auto-detects environment (Docker vs Host) and sets bootstrap_servers accordingly.
"""

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
import socket
import os
import time

# --- Detect environment ---
def detect_bootstrap():
    """Try to detect if running inside docker or host"""
    # Priority: environment variable
    env_bootstrap = os.getenv("BOOTSTRAP_SERVERS")
    if env_bootstrap:
        return env_bootstrap

    # Try docker hostname
    try:
        socket.gethostbyname("kafka")
        return "kafka:9092"
    except socket.error:
        return "localhost:9092"

BOOTSTRAP_SERVERS = detect_bootstrap()

# --- Topics to create ---
TOPICS = {
    "new_tokens": 1,
    "filtered_tokens": 1,
}

print(f"🔄 Using Kafka bootstrap servers: {BOOTSTRAP_SERVERS}")

# --- Try connection with retries ---
for attempt in range(10):
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=BOOTSTRAP_SERVERS,
            client_id="phase1_admin"
        )
        print("✅ Connected to Kafka broker.")
        break
    except NoBrokersAvailable:
        print(f"⏳ Kafka not ready (attempt {attempt+1}/10)...")
        time.sleep(3)
else:
    raise SystemExit("❌ Could not connect to Kafka after 10 attempts.")

# --- Create topics if not exist ---
existing_topics = set(admin_client.list_topics())
for name, partitions in TOPICS.items():
    if name in existing_topics:
        print(f"⚪ Topic '{name}' already exists — skipping.")
        continue
    try:
        topic = NewTopic(name=name, num_partitions=partitions, replication_factor=1)
        admin_client.create_topics([topic])
        print(f"✅ Created topic '{name}'.")
    except TopicAlreadyExistsError:
        print(f"⚪ Topic '{name}' already exists (race condition).")

admin_client.close()
print("🎉 All Kafka topics for Phase 1 are ready.")
