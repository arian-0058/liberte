#!/usr/bin/env python3
"""
Create Kafka topics required for phase 2 (Windows-friendly & robust).

Strategy:
1) Try confluent_kafka.AdminClient (stable on Windows).
2) If unavailable or fails, fall back to aiokafka.admin.

Env/Args:
  BOOTSTRAP_SERVERS or --bootstrap host:port[,host2:port2]
  --topic scored_tokens (default)
"""

from __future__ import annotations

import argparse
import asyncio
import os
import re
import sys
import time
from typing import List, Optional, TYPE_CHECKING, Any

# For type checkers only (won't run at runtime)
if TYPE_CHECKING:
    from confluent_kafka.admin import AdminClient as _AdminClient
    from aiokafka.admin import AIOKafkaAdminClient as _AioAdmin

# Optional backends (runtime)
try:
    from confluent_kafka.admin import AdminClient, NewTopic  # type: ignore
except Exception:  # pragma: no cover
    AdminClient = None  # type: ignore
    NewTopic = None  # type: ignore

try:
    from aiokafka.admin import AIOKafkaAdminClient, NewTopic as AioNewTopic  # type: ignore
except Exception:  # pragma: no cover
    AIOKafkaAdminClient = None  # type: ignore
    AioNewTopic = None  # type: ignore


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Create Kafka topics for phase 2")
    p.add_argument(
        "--bootstrap",
        dest="bootstrap_servers",
        default=os.getenv("BOOTSTRAP_SERVERS", "localhost:9092"),
        help="Kafka bootstrap servers (host:port[,host2:port2])",
    )
    p.add_argument("--topic", default="scored_tokens", help="Topic name")
    p.add_argument("--partitions", type=int, default=1, help="Partitions")
    p.add_argument("--replicas", type=int, default=1, help="Replication factor")
    p.add_argument("--timeout", type=float, default=10.0, help="Metadata timeout (s)")
    p.add_argument("--retries", type=int, default=10, help="Max retries to reach broker")
    return p.parse_args(argv if argv is not None else sys.argv[1:])


_HOSTPORT_RE = re.compile(r"^[^,:]+:\d+$")


def _validate_bootstrap(bootstrap: str) -> None:
    for hp in bootstrap.split(","):
        hp = hp.strip()
        if not _HOSTPORT_RE.match(hp):
            raise SystemExit(
                f"[FATAL] Invalid --bootstrap '{hp}'. Use host:port (e.g. 127.0.0.1:9092)."
            )


# -------------------------
# Confluent path (preferred)
# -------------------------
def _confluent_wait_ready(admin: Any, timeout: float, retries: int) -> None:
    """Poll cluster metadata with backoff until it responds or we give up."""
    attempt = 0
    delay = 0.5
    last_err = None
    while attempt < retries:
        attempt += 1
        try:
            md = admin.list_topics(timeout=timeout)
            if md is not None and md.brokers:
                return
        except Exception as e:  # pragma: no cover
            last_err = e
        time.sleep(delay)
        delay = min(delay * 1.8, 5.0)
    if last_err:
        raise RuntimeError(f"Broker not reachable via confluent_kafka: {last_err}")
    raise RuntimeError("Broker not reachable via confluent_kafka (unknown error)")


def _confluent_create(
    bootstrap: str, topic: str, partitions: int, replicas: int, timeout: float, retries: int
) -> bool:
    if AdminClient is None or NewTopic is None:
        return False
    admin = AdminClient({"bootstrap.servers": bootstrap})
    _confluent_wait_ready(admin, timeout=timeout, retries=retries)

    md = admin.list_topics(timeout=timeout)
    if topic in md.topics and not md.topics[topic].error:
        print(f"[OK] Topic '{topic}' already exists (confluent).")
        return True

    new_topic = NewTopic(topic, num_partitions=partitions, replication_factor=replicas)
    futures = admin.create_topics([new_topic])
    fut = futures[topic]
    try:
        fut.result(timeout=timeout)
        print(f"[OK] Created topic '{topic}' with {partitions} partitions, {replicas} replicas (confluent).")
        return True
    except Exception as e:
        msg = str(e).lower()
        if "already exists" in msg or "topic_exists" in msg:
            print(f"[OK] Topic '{topic}' already exists (confluent).")
            return True
        raise


# ---------------------
# aiokafka path (fallback)
# ---------------------
async def _aiokafka_wait_ready(admin: Any, retries: int) -> None:
    attempt = 0
    delay = 0.5
    last_err = None
    while attempt < retries:
        attempt += 1
        try:
            topics = await admin.list_topics()
            if topics is not None:
                return
        except Exception as e:  # pragma: no cover
            last_err = e
        await asyncio.sleep(delay)
        delay = min(delay * 1.8, 5.0)
    if last_err:
        raise RuntimeError(f"Broker not reachable via aiokafka: {last_err}")
    raise RuntimeError("Broker not reachable via aiokafka (unknown error)")


async def _aiokafka_create(
    bootstrap: str, topic: str, partitions: int, replicas: int, retries: int
) -> None:
    if AIOKafkaAdminClient is None or AioNewTopic is None:
        print("[WARN] aiokafka not installed; skipping aiokafka fallback.")
        return
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap)
    await admin.start()
    try:
        await _aiokafka_wait_ready(admin, retries=retries)
        existing = await admin.list_topics()
        if topic in existing:
            print(f"[OK] Topic '{topic}' already exists (aiokafka).")
            return
        await admin.create_topics(
            [AioNewTopic(name=topic, num_partitions=partitions, replication_factor=replicas)]
        )
        print(f"[OK] Created topic '{topic}' with {partitions} partitions, {replicas} replicas (aiokafka).")
    finally:
        await admin.stop()


def main() -> None:
    args = _parse_args()
    bootstrap = args.bootstrap_servers.strip()
    _validate_bootstrap(bootstrap)

    # Windows-tip: prefer 127.0.0.1 over 'localhost' if you hit DNS errors.
    if bootstrap.lower().startswith("localhost:"):
        print("[HINT] If you see DNS/getaddrinfo errors on Windows, try --bootstrap 127.0.0.1:9092")

    # 1) Try confluent first
    try:
        ok = _confluent_create(
            bootstrap=bootstrap,
            topic=args.topic,
            partitions=args.partitions,
            replicas=args.replicas,
            timeout=args.timeout,
            retries=args.retries,
        )
        if ok:
            return
        print("[INFO] Confluent path unavailable, falling back to aiokafka...")
    except Exception as e:
        print(f"[WARN] Confluent admin failed: {e}\n[INFO] Falling back to aiokafka...")

    # 2) aiokafka fallback
    try:
        asyncio.run(
            _aiokafka_create(
                bootstrap=bootstrap,
                topic=args.topic,
                partitions=args.partitions,
                replicas=args.replicas,
                retries=args.retries,
            )
        )
    except Exception as e:
        raise SystemExit(f"[FATAL] Failed to create topic via both backends: {e}") from e


if __name__ == "__main__":  # pragma: no cover
    main()
