import asyncio
import json
import time
import structlog
from aiokafka import AIOKafkaProducer
import websockets
from websockets.exceptions import ConnectionClosedError
from prometheus_client import Counter, start_http_server
import hvac
import yaml

structlog.configure(processors=[structlog.processors.JSONRenderer()])
logger = structlog.get_logger("collector")

# Metrics
messages_processed = Counter('collector_messages_processed', 'Number of messages processed')
errors_total = Counter('collector_errors_total', 'Total errors')
reconnects_total = Counter('collector_reconnects_total', 'Total reconnect attempts')

# Load config
with open('config/collector.yaml', 'r') as f:
    CONFIG = yaml.safe_load(f)

KAFKA_BOOTSTRAP = CONFIG['kafka']['bootstrap_servers']
NEW_TOKENS_TOPIC = CONFIG['kafka']['topics']['new_tokens']
DEX_WS_URL = CONFIG['sources']['dex_ws_url']
SOL_WS_URLS = CONFIG['sources']['sol_ws_urls']
VAULT_URL = CONFIG['vault']['url']
VAULT_TOKEN = CONFIG['vault']['token']
VAULT_PATH = CONFIG['vault']['secrets_path']  # e.g., "collector"
MIN_LIQUIDITY = CONFIG['filters']['min_liquidity_usd']

# --- Vault ---
async def get_vault_secrets():
    client = hvac.Client(url=VAULT_URL, token=VAULT_TOKEN)
    if not client.is_authenticated():
        raise Exception("Vault auth failed")
    # Explicitly use raise_on_deleted_version=True to avoid deprecation warning
    secrets = client.secrets.kv.v2.read_secret_version(path=VAULT_PATH, raise_on_deleted_version=True)['data']['data']
    return secrets

# --- Kafka with retry ---
async def init_kafka_producer(max_retries=10, backoff_start=1, max_backoff=10):
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    backoff = backoff_start
    for attempt in range(1, max_retries + 1):
        try:
            await producer.start()
            logger.info("Kafka producer started")
            return producer
        except Exception as e:
            errors_total.inc()
            logger.error(f"Kafka not ready, attempt {attempt}/{max_retries}", error=str(e))
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)
    raise RuntimeError("Kafka not ready after max retries")


# --- WebSocket handler with reconnect ---
async def ws_reconnect_handler(url: str, handler_func, backoff_start=1, max_backoff=60):
    backoff = backoff_start
    while True:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=30) as ws:
                logger.info("Connected to WS", url=url)
                reconnects_total.inc(0)
                await handler_func(ws)
        except (ConnectionClosedError, Exception) as e:
            errors_total.inc()
            logger.error("WS error, reconnecting", error=str(e), url=url)
            reconnects_total.inc()
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, max_backoff)

# --- Message handling ---
async def handle_dex_messages(ws, producer: AIOKafkaProducer, secrets: dict):
    if 'x_api_key' in secrets:
        await ws.send(json.dumps({"auth": secrets['x_api_key']}))
    async for raw in ws:
        try:
            data = json.loads(raw)
            event = normalize_dex_event(data)
            if event:
                await producer.send_and_wait(NEW_TOKENS_TOPIC, json.dumps(event).encode())
                messages_processed.inc()
                logger.info("Published event", token=event.get("token"))
        except Exception as e:
            errors_total.inc()
            logger.exception("Message handling error", error=str(e))

def normalize_dex_event(raw: dict) -> dict | None:
    try:
        if raw.get("type") in ("pair_created", "add_liquidity"):
            liquidity = raw.get("liquidity_usd", 0)
            if liquidity < MIN_LIQUIDITY:
                return None
            token = raw.get("token_address") or raw.get("pair")
            return {
                "token": token,
                "event_type": raw.get("type"),
                "liquidity": liquidity,
                "timestamp": raw.get("timestamp") or int(time.time()),
                "raw": raw
            }
        return None
    except Exception:
        return None

# --- Main ---
async def main():
    secrets = await get_vault_secrets()
    producer = await init_kafka_producer()
    start_http_server(8000, addr="0.0.0.0")
    tasks = []
    try:
        tasks.append(asyncio.create_task(ws_reconnect_handler(
            DEX_WS_URL,
            lambda ws: handle_dex_messages(ws, producer, secrets)
        )))
        for sol_url in SOL_WS_URLS:
            tasks.append(asyncio.create_task(ws_reconnect_handler(
                sol_url,
                lambda ws: handle_dex_messages(ws, producer, secrets)
            )))
        await asyncio.gather(*tasks)
    finally:
        await producer.stop()

if __name__ == "__main__":
    asyncio.run(main())
