import asyncio
import json
import signal
import time
from typing import Any, Dict, Optional

import asyncpg
import structlog
import hvac
import yaml
from aiokafka import AIOKafkaConsumer
from prometheus_client import Counter, Gauge, Summary, start_http_server
import aiohttp

structlog.configure(processors=[structlog.processors.JSONRenderer()])
log = structlog.get_logger("execution_stub")

TRADES_STARTED = Counter("exec_simulated_trades_total", "Number of simulated trades started")
ROI_OBSERVATIONS = Counter("exec_roi_points_total", "ROI observation points (5m/15m/1h)")
AVG_ROI = Gauge("exec_avg_roi", "Running average ROI across all observations")

def load_config(path: str = "config/execution.yaml") -> Dict[str, Any]:
    with open(path, "r") as f:
        return yaml.safe_load(f)

async def vault_get(cfg: Dict[str, Any], secret: str, key: str) -> Optional[str]:
    v = cfg["vault"]
    client = hvac.Client(url=v["url"], token=v["token"])
    if not client.is_authenticated():
        raise RuntimeError("Vault auth failed")
    path = v["secrets_paths"][secret]
    data = client.secrets.kv.v2.read_secret_version(path=path, raise_on_deleted_version=True)["data"]["data"]
    return data.get(key)

async def db_init(pool: asyncpg.Pool):
    async with pool.acquire() as con:
        await con.execute("""
        CREATE TABLE IF NOT EXISTS signals (
          id SERIAL PRIMARY KEY,
          token TEXT NOT NULL,
          liquidity DOUBLE PRECISION,
          creator_share DOUBLE PRECISION,
          volume_5m DOUBLE PRECISION,
          timestamp TIMESTAMP DEFAULT NOW(),
          score INT,
          passed BOOLEAN DEFAULT TRUE
        );
        """)
        await con.execute("""
        CREATE TABLE IF NOT EXISTS paper_trades (
          id SERIAL PRIMARY KEY,
          token TEXT NOT NULL,
          buy_price DOUBLE PRECISION,
          current_price DOUBLE PRECISION,
          roi DOUBLE PRECISION,
          time_elapsed INTERVAL,
          status TEXT,
          created_at TIMESTAMP DEFAULT NOW(),
          updated_at TIMESTAMP DEFAULT NOW()
        );
        """)

class PriceClient:
    def __init__(self, api_key: Optional[str], chain: str, mock: Dict[str, Any]):
        self.api_key = api_key
        self.chain = chain
        self.mock = mock

    async def fetch(self, session: aiohttp.ClientSession, token_addr: str) -> Optional[float]:
        if self.mock.get("enabled"):
            return float(self.mock.get("fixed_price", 1.0))
        headers = {"X-API-KEY": self.api_key} if self.api_key else {}
        params = {"address": token_addr, "chain": self.chain}
        url = "https://public-api.birdeye.so/defi/price"
        try:
            async with session.get(url, headers=headers, params=params, timeout=5) as resp:
                if resp.status != 200:
                    log.warn("price_non_200", status=resp.status)
                    return None
                data = await resp.json()
                # Birdeye payload schema can vary; try common fields:
                price = None
                for k in ("value", "price", "amount", "data"):
                    v = data.get(k)
                    if isinstance(v, (int, float)):
                        price = float(v)
                        break
                    if isinstance(v, dict):
                        for kk in ("value","price","amount"):
                            if isinstance(v.get(kk), (int,float)):
                                price = float(v[kk])
                                break
                return price
        except Exception as e:
            log.error("price_fetch_error", err=str(e))
            return None

class RoiAverager:
    def __init__(self):
        self.n = 0
        self.sum = 0.0
    def add(self, roi: float):
        self.n += 1
        self.sum += roi
        try:
            AVG_ROI.set(self.sum / max(self.n, 1))
        except Exception:
            pass

async def send_telegram(bot_token: Optional[str], chat_id: Optional[str], text: str):
    if not bot_token or not chat_id:
        return
    url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text, "parse_mode": "HTML", "disable_web_page_preview": True}
    try:
        async with aiohttp.ClientSession() as s:
            await s.post(url, json=payload, timeout=5)
    except Exception as e:
        log.error("telegram_send_error", err=str(e))

async def handle_trade(pool: asyncpg.Pool, price_client: PriceClient, roi_windows: list[int],
                       bot_token: Optional[str], chat_id: Optional[str], msg: Dict[str, Any]):
    token = msg["token"]
    liq = msg.get("liquidity")
    cshare = msg.get("creator_share")
    vol5 = msg.get("volume_5m")

    async with aiohttp.ClientSession() as http:
        buy_price = await price_client.fetch(http, token)
    if buy_price is None:
        buy_price = 1.0  # fallback
    TRADES_STARTED.inc()

    # persist signal + opening trade
    async with pool.acquire() as con:
        await con.execute(
            "INSERT INTO signals(token, liquidity, creator_share, volume_5m, score, passed) VALUES($1,$2,$3,$4,$5,$6)",
            token, liq, cshare, vol5, msg.get("score"), True
        )
        await con.execute(
            "INSERT INTO paper_trades(token, buy_price, current_price, roi, time_elapsed, status) VALUES($1,$2,$2,0,'0 seconds','OPEN')",
            token, buy_price
        )

    await send_telegram(bot_token, chat_id,
        f"🚀 <b>New Token</b>\n💎 <b>Address:</b> <code>{token}</code>\n💰 <b>Liquidity:</b> ${liq}\n⚠️ <b>Creator Share:</b> {cshare}")

    # schedule ROI checks
    async def roi_check(delay: int):
        await asyncio.sleep(delay)
        async with aiohttp.ClientSession() as http:
            cur = await price_client.fetch(http, token)
        if cur is None:
            cur = buy_price
        roi = (cur / buy_price) - 1.0
        ROI_OBSERVATIONS.inc()
        roi_agg.add(roi)
        async with pool.acquire() as con:
            await con.execute(
                "UPDATE paper_trades SET current_price=$1, roi=$2, time_elapsed=$3, status=$4, updated_at=NOW() WHERE token=$5",
                cur, roi, f"{delay} seconds", f"T{delay}s", token
            )
        # pretty duration
        mins = delay // 60
        h = delay // 3600
        tlabel = f"{mins}m" if delay < 3600 else f"{h}h"
        sign = "📈" if roi >= 0 else "📉"
        roi_pct = round(roi * 100, 2)
        await send_telegram(bot_token, chat_id, f"{sign} ROI: {roi_pct}% in {tlabel}\n💰 Token: <code>{token}</code>")

    tasks = [asyncio.create_task(roi_check(d)) for d in roi_windows]
    await asyncio.gather(*tasks)

async def run(cfg: Dict[str, Any]):
    bs = cfg["kafka"]["bootstrap_servers"]
    topic = cfg["kafka"]["topics"]["filtered_tokens"]

    bot_token = None
    if cfg.get("telegram", {}).get("enabled"):
        bot_token = await vault_get(cfg, "telegram", "bot_token")
    chat_id = cfg.get("telegram", {}).get("chat_id")

    birdeye_key = await vault_get(cfg, "birdeye", "api_key")
    price_client = PriceClient(api_key=birdeye_key,
                               chain=cfg["price_feed"]["chain"],
                               mock=cfg["price_feed"].get("mock", {}))

    pool = await asyncpg.create_pool(dsn=cfg["db"]["dsn"], min_size=1, max_size=5)
    await db_init(pool)

    consumer = AIOKafkaConsumer(
        topic,
        bootstrap_servers=bs,
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    await consumer.start()
    log.info("execution_stub_started", topic=topic, bootstrap=bs)

    stop_event = asyncio.Event()
    def _graceful(*_): stop_event.set()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            asyncio.get_running_loop().add_signal_handler(sig, _graceful)
        except NotImplementedError:
            pass

    try:
        while not stop_event.is_set():
            batch = await consumer.getmany(timeout_ms=500, max_records=128)
            for _, msgs in batch.items():
                for m in msgs:
                    asyncio.create_task(handle_trade(
                        pool, price_client, cfg["roi_windows"], bot_token, chat_id, m.value
                    ))
            await asyncio.sleep(0)
    finally:
        await consumer.stop()
        await pool.close()
        log.info("execution_stub_stopped")

roi_agg = RoiAverager()

def main():
    cfg = load_config()
    port = cfg["prometheus"]["port"]
    start_http_server(port, addr="0.0.0.0")
    log.info("prometheus_started", port=port)
    asyncio.run(run(cfg))

if __name__ == "__main__":
    main()
