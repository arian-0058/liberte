import os
import json
import time
import yaml
import asyncio
import signal
from pathlib import Path
from datetime import datetime, timezone

import structlog
from fastapi import FastAPI, Response
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRecord
import hvac

from .metrics import (
    messages_in_total, messages_out_total, errors_total,
    processing_seconds, kafka_consumer_lag, service_healthy,
    config_reload_total, config_reload_success_total, config_last_loaded_ts,
    metrics_response
)
from .roi_analyzer import analyze
from . import db as dbm

def setup_logging():
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.add_log_level,
            structlog.processors.JSONRenderer(),
        ]
    )

logger = structlog.get_logger("score_engine")

class Config:
    def __init__(self, path: str):
        self.path = Path(path)
        self.data = {}
        self.mtime = 0.0

    def load(self):
        config_reload_total.inc()
        with open(self.path, "r", encoding="utf-8") as f:
            self.data = yaml.safe_load(f) or {}
        self.mtime = self.path.stat().st_mtime
        config_reload_success_total.inc()
        config_last_loaded_ts.set(time.time())
        return self.data

    def maybe_reload(self):
        try:
            st = self.path.stat().st_mtime
            if st > self.mtime:
                logger.info("config_changed_reloading", file=str(self.path))
                self.load()
        except Exception as e:
            errors_total.inc()
            logger.exception("config_reload_failed", error=str(e))

async def hot_reload_loop(cfg: Config, interval: int):
    while True:
        await asyncio.sleep(interval)
        cfg.maybe_reload()

def vault_get(key: str, default: str = None):
    addr = os.getenv("VAULT_ADDR")
    token = os.getenv("VAULT_TOKEN")
    kv_path = os.getenv("VAULT_KV_PATH")
    if addr and token and kv_path:
        try:
            client = hvac.Client(url=addr, token=token)
            try:
                secret = client.secrets.kv.v2.read_secret_version(path=kv_path.replace("secret/data/",""))
                data = secret["data"]["data"]
            except Exception:
                secret = client.read(kv_path)
                data = secret["data"] if secret and "data" in secret else {}
            if key in data and data[key]:
                return data[key]
        except Exception as e:
            logger.warning("vault_read_failed", error=str(e))
    return os.getenv(key, default)

def _tanh_norm(x: float, scale: float = 1.0, clip: float = 5.0) -> float:
    import math
    x = max(-clip, min(clip, x * scale))
    return math.tanh(x)

def build_features(roi_res: dict) -> dict:
    feats = {}
    for tf in ("5m","15m","1h"):
        if tf in roi_res:
            feats[f"roi_{tf}"] = roi_res[tf]["roi"]
    for tf in ("1h","15m","5m"):
        if tf in roi_res:
            feats["volatility"] = roi_res[tf]["volatility"]
            feats["slope"] = roi_res[tf]["slope"]
            break
    return feats

def score_from_features(feats: dict, weights: dict, min_score=-1.0, max_score=1.0) -> float:
    n = {}
    if "roi_5m" in feats:  n["roi_5m"]  = _tanh_norm(feats["roi_5m"], scale=2.5)
    if "roi_15m" in feats: n["roi_15m"] = _tanh_norm(feats["roi_15m"], scale=2.0)
    if "roi_1h" in feats:  n["roi_1h"]  = _tanh_norm(feats["roi_1h"], scale=1.5)
    if "volatility" in feats: n["volatility"] = _tanh_norm(feats["volatility"], scale=0.5)
    if "slope" in feats:      n["slope"]      = _tanh_norm(feats["slope"], scale=0.1)

    total = 0.0
    for k, w in (weights or {}).items():
        if k in n:
            total += w * n[k]
    total = max(min_score, min(max_score, total))
    return float(total)

class Worker:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self._stop = asyncio.Event()
        self.consumer = None
        self.producer = None

    async def start(self):
        dsn = vault_get("POSTGRES_DSN")
        if not dsn:
            raise RuntimeError("POSTGRES_DSN missing (Vault/ENV)")
        await dbm.create_pool(dsn, self.cfg.data["db"]["pool_min_size"], self.cfg.data["db"]["pool_max_size"])
        await dbm.ensure_tables()

        bootstrap = vault_get("KAFKA_BOOTSTRAP_SERVERS")
        if not bootstrap:
            raise RuntimeError("KAFKA_BOOTSTRAP_SERVERS missing (Vault/ENV)")
        in_topic = self.cfg.data["kafka"]["input_topic"]
        out_topic = self.cfg.data["kafka"]["output_topic"]
        group_id = self.cfg.data["kafka"]["group_id"]
        client_id = self.cfg.data["kafka"]["client_id"]

        self.consumer = AIOKafkaConsumer(
            in_topic,
            bootstrap_servers=bootstrap,
            group_id=group_id,
            client_id=client_id,
            enable_auto_commit=False,
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            max_poll_interval_ms=300000,
            request_timeout_ms=40000,
        )
        self.producer = AIOKafkaProducer(
            bootstrap_servers=bootstrap,
            client_id=client_id + "-producer",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        await self.consumer.start()
        await self.producer.start()
        logger.info("kafka_started", in_topic=in_topic, out_topic=out_topic)
        service_healthy.set(1)

    async def stop(self):
        self._stop.set()
        try:
            if self.consumer:
                await self.consumer.stop()
            if self.producer:
                await self.producer.stop()
        finally:
            service_healthy.set(0)

    async def run(self):
        backoff = 1.0
        max_backoff = 30.0
        while not self._stop.is_set():
            try:
                async for record in self.consumer:
                    with processing_seconds.time():
                        await self.handle(record)
                        await self.consumer.commit()
                        messages_in_total.inc()
                await asyncio.sleep(0.1)
            except Exception as e:
                errors_total.inc()
                logger.exception("consume_loop_error", error=str(e))
                await asyncio.sleep(backoff)
                backoff = min(max_backoff, backoff * 2)
            else:
                backoff = 1.0

    async def handle(self, record):
        cfg = self.cfg.data
        now = datetime.now(timezone.utc)
        msg = record.value

        token = msg.get("token")
        prices = None
        if isinstance(msg.get("features"), dict):
            prices = msg["features"].get("prices")

        roi_res = {}
        if prices and isinstance(prices, list) and len(prices) >= cfg["roi"]["min_points"]:
            roi_res = analyze(prices, tuple(cfg["roi"]["timeframes"]), cfg["roi"]["min_points"])
            for tf, data in roi_res.items():
                await dbm.upsert_roi(
                    token=token,
                    timeframe=tf,
                    window_start=datetime.fromtimestamp(data["window_start"], tz=timezone.utc),
                    window_end=datetime.fromtimestamp(data["window_end"], tz=timezone.utc),
                    roi=float(data["roi"]),
                    volatility=float(data["volatility"]),
                    slope=float(data["slope"]),
                )

        feats = build_features(roi_res)
        w = cfg["weights"]
        sc_conf = cfg["scoring"]
        score = score_from_features(feats, w, sc_conf["min_score"], sc_conf["max_score"])

        await dbm.upsert_score(
            token=token,
            scored_at=now,
            score=score,
            features=feats,
            weights=w,
            config_ver=cfg.get("version", "0"),
        )

        out = {
            "token": token,
            "scored_at": now.isoformat(),
            "score": score,
            "features": feats,
            "config_ver": cfg.get("version", "0"),
            "source": "score_engine",
        }
        await self.producer.send_and_wait(self.cfg.data["kafka"]["output_topic"], out)
        messages_out_total.inc()

def make_app(worker: Worker, cfg: Config):
    app = FastAPI(title="Liberte Score Engine", version=cfg.data.get("version","2.0.0"))

    @app.get("/health")
    async def health():
        return {"ok": True, "version": cfg.data.get("version",""), "healthy": service_healthy._value.get()}

    @app.get("/metrics")
    async def prom():
        data, ctype = metrics_response()
        return Response(content=data, media_type=ctype)

    @app.get("/score/{token}")
    async def get_score(token: str):
        row = await dbm.fetch_latest_score(token)
        return {"token": token, "score": row} if row else {"token": token, "score": None}

    @app.get("/roi/{token}")
    async def get_roi(token: str):
        rows = await dbm.fetch_latest_roi(token)
        return {"token": token, "roi": rows}

    return app

async def main():
    setup_logging()
    cfg = Config(os.getenv("SCORE_ENGINE_CONFIG", "config/score_engine.yaml"))
    cfg.load()

    worker = Worker(cfg)
    await worker.start()

    hr_task = asyncio.create_task(hot_reload_loop(cfg, cfg.data["hot_reload"]["interval_seconds"]))
    run_task = asyncio.create_task(worker.run())

    import uvicorn
    app = make_app(worker, cfg)
    config = uvicorn.Config(app, host=cfg.data["server"]["host"], port=cfg.data["server"]["port"], log_level="info")
    server = uvicorn.Server(config)

    async def shutdown():
        await worker.stop()
        hr_task.cancel()
        run_task.cancel()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(shutdown()))

    await server.serve()

if __name__ == "__main__":
    asyncio.run(main())
