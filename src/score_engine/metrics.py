import time
from prometheus_client import Counter, Histogram, Gauge, CollectorRegistry, CONTENT_TYPE_LATEST, generate_latest

REGISTRY = CollectorRegistry()

def ns(name: str) -> str:
    return name

# Counters
messages_in_total = Counter(ns("score_engine_messages_in_total"), "Total input messages", registry=REGISTRY)
messages_out_total = Counter(ns("score_engine_messages_out_total"), "Total output messages", registry=REGISTRY)
errors_total = Counter(ns("score_engine_errors_total"), "Total errors", registry=REGISTRY)
db_upserts_total = Counter(ns("score_engine_db_upserts_total"), "Total DB upserts", registry=REGISTRY)
config_reload_total = Counter(ns("score_engine_config_reload_total"), "Total config reload attempts", registry=REGISTRY)
config_reload_success_total = Counter(ns("score_engine_config_reload_success_total"), "Total successful config reloads", registry=REGISTRY)
roi_computations_total = Counter(ns("roi_analyzer_computations_total"), "Total ROI computations", registry=REGISTRY)

# Histograms (seconds)
processing_seconds = Histogram(ns("score_engine_processing_seconds"), "End-to-end processing time per message (s)", registry=REGISTRY)
roi_seconds = Histogram(ns("roi_analyzer_seconds"), "ROI analyzer compute time (s)", registry=REGISTRY)
db_seconds = Histogram(ns("score_engine_db_seconds"), "DB operations time (s)", registry=REGISTRY)

# Gauges
config_last_loaded_ts = Gauge(ns("score_engine_config_last_loaded_timestamp"), "Last successful config load unix ts", registry=REGISTRY)
kafka_consumer_lag = Gauge(ns("score_engine_kafka_consumer_lag"), "Kafka consumer lag (approx)", ["topic", "partition"], registry=REGISTRY)
service_healthy = Gauge(ns("score_engine_service_healthy"), "1=healthy,0=unhealthy", registry=REGISTRY)

def metrics_response():
    return generate_latest(REGISTRY), CONTENT_TYPE_LATEST
