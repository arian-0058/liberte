Write-Host "=== Liberte Phase 1 Health Check Starting ===" -ForegroundColor Cyan

# CONFIG
$DB_QUERY_SIGNALS = "SELECT id, token, created_at FROM signals ORDER BY id DESC LIMIT 5;"
$DB_QUERY_TRADES  = "SELECT id, token, status, roi, last_updated FROM paper_trades ORDER BY id DESC LIMIT 5;"

# 1) Kafka Topics
Write-Host "`n[1] Checking Kafka topics..." -ForegroundColor Yellow
docker exec liberte-kafka-1 /usr/bin/kafka-topics --bootstrap-server kafka:9092 --list

# 2) Collector Logs
Write-Host "`n[2] Collector logs (last 5 lines):" -ForegroundColor Yellow
docker logs --tail=5 liberte-collector-1

# 3) Scanner Logs
Write-Host "`n[3] Scanner logs (last 5 lines):" -ForegroundColor Yellow
docker logs --tail=5 liberte-scanner-1

# 4) Execution Logs
Write-Host "`n[4] Execution logs (last 5 lines):" -ForegroundColor Yellow
docker logs --tail=5 liberte-execution-1

# 5) Prometheus Metrics
Write-Host "`n[5] Checking metrics endpoints..." -ForegroundColor Yellow
try {
    $collector = (Invoke-WebRequest -UseBasicParsing http://localhost:8000/metrics | Select-String "messages_processed").Line
    $scanner   = (Invoke-WebRequest -UseBasicParsing http://localhost:8001/metrics | Select-String "scanner_tokens_processed_total").Line
    $exec      = (Invoke-WebRequest -UseBasicParsing http://localhost:8002/metrics | Select-String "exec_simulated_trades_total").Line
    Write-Host "Collector metric: $collector"
    Write-Host "Scanner metric:   $scanner"
    Write-Host "Execution metric: $exec"
} catch {
    Write-Host "Warning: one or more Prometheus endpoints not reachable yet." -ForegroundColor Red
}

# 6) Database Check via Postgres container
Write-Host "`n[6] Database (signals and paper_trades):" -ForegroundColor Yellow
$dsn = "postgresql://postgres:%40123%23246%21@localhost:5432/liberte"
docker exec -i liberte-postgres-1 psql "$dsn" -c "$DB_QUERY_SIGNALS"
docker exec -i liberte-postgres-1 psql "$dsn" -c "$DB_QUERY_TRADES"

Write-Host "`n=== Health Check Complete ===" -ForegroundColor Green
