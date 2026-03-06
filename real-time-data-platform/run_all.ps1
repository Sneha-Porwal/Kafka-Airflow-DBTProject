param(
    [switch]$SkipDbt,
    [switch]$SkipStreams
)

$ErrorActionPreference = 'Stop'

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $projectRoot

Write-Host "[1/5] Starting Docker services..." -ForegroundColor Cyan
docker compose up -d

Write-Host "[2/5] Waiting for core services..." -ForegroundColor Cyan
for ($i = 0; $i -lt 30; $i++) {
    $ps = docker compose ps --format json | ConvertFrom-Json
    $kafkaUp = ($ps | Where-Object { $_.Service -eq 'kafka' -and $_.State -eq 'running' }).Count -gt 0
    $pgUp = ($ps | Where-Object { $_.Service -eq 'postgres' -and $_.State -eq 'running' }).Count -gt 0
    if ($kafkaUp -and $pgUp) { break }
    Start-Sleep -Seconds 2
}

Write-Host "[3/5] Service status:" -ForegroundColor Cyan
docker compose ps

if (-not $SkipDbt) {
    Write-Host "[4/5] Running dbt debug/run/test..." -ForegroundColor Cyan
    docker compose exec dbt dbt debug --profiles-dir .
    docker compose exec dbt dbt run --profiles-dir .
    docker compose exec dbt dbt test --profiles-dir .
}

if (-not $SkipStreams) {
    Write-Host "[5/5] Launching producers and consumers in new PowerShell windows..." -ForegroundColor Cyan

    $commands = @(
        "python -m kafka.producers.order_producer",
        "python -m kafka.producers.payment_producer",
        "python -m kafka.producers.user_producer",
        "python -m kafka.producers.inventory_producer",
        "python -m kafka.producers.product_catalog_producer",
        "python -m kafka.producers.shipping_producer",
        "python -m kafka.consumers.bronze_consumer",
        "python -m kafka.consumers.fraud_detection_consumer",
        "python -m kafka.consumers.inventory_alert_consumer"
    )

    foreach ($cmd in $commands) {
        $full = "Set-Location '$projectRoot'; $cmd"
        Start-Process powershell -ArgumentList "-NoExit", "-Command", $full | Out-Null
        Start-Sleep -Milliseconds 300
    }
}

Write-Host "Done." -ForegroundColor Green
Write-Host "Kafka UI: http://localhost:8080"
Write-Host "Airflow UI: http://localhost:8081 (admin/admin)"
