# Real-Time Multi-Source E-Commerce Data Platform

Production-style real-time data platform using Kafka, Airflow, dbt, PostgreSQL, and Docker Compose.

## Stack
- Apache Kafka + Zookeeper
- Multiple Kafka producers and consumers
- Apache Airflow
- dbt (Postgres adapter)
- PostgreSQL warehouse
- Python + JSON Schema validation

## Project Structure
```text
real-time-data-platform/
+-- docker-compose.yml
+-- .env
+-- README.md
+-- kafka/
�   +-- common.py
�   +-- producers/
�   +-- consumers/
�   +-- schemas/
+-- airflow/
�   +-- dags/
�   +-- requirements.txt
+-- dbt/
�   +-- dbt_project.yml
�   +-- profiles.yml
�   +-- models/
+-- warehouse/
�   +-- init.sql
+-- monitoring/
    +-- metrics_logger.py
```

## 1) Prerequisites
- Docker Desktop (running)
- Python 3.10+
- PowerShell terminal

## 2) Go to Project

## 3) Start Full Platform
```powershell
docker compose up -d
```

Check status:
```powershell
docker compose ps
```

## 4) Access UIs
- Kafka UI: http://localhost:8080
- Airflow UI: http://localhost:8081
  - Username: `admin`
  - Password: `admin`

## 5) Run Producers (Host Machine)
Open separate PowerShell terminals and run one command per terminal:
```powershell
cd C:\Users\sneha\Downloads\Kafka_Airflow_DBT_Project\real-time-data-platform
python -m kafka.producers.order_producer
python -m kafka.producers.payment_producer
python -m kafka.producers.user_producer
python -m kafka.producers.inventory_producer
python -m kafka.producers.product_catalog_producer
python -m kafka.producers.shipping_producer
```

## 6) Run Consumers (Host Machine)
Open separate terminals:
```powershell
cd C:\Users\sneha\Downloads\Kafka_Airflow_DBT_Project\real-time-data-platform
python -m kafka.consumers.bronze_consumer
python -m kafka.consumers.fraud_detection_consumer
python -m kafka.consumers.inventory_alert_consumer
```

## 7) Airflow DAGs
In Airflow UI:
1. Enable `bronze_to_silver_dag`
2. Enable `silver_to_gold_dag`
3. Trigger both once manually

## 8) Run dbt
```powershell
cd C:\Users\sneha\Downloads\Kafka_Airflow_DBT_Project\real-time-data-platform
docker compose exec dbt dbt debug --profiles-dir .
docker compose exec dbt dbt run --profiles-dir .
docker compose exec dbt dbt test --profiles-dir .
docker compose exec dbt dbt source freshness --profiles-dir .
```

## 9) Verify Data in PostgreSQL
```powershell
cd C:\Users\sneha\Downloads\Kafka_Airflow_DBT_Project\real-time-data-platform
docker compose exec postgres psql -U postgres -d Ecommerce -c "select count(*) from bronze.events_raw;"
docker compose exec postgres psql -U postgres -d Ecommerce -c "select count(*) from bronze.fraud_alerts;"
docker compose exec postgres psql -U postgres -d Ecommerce -c "select count(*) from bronze.inventory_alerts;"
docker compose exec postgres psql -U postgres -d Ecommerce -c "select count(*) from gold.fact_orders;"
docker compose exec postgres psql -U postgres -d Ecommerce -c "select * from monitoring.pipeline_metrics order by metric_timestamp desc limit 10;"
```

## 10) Example Analytics Queries
```powershell
docker compose exec postgres psql -U postgres -d Ecommerce -c "select * from gold.daily_revenue order by date_day desc limit 10;"
docker compose exec postgres psql -U postgres -d Ecommerce -c "select * from gold.customer_lifetime_value order by lifetime_value desc limit 10;"
docker compose exec postgres psql -U postgres -d Ecommerce -c "select * from gold.top_selling_products limit 10;"
docker compose exec postgres psql -U postgres -d Ecommerce -c "select * from gold.conversion_rate limit 10;"
```

## 11) Stop / Restart
Stop running Python producer/consumer terminals:
- Press `Ctrl + C` in each terminal

Stop containers:
```powershell
docker compose down
```

Restart containers:
```powershell
docker compose up -d
```

## 12) Full Reset (Deletes Data)
```powershell
docker compose down -v
docker compose up -d
```

## 13) Common Issues and Fixes

### A) `port is already allocated`
```powershell
docker ps
netstat -ano | findstr :9092
netstat -ano | findstr :5432
```
Use mapped ports already configured in this project:
- Kafka host: `localhost:29092`
- Postgres host: `127.0.0.1:5433`

### B) `database ... does not exist`
Use correct database/user:
- DB: `Ecommerce`
- User: `postgres`

### C) PowerShell `<` redirection error while loading SQL
Use:
```powershell
Get-Content .\warehouse\init.sql | docker compose exec -T postgres psql -U postgres -d Ecommerce
```

### D) `service "dbt" is not running`
```powershell
docker compose up -d --force-recreate dbt
docker compose ps dbt
```

### E) Consumer can connect to Kafka but not Postgres
For host-run Python, `.env` should use:
- `POSTGRES_HOST=127.0.0.1`
- `POSTGRES_PORT=5433`
- `KAFKA_BOOTSTRAP_SERVERS=localhost:29092`

## 14) Current Working Environment Values
```env
POSTGRES_USER=postgres
POSTGRES_PASSWORD=Tiger
POSTGRES_DB=Ecommerce
POSTGRES_HOST=127.0.0.1
POSTGRES_PORT=5433
KAFKA_BOOTSTRAP_SERVERS=localhost:29092
```

## 15) Notes
- Streaming producers/consumers run continuously by design.
- `bronze.events_raw` should increase continuously while producers + bronze consumer are running.
- `gold` tables depend on DAG/dbt runs and referential consistency.

## 16) One-Command Automation

Start full stack + dbt + launch all producers/consumers:
```powershell
cd C:\Users\sneha\Downloads\Kafka_Airflow_DBT_Project\real-time-data-platform
.\run_all.ps1
```

Start infra + streams but skip dbt:
```powershell
.\run_all.ps1 -SkipDbt
```

Start only infra/dbt (no producers/consumers):
```powershell
.\run_all.ps1 -SkipStreams
```

Stop everything:
```powershell
.\stop_all.ps1
```
