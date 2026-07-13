# ============================================================
# Olist Big Data Pipeline — Makefile (Modern Data Stack Edition)
# ============================================================
# Usage:
#   make setup     — Create Docker network & start all services (incl. Doris)
#   make download  — Download Olist dataset from Kaggle
#   make bronze    — Ingest CSV to Bronze layer (HDFS via Spark)
#   make dbt-run   — Transform Bronze → Silver → Gold (via dbt)
#   make pipeline  — Run full pipeline (bronze + dbt)
#   make dashboard — Register tables in Superset
#   make all       — Setup + download + pipeline + dashboard
#   make stop      — Stop all Docker services
#   make clean     — Stop services and remove volumes
# ============================================================

.PHONY: setup download bronze dbt-run dbt-test pipeline dashboard all stop clean lint

# --- Variables ---
ICEBERG_PACKAGE ?= org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.3

# --- Infrastructure ---
setup:
	@echo "🔧 Creating Docker network..."
	bash scripts/setup_network.sh
	@echo "🐳 Starting HDFS..."
	docker compose -f docker/docker-compose-hdfs.yml up -d
	@echo "🗄️ Starting Apache Doris..."
	docker compose -f docker/docker-compose-doris.yml up -d
	@echo "⚡ Starting Spark..."
	docker compose -f docker/docker-compose-spark.yml up -d
	@echo "📊 Starting Superset..."
	docker compose -f docker/docker-compose-superset.yml up -d
	@echo "🔄 Starting Airflow..."
	docker compose -f docker/docker-compose-airflow.yml up -d
	@echo "🛠️  Starting Dev container..."
	docker compose -f docker/docker-compose-dev.yml up -d
	@echo "✅ All services are up!"

# --- Data Pipeline ---
download:
	@echo "📥 Downloading Olist dataset from Kaggle..."
	docker exec olist-dev python scripts/download_dataset.py

bronze:
	@echo "🟤 Running Bronze ingestion (PySpark)..."
	docker exec olist-dev spark-submit --packages $(ICEBERG_PACKAGE) processing/bronze_ingestion.py

dbt-run:
	@echo "⚪⭐ Running dbt transformations (Silver & Gold)..."
	cd olist_dbt && dbt deps && dbt run

dbt-test:
	@echo "🧪 Running dbt data quality tests..."
	cd olist_dbt && dbt test

pipeline: bronze dbt-run dbt-test
	@echo "✅ Full pipeline complete!"

dashboard:
	@echo "📊 Registering tables in Superset..."
	docker exec olist-dev python visualization/register_tables.py
	@echo "📈 Creating charts and dashboard..."
	docker exec olist-dev python visualization/create_dashboard.py

all: setup download pipeline dashboard
	@echo "🎉 Everything is ready! Open http://localhost:8088/superset/dashboard/olist-analytics/"

# --- Utilities ---
stop:
	@echo "🛑 Stopping all services..."
	-docker compose -f docker/docker-compose-airflow.yml down
	-docker compose -f docker/docker-compose-superset.yml down
	-docker compose -f docker/docker-compose-doris.yml down
	-docker compose -f docker/docker-compose-spark.yml down
	-docker compose -f docker/docker-compose-hdfs.yml down
	-docker compose -f docker/docker-compose-dev.yml down
	@echo "✅ All services stopped."

clean: stop
	@echo "🗑️  Removing volumes..."
	-docker compose -f docker/docker-compose-doris.yml down -v
	-docker compose -f docker/docker-compose-hdfs.yml down -v
	-docker compose -f docker/docker-compose-spark.yml down -v
	-docker compose -f docker/docker-compose-superset.yml down -v
	@echo "✅ Cleaned up."

lint:
	@echo "🔍 Running code quality checks..."
	black --check processing/ scripts/
	flake8 processing/ scripts/
