# ============================================================
# Olist Big Data Pipeline — Makefile
# ============================================================
# Usage:
#   make setup     — Create Docker network & start all services
#   make download  — Download Olist dataset from Kaggle
#   make bronze    — Ingest CSV to Bronze layer (Iceberg)
#   make silver    — Transform Bronze → Silver (cleaned)
#   make gold      — Model Silver → Gold (Star Schema)
#   make pipeline  — Run full pipeline (bronze → silver → gold)
#   make dashboard — Register tables in Superset
#   make all       — Setup + download + pipeline + dashboard
#   make stop      — Stop all Docker services
#   make clean     — Stop services and remove volumes
# ============================================================

.PHONY: setup download bronze silver gold pipeline dashboard all stop clean test lint

# --- Variables ---
ICEBERG_PACKAGE ?= org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.4.3

# --- Infrastructure ---
setup:
	@echo "🔧 Creating Docker network..."
	bash scripts/setup_network.sh
	@echo "🐳 Starting HDFS..."
	docker compose -f docker/docker-compose-hdfs.yml up -d
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
	@echo "🟤 Running Bronze ingestion..."
	docker exec olist-dev spark-submit --packages $(ICEBERG_PACKAGE) processing/bronze_ingestion.py

silver:
	@echo "⚪ Running Silver transformation..."
	docker exec olist-dev spark-submit --packages $(ICEBERG_PACKAGE) processing/silver_transformation.py

gold:
	@echo "⭐ Running Gold modeling..."
	docker exec olist-dev spark-submit --packages $(ICEBERG_PACKAGE) processing/gold_modeling.py

pipeline: bronze silver gold
	@echo "✅ Full pipeline complete!"

dashboard:
	@echo "📊 Registering tables in Superset..."
	docker exec olist-dev python visualization/register_tables.py

all: setup download pipeline dashboard
	@echo "🎉 Everything is ready! Open http://localhost:8088 for Superset."

# --- Utilities ---
stop:
	@echo "🛑 Stopping all services..."
	-docker compose -f docker/docker-compose-airflow.yml down
	-docker compose -f docker/docker-compose-superset.yml down
	-docker compose -f docker/docker-compose-spark.yml down
	-docker compose -f docker/docker-compose-hdfs.yml down
	-docker compose -f docker/docker-compose-dev.yml down
	@echo "✅ All services stopped."

clean: stop
	@echo "🗑️  Removing volumes..."
	-docker compose -f docker/docker-compose-hdfs.yml down -v
	-docker compose -f docker/docker-compose-spark.yml down -v
	-docker compose -f docker/docker-compose-superset.yml down -v
	@echo "✅ Cleaned up."

test:
	@echo "🧪 Running tests..."
	python -m pytest tests/ -v

lint:
	@echo "🔍 Running code quality checks..."
	black --check processing/ visualization/ scripts/
	flake8 processing/ visualization/ scripts/
