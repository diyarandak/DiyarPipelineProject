import os
import sys
import time
import logging
import requests

# Add project root to path so we can use utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from processing.utils import setup_logger

logger = setup_logger("SupersetRegistration")

# Superset Connection Details (from docker-compose)
SUPERSET_URL = "http://localhost:8088"
USERNAME = "admin"
PASSWORD = "admin"

# Spark Thrift Server / Iceberg Connection
SPARK_URI = "hive://spark-iceberg:10000/iceberg_catalog"
DB_NAME = "Olist_Iceberg"

# The Gold tables we want to visualize
TABLES = [
    "dim_customers",
    "dim_sellers",
    "dim_products",
    "dim_geolocation",
    "dim_dates",
    "fact_order_payments",
    "fact_order_sales"
]

def get_auth_token():
    """Logs into Superset API and returns JWT token."""
    logger.info("Logging into Superset API...")
    try:
        response = requests.post(f"{SUPERSET_URL}/api/v1/security/login", json={
            "username": USERNAME,
            "password": PASSWORD,
            "provider": "db"
        })
        response.raise_for_status()
        return response.json().get("access_token")
    except Exception as e:
        logger.error(f"Failed to login: {e}. Is Superset running? (Check Docker)")
        return None

def get_csrf_token(token):
    """Fetches CSRF token required for POST requests."""
    headers = {"Authorization": f"Bearer {token}"}
    response = requests.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/", headers=headers)
    if response.ok:
        return response.json().get("result")
    return None

def register_database(token, csrf):
    """Creates the Spark-Iceberg database connection in Superset."""
    logger.info(f"Checking/Registering Database Connection: {DB_NAME}")
    headers = {
        "Authorization": f"Bearer {token}",
        "X-CSRFToken": csrf,
        "Content-Type": "application/json"
    }
    
    # Check if DB already exists
    resp = requests.get(f"{SUPERSET_URL}/api/v1/database/?q=(filters:!((col:database_name,opr:eq,value:{DB_NAME})))", headers=headers)
    if resp.ok and resp.json().get("count", 0) > 0:
        db_id = resp.json()["result"][0]["id"]
        logger.info(f"  -> Database '{DB_NAME}' already exists (ID: {db_id}).")
        return db_id
        
    # Create new Database connection
    payload = {
        "database_name": DB_NAME,
        "sqlalchemy_uri": SPARK_URI,
        "expose_in_sqllab": True,
        "allow_run_async": False
    }
    resp = requests.post(f"{SUPERSET_URL}/api/v1/database/", json=payload, headers=headers)
    if resp.ok:
        db_id = resp.json().get("id")
        logger.info(f"  -> Database created successfully! (ID: {db_id})")
        return db_id
    else:
        logger.error(f"  -> Failed to create DB: {resp.text}")
        return None

def register_datasets(token, csrf, db_id):
    """Registers the Gold tables as Datasets for visualization."""
    headers = {
        "Authorization": f"Bearer {token}",
        "X-CSRFToken": csrf,
        "Content-Type": "application/json"
    }
    
    logger.info("Registering Gold Layer Tables as Datasets...")
    for table in TABLES:
        payload = {
            "database": db_id,
            "schema": "gold",
            "table_name": table
        }
        
        resp = requests.post(f"{SUPERSET_URL}/api/v1/dataset/", json=payload, headers=headers)
        if resp.status_code == 201:
            logger.info(f"  [+] Successfully registered: {table}")
        elif resp.status_code == 422:
            logger.warning(f"  [~] Dataset already exists: {table}")
        else:
            logger.error(f"  [-] Failed to register {table}: {resp.text}")

def main():
    logger.info("Starting Superset Automatic Registration Script...")
    
    token = get_auth_token()
    if not token:
        sys.exit(1)
        
    csrf = get_csrf_token(token)
    if not csrf:
        logger.error("Could not get CSRF token.")
        sys.exit(1)
        
    db_id = register_database(token, csrf)
    if db_id:
        register_datasets(token, csrf, db_id)
        logger.info("🎉 All tables registered! You can now build dashboards in Superset (http://localhost:8088)")
    else:
        logger.error("Aborting dataset registration due to DB error.")

if __name__ == "__main__":
    main()