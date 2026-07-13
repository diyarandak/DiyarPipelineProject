import os
import sys
import logging
import requests

# Add project root to path so we can use utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from processing.utils import setup_logger

logger = setup_logger("SupersetRegistration")

# Superset Connection Details (from docker-compose)
SUPERSET_URL = "http://superset:8088"
USERNAME = "admin"
PASSWORD = "admin"

# Doris Connection (MySQL protocol)
DORIS_URI = "mysql://root:@doris-fe:9030/olist_gold"
DB_NAME = "Olist_Doris"

# The Gold tables we want to visualize
TABLES = [
    "dim_customers",
    "dim_sellers",
    "dim_products",
    "dim_geography",
    "dim_date",
    "fact_order_items",
    "fact_orders",
    "dim_payment_type"
]

def get_auth_session():
    """Logs into Superset API and returns a configured requests Session."""
    logger.info("Logging into Superset API...")
    session = requests.Session()
    try:
        response = session.post(f"{SUPERSET_URL}/api/v1/security/login", json={
            "username": USERNAME,
            "password": PASSWORD,
            "provider": "db"
        })
        response.raise_for_status()
        token = response.json().get("access_token")
        session.headers.update({"Authorization": f"Bearer {token}"})
        return session
    except Exception as e:
        logger.error(f"Failed to login: {e}. Is Superset running? (Check Docker)")
        return None

def get_csrf_token(session):
    """Fetches CSRF token required for POST requests."""
    response = session.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/")
    if response.ok:
        csrf = response.json().get("result")
        session.headers.update({"X-CSRFToken": csrf})
        return True
    return False

def register_database(session):
    """Creates the Spark-Iceberg database connection in Superset."""
    logger.info(f"Checking/Registering Database Connection: {DB_NAME}")
    
    # Check if DB already exists
    resp = session.get(f"{SUPERSET_URL}/api/v1/database/?q=(filters:!((col:database_name,opr:eq,value:{DB_NAME})))")
    if resp.ok and resp.json().get("count", 0) > 0:
        db_id = resp.json()["result"][0]["id"]
        logger.info(f"  -> Database '{DB_NAME}' already exists (ID: {db_id}).")
        return db_id
        
    # Create new Database connection
    payload = {
        "database_name": DB_NAME,
        "sqlalchemy_uri": DORIS_URI,
        "expose_in_sqllab": True,
        "allow_run_async": False
    }
    resp = session.post(f"{SUPERSET_URL}/api/v1/database/", json=payload)
    if resp.ok:
        db_id = resp.json().get("id")
        logger.info(f"  -> Database created successfully! (ID: {db_id})")
        return db_id
    else:
        logger.error(f"  -> Failed to create DB: {resp.text}")
        return None

def register_datasets(session, db_id):
    """Registers the Gold tables as Datasets for visualization."""
    logger.info("Registering Gold Layer Tables as Datasets...")
    for table in TABLES:
        payload = {
            "database": db_id,
            "schema": "olist_gold",
            "table_name": table
        }
        
        resp = session.post(f"{SUPERSET_URL}/api/v1/dataset/", json=payload)
        if resp.status_code == 201:
            logger.info(f"  [+] Successfully registered: {table}")
        elif resp.status_code == 422:
            logger.warning(f"  [~] Dataset already exists or validation failed: {table}. Details: {resp.text}")
        else:
            logger.error(f"  [-] Failed to register {table}: {resp.text}")

def main():
    logger.info("Starting Superset Automatic Registration Script...")
    
    session = get_auth_session()
    if not session:
        sys.exit(1)
        
    if not get_csrf_token(session):
        logger.error("Could not get CSRF token.")
        sys.exit(1)
        
    db_id = register_database(session)
    if db_id:
        register_datasets(session, db_id)
        logger.info("🎉 All tables registered! You can now build dashboards in Superset (http://localhost:8088)")
    else:
        logger.error("Aborting dataset registration due to DB error.")

if __name__ == "__main__":
    main()