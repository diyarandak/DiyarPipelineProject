import requests
import json
import logging
from time import sleep

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] AutoCharts — %(message)s')

SUPERSET_URL = "http://superset:8088"
USERNAME = "admin"
PASSWORD = "admin"

def main():
    session = requests.Session()
    
    # 1. Login
    logging.info("Logging into Superset...")
    login_url = f"{SUPERSET_URL}/api/v1/security/login"
    res = session.post(login_url, json={"username": USERNAME, "password": PASSWORD, "provider": "db"})
    if res.status_code != 200:
        logging.error("Failed to login.")
        return
    token = res.json().get("access_token")
    session.headers.update({"Authorization": f"Bearer {token}"})

    # Fetch CSRF Token
    csrf_res = session.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/")
    if csrf_res.status_code == 200:
        csrf_token = csrf_res.json().get("result")
        session.headers.update({"X-CSRFToken": csrf_token})

    # 2. Get Database ID
    db_id = 2 # Hardcoded from register_tables.py success output
    
    # 3. Create Datasets
    logging.info("Creating Virtual Datasets...")
    datasets = {
        "Master_Sales_Auto": {
            "database": db_id,
            "schema": "gold",
            "table_name": "Master_Sales_Auto",
            "sql": "SELECT f.order_id, f.customer_id, c.customer_unique_id, f.order_status, f.order_purchase_timestamp, f.order_delivered_customer_date, f.order_estimated_delivery_date, f.price, f.freight_value, f.avg_review_score, c.customer_city, c.customer_state, p.product_category_name, s.seller_city, s.seller_state, geo.avg_latitude as customer_lat, geo.avg_longitude as customer_lon FROM fact_order_sales f LEFT JOIN dim_customers c ON f.customer_id = c.customer_id LEFT JOIN dim_products p ON f.product_id = p.product_id LEFT JOIN dim_sellers s ON f.seller_id = s.seller_id LEFT JOIN dim_geolocation geo ON c.customer_zip_code_prefix = geo.geolocation_zip_code_prefix"
        },
        "Master_Payments_Auto": {
            "database": db_id,
            "schema": "gold",
            "table_name": "Master_Payments_Auto",
            "sql": "SELECT * FROM fact_order_payments"
        }
    }
    
    dataset_ids = {}
    for ds_name, ds_payload in datasets.items():
        res = session.post(f"{SUPERSET_URL}/api/v1/dataset/", json=ds_payload)
        if res.status_code in (200, 201):
            ds_id = res.json().get("id")
            dataset_ids[ds_name] = ds_id
            logging.info(f"✅ Created Dataset: {ds_name} (ID: {ds_id})")
        elif res.status_code == 422 and "already exists" in res.text:
            # If exists, we need to fetch its ID
            logging.info(f"Dataset {ds_name} already exists. Fetching its ID...")
            ds_res = session.get(f"{SUPERSET_URL}/api/v1/dataset/")
            for ds in ds_res.json().get("result", []):
                if ds.get("table_name") == ds_name:
                    dataset_ids[ds_name] = ds.get("id")
        else:
            logging.error(f"❌ Failed to create Dataset {ds_name}: {res.text}")
            
    sales_id = dataset_ids.get("Master_Sales_Auto")
    payments_id = dataset_ids.get("Master_Payments_Auto")
    
    if not sales_id:
        logging.error("Sales dataset ID missing, aborting charts.")
        return

    # 4. Create Charts
    logging.info("Creating awesome charts...")
    charts = [
        {
            "slice_name": "1. Toplam Ciro (KPI) v2",
            "viz_type": "big_number_total",
            "datasource_id": sales_id,
            "datasource_type": "table",
            "params": json.dumps({
                "metric": {
                    "expressionType": "SQL",
                    "sqlExpression": "SUM(price)",
                    "label": "Toplam Ciro"
                },
                "header_font_size": 0.4,
                "subheader_font_size": 0.15
            })
        },
        {
            "slice_name": "2. Toplam Sipariş Sayısı (KPI) v2",
            "viz_type": "big_number_total",
            "datasource_id": sales_id,
            "datasource_type": "table",
            "params": json.dumps({
                "metric": {
                    "expressionType": "SQL",
                    "sqlExpression": "COUNT(DISTINCT order_id)",
                    "label": "Sipariş Sayısı"
                }
            })
        },
        {
            "slice_name": "3. Kategori Kralları (Top 10) v2",
            "viz_type": "echarts_timeseries_bar",
            "datasource_id": sales_id,
            "datasource_type": "table",
            "params": json.dumps({
                "x_axis": "product_category_name",
                "metrics": [{
                    "expressionType": "SQL",
                    "sqlExpression": "SUM(price)",
                    "label": "Ciro"
                }],
                "orientation": "horizontal",
                "sort_series_type": "sum",
                "sort_series_ascending": False,
                "row_limit": 10
            })
        },
        {
            "slice_name": "4. Şehirlere Göre Kargo Maliyeti v2",
            "viz_type": "echarts_timeseries_bar",
            "datasource_id": sales_id,
            "datasource_type": "table",
            "params": json.dumps({
                "x_axis": "customer_state",
                "metrics": [{
                    "expressionType": "SQL",
                    "sqlExpression": "AVG(freight_value)",
                    "label": "Ortalama Kargo"
                }],
                "orientation": "vertical",
                "sort_series_type": "sum",
                "sort_series_ascending": False,
                "row_limit": 15
            })
        }
    ]

    for chart in charts:
        res = session.post(f"{SUPERSET_URL}/api/v1/chart/", json=chart)
        if res.status_code in (200, 201):
            logging.info(f"✅ Created Chart: {chart['slice_name']}")
        else:
            logging.warning(f"❌ Failed to create {chart['slice_name']}: {res.text}")

    logging.info("🎉 Done! All charts have been automatically generated. Go check the 'Charts' tab in Superset!")

if __name__ == "__main__":
    main()
