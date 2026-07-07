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

    # Fetch Datasets to get IDs
    ds_res = session.get(f"{SUPERSET_URL}/api/v1/dataset/")
    sales_id = None
    payments_id = None
    
    for ds in ds_res.json().get("result", []):
        if ds.get("table_name") == "Master_Sales_Auto":
            sales_id = ds.get("id")
        elif ds.get("table_name") == "Master_Payments_Auto":
            payments_id = ds.get("id")
            
    if not sales_id or not payments_id:
        logging.error("Could not find datasets. Run create_charts.py first.")
        return

    # Create Extra Charts (5 to 10)
    logging.info("Creating the remaining 6 charts...")
    charts = [
        {
            "slice_name": "5. Toplam Müşteri (KPI)",
            "viz_type": "big_number_total",
            "datasource_id": sales_id,
            "datasource_type": "table",
            "params": json.dumps({
                "metric": {
                    "expressionType": "SQL",
                    "sqlExpression": "COUNT(DISTINCT customer_unique_id)",
                    "label": "Toplam Müşteri"
                }
            })
        },
        {
            "slice_name": "6. Aylık Satış Trendi (Zaman Serisi)",
            "viz_type": "echarts_timeseries_line",
            "datasource_id": sales_id,
            "datasource_type": "table",
            "params": json.dumps({
                "x_axis": "order_purchase_timestamp",
                "time_grain_sqla": "P1M", # Monthly
                "metrics": [{
                    "expressionType": "SQL",
                    "sqlExpression": "SUM(price)",
                    "label": "Aylık Ciro"
                }],
                "x_axis_title": "Tarih",
                "y_axis_title": "Ciro (BRL)",
                "color_scheme": "supersetColors"
            })
        },
        {
            "slice_name": "7. Sipariş Durumları (Pie Chart)",
            "viz_type": "pie",
            "datasource_id": sales_id,
            "datasource_type": "table",
            "params": json.dumps({
                "groupby": ["order_status"],
                "metric": {
                    "expressionType": "SQL",
                    "sqlExpression": "COUNT(DISTINCT order_id)",
                    "label": "Sipariş Adedi"
                },
                "innerRadius": 40, # Make it a donut chart
                "labels_outside": True
            })
        },
        {
            "slice_name": "8. En Başarılı Satıcı Şehirleri (Bar Chart)",
            "viz_type": "echarts_timeseries_bar",
            "datasource_id": sales_id,
            "datasource_type": "table",
            "params": json.dumps({
                "x_axis": "seller_city",
                "metrics": [{
                    "expressionType": "SQL",
                    "sqlExpression": "SUM(price)",
                    "label": "Toplam Satış"
                }],
                "sort_series_type": "sum",
                "sort_series_ascending": False,
                "row_limit": 10,
                "x_axis_title": "Satıcı Şehri",
                "y_axis_title": "Satış Tutarı (BRL)"
            })
        },
        {
            "slice_name": "9. Ödeme Tercihleri (Pie Chart)",
            "viz_type": "pie",
            "datasource_id": payments_id, # Using the Payments dataset!
            "datasource_type": "table",
            "params": json.dumps({
                "groupby": ["payment_type"],
                "metric": {
                    "expressionType": "SQL",
                    "sqlExpression": "COUNT(order_id)",
                    "label": "İşlem Sayısı"
                },
                "innerRadius": 0
            })
        },
        {
            "slice_name": "10. Taksit Dağılımı (Histogram)",
            "viz_type": "echarts_timeseries_bar",
            "datasource_id": payments_id,
            "datasource_type": "table",
            "params": json.dumps({
                "x_axis": "payment_installments",
                "metrics": [{
                    "expressionType": "SQL",
                    "sqlExpression": "COUNT(order_id)",
                    "label": "İşlem Sayısı"
                }],
                "x_axis_title": "Taksit Sayısı",
                "y_axis_title": "Tercih Edilme Sıklığı"
            })
        }
    ]

    for chart in charts:
        res = session.post(f"{SUPERSET_URL}/api/v1/chart/", json=chart)
        if res.status_code in (200, 201):
            logging.info(f"✅ Created Chart: {chart['slice_name']}")
        else:
            logging.warning(f"❌ Failed to create {chart['slice_name']}: {res.text}")

    logging.info("🎉 Done! All 10 charts are now ready in Superset!")

if __name__ == "__main__":
    main()
