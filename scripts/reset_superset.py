import requests
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] — %(message)s')
SUPERSET_URL = "http://superset:8088"

session = requests.Session()
res = session.post(f"{SUPERSET_URL}/api/v1/security/login", json={"username": "admin", "password": "admin", "provider": "db"})
token = res.json().get("access_token")
session.headers.update({"Authorization": f"Bearer {token}"})

csrf_res = session.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/")
session.headers.update({"X-CSRFToken": csrf_res.json().get("result")})

# Delete all charts created by this project
charts_res = session.get(f"{SUPERSET_URL}/api/v1/chart/")
for chart in charts_res.json().get("result", []):
    slice_name = chart.get('slice_name', '')
    if any(keyword in slice_name for keyword in ["Auto", "KPI", "v2", "Toplam", "Maliyet", "Taksit", "Satıcı", "Durum", "Kategori", "Ödeme", "Trendi", "Aylık", "Müşteri"]):
        logging.info(f"Deleting Chart: {slice_name}")
        session.delete(f"{SUPERSET_URL}/api/v1/chart/{chart['id']}")

# Delete Virtual Datasets
ds_res = session.get(f"{SUPERSET_URL}/api/v1/dataset/")
for ds in ds_res.json().get("result", []):
    tname = ds.get("table_name", "")
    if tname in ["Master_Sales_Auto", "Master_Payments_Auto"]:
        logging.info(f"Deleting Dataset: {tname}")
        session.delete(f"{SUPERSET_URL}/api/v1/dataset/{ds['id']}")

logging.info("Deleted old assets successfully!")
