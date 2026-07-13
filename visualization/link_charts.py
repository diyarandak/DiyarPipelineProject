import requests
import json

SUPERSET_URL = "http://superset:8088"
session = requests.Session()
token = session.post(f"{SUPERSET_URL}/api/v1/security/login", json={"username": "admin", "password": "admin", "provider": "db"}).json().get("access_token")
session.headers.update({"Authorization": f"Bearer {token}"})
csrf = session.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/").json().get("result")
session.headers.update({"X-CSRFToken": csrf})

d_resp = session.get(f"{SUPERSET_URL}/api/v1/dashboard/")
if not d_resp.ok or len(d_resp.json().get("result", [])) == 0:
    print("No dashboards found")
    exit(1)
dash_id = d_resp.json()["result"][0]["id"]
print(f"Dashboard ID: {dash_id}")

c_resp = session.get(f"{SUPERSET_URL}/api/v1/chart/")
if c_resp.ok:
    for c in c_resp.json().get("result", []):
        chart_id = c["id"]
        update_resp = session.put(f"{SUPERSET_URL}/api/v1/chart/{chart_id}", json={"dashboards": [dash_id]})
        if update_resp.ok:
            print(f"Linked chart {chart_id} to dashboard {dash_id}")
        else:
            print(f"Failed to link chart {chart_id}: {update_resp.text}")
