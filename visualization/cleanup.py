import requests
import json

SUPERSET_URL = "http://superset:8088"
session = requests.Session()
resp = session.post(f"{SUPERSET_URL}/api/v1/security/login", json={"username": "admin", "password": "admin", "provider": "db"})
token = resp.json().get("access_token")
session.headers.update({"Authorization": f"Bearer {token}"})
csrf = session.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/").json().get("result")
session.headers.update({"X-CSRFToken": csrf})

d_resp = session.get(f"{SUPERSET_URL}/api/v1/dashboard/")
if d_resp.ok:
    for d in d_resp.json().get("result", []):
        session.delete(f"{SUPERSET_URL}/api/v1/dashboard/{d['id']}")
        print(f"Deleted dashboard {d['id']}")

c_resp = session.get(f"{SUPERSET_URL}/api/v1/chart/")
if c_resp.ok:
    for c in c_resp.json().get("result", []):
        session.delete(f"{SUPERSET_URL}/api/v1/chart/{c['id']}")
        print(f"Deleted chart {c['id']}")
