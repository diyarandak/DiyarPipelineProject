import requests

SUPERSET_URL = "http://superset:8088"
session = requests.Session()
token = session.post(f"{SUPERSET_URL}/api/v1/security/login", json={"username": "admin", "password": "admin", "provider": "db"}).json().get("access_token")
session.headers.update({"Authorization": f"Bearer {token}"})
csrf = session.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/").json().get("result")
session.headers.update({"X-CSRFToken": csrf})

resp = session.get(f"{SUPERSET_URL}/api/v1/dataset/")
if resp.ok:
    for ds in resp.json().get("result", []):
        ds_id = ds["id"]
        table_name = ds["table_name"]
        refresh_resp = session.put(f"{SUPERSET_URL}/api/v1/dataset/{ds_id}/refresh")
        if refresh_resp.ok:
            print(f"Refreshed columns for {table_name}")
        else:
            print(f"Failed to refresh {table_name}: {refresh_resp.text}")
