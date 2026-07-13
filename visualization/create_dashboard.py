"""
Superset Dashboard Builder — Olist Big Data Pipeline
=====================================================
Automatically creates Charts and a Dashboard in Apache Superset
using the Superset REST API. No manual drag-and-drop needed!

This script:
1. Reuses the authenticated session from register_tables.py
2. Finds the registered datasets (Gold layer tables)
3. Creates individual charts (bar, pie, line, big number)
4. Assembles them into a professional dashboard

Run AFTER register_tables.py has completed.
"""

import os
import sys
import json
import logging
import requests

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from processing.utils import setup_logger

logger = setup_logger("SupersetDashboardBuilder")

# Superset Connection (same as register_tables.py)
SUPERSET_URL = "http://superset:8088"
USERNAME = "admin"
PASSWORD = "admin"


# ============================================================
# 1. Authentication (Login + CSRF Token)
# ============================================================
def get_auth_session():
    """Logs into Superset API and returns a configured session."""
    logger.info("Logging into Superset API...")
    session = requests.Session()
    try:
        resp = session.post(f"{SUPERSET_URL}/api/v1/security/login", json={
            "username": USERNAME,
            "password": PASSWORD,
            "provider": "db"
        })
        resp.raise_for_status()
        token = resp.json().get("access_token")
        session.headers.update({"Authorization": f"Bearer {token}"})

        # Get CSRF token for POST/PUT requests
        csrf_resp = session.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/")
        if csrf_resp.ok:
            csrf = csrf_resp.json().get("result")
            session.headers.update({"X-CSRFToken": csrf})

        logger.info("  -> Login successful!")
        return session
    except Exception as e:
        logger.error(f"Login failed: {e}")
        return None


# ============================================================
# 2. Find Dataset IDs (Gold layer tables)
# ============================================================
def get_dataset_id(session, table_name):
    """Finds the Superset dataset ID for a given table name."""
    resp = session.get(
        f"{SUPERSET_URL}/api/v1/dataset/",
        params={"q": json.dumps({
            "filters": [{"col": "table_name", "opr": "eq", "value": table_name}]
        })}
    )
    if resp.ok and resp.json().get("count", 0) > 0:
        ds_id = resp.json()["result"][0]["id"]
        logger.info(f"  Found dataset '{table_name}' -> ID: {ds_id}")
        return ds_id
    logger.warning(f"  Dataset not found: {table_name}")
    return None


# ============================================================
# 3. Create Charts
# ============================================================
def create_chart(session, chart_config):
    """Creates a single chart via Superset API."""
    name = chart_config["slice_name"]
    resp = session.post(f"{SUPERSET_URL}/api/v1/chart/", json=chart_config)
    if resp.status_code == 201:
        chart_id = resp.json().get("id")
        logger.info(f"  [+] Chart created: '{name}' (ID: {chart_id})")
        return chart_id
    elif resp.status_code == 422:
        logger.warning(f"  [~] Chart already exists: '{name}'")
        return None
    else:
        logger.error(f"  [-] Failed to create chart '{name}': {resp.text}")
        return None


def build_all_charts(session):
    """Builds all charts for the Olist dashboard."""
    chart_ids = []

    # --- Find Dataset IDs ---
    ds_orders = get_dataset_id(session, "fact_orders")
    ds_payments = get_dataset_id(session, "fact_orders")
    ds_customers = get_dataset_id(session, "dim_customers")
    ds_products = get_dataset_id(session, "dim_products")
    ds_sellers = get_dataset_id(session, "dim_sellers")

    # ── Chart 1: Total Revenue (Big Number) ──
    if ds_orders:
        cid = create_chart(session, {
            "slice_name": "Total Revenue (R$)",
            "viz_type": "big_number_total",
            "datasource_id": ds_orders,
            "datasource_type": "table",
            "params": json.dumps({
                "metric": {
                    "expressionType": "SQL",
                    "sqlExpression": "SUM(total_order_value)",
                    "label": "Total Revenue"
                },
                "header_font_size": 0.4,
                "subheader_font_size": 0.15,
            })
        })
        if cid:
            chart_ids.append(cid)

    # ── Chart 2: Total Orders (Big Number) ──
    if ds_orders:
        cid = create_chart(session, {
            "slice_name": "Total Orders",
            "viz_type": "big_number_total",
            "datasource_id": ds_orders,
            "datasource_type": "table",
            "params": json.dumps({
                "metric": {
                    "expressionType": "SQL",
                    "sqlExpression": "COUNT(DISTINCT order_id)",
                    "label": "Total Orders"
                },
            })
        })
        if cid:
            chart_ids.append(cid)

    # ── Chart 3: Revenue by Month (Area Chart) ──
    if ds_orders:
        cid = create_chart(session, {
            "slice_name": "Monthly Revenue Trend",
            "viz_type": "echarts_area",
            "datasource_id": ds_orders,
            "datasource_type": "table",
            "params": json.dumps({
                "x_axis": "order_date_key",
                "time_grain_sqla": "P1M",
                "metrics": [{
                    "expressionType": "SQL",
                    "sqlExpression": "SUM(total_order_value)",
                    "label": "Revenue"
                }],
                "rich_tooltip": True,
                "show_legend": True,
                "opacity": 0.6,
                "color_scheme": "supersetColors",
            })
        })
        if cid:
            chart_ids.append(cid)

    # ── Chart 4: Orders by Status (Pie Chart) ──
    if ds_orders:
        cid = create_chart(session, {
            "slice_name": "Order Status Distribution",
            "viz_type": "pie",
            "datasource_id": ds_orders,
            "datasource_type": "table",
            "params": json.dumps({
                "groupby": ["order_status"],
                "metric": {
                    "expressionType": "SQL",
                    "sqlExpression": "COUNT(*)",
                    "label": "Order Count"
                },
                "donut": True,
                "show_labels": True,
                "label_type": "key_percent",
            })
        })
        if cid:
            chart_ids.append(cid)

    # ── Chart 5: Top Product Categories (Treemap) ──
    if ds_products:
        cid = create_chart(session, {
            "slice_name": "Product Categories (Treemap)",
            "viz_type": "treemap_v2",
            "datasource_id": ds_products,
            "datasource_type": "table",
            "params": json.dumps({
                "groupby": ["product_category"],
                "metric": {
                    "expressionType": "SQL",
                    "sqlExpression": "COUNT(*)",
                    "label": "Product Count"
                },
                "color_scheme": "supersetColors",
                "treemap_ratio": 1.618033988749895,
            })
        })
        if cid:
            chart_ids.append(cid)

    # ── Chart 6: Customers by Region (Pie) ──
    if ds_customers:
        cid = create_chart(session, {
            "slice_name": "Customers by Region (Pie)",
            "viz_type": "pie",
            "datasource_id": ds_customers,
            "datasource_type": "table",
            "params": json.dumps({
                "groupby": ["customer_region"],
                "metric": {
                    "expressionType": "SQL",
                    "sqlExpression": "COUNT(*)",
                    "label": "Customer Count"
                },
                "innerRadius": 40,
                "outerRadius": 70,
                "label_type": "key_percent",
                "color_scheme": "supersetColors",
            })
        })
        if cid:
            chart_ids.append(cid)

    # ── Chart 7: Product Categories (Word Cloud) ──
    if ds_products:
        cid = create_chart(session, {
            "slice_name": "Category Popularity (Word Cloud)",
            "viz_type": "word_cloud",
            "datasource_id": ds_products,
            "datasource_type": "table",
            "params": json.dumps({
                "series": "product_category",
                "metric": {
                    "expressionType": "SQL",
                    "sqlExpression": "COUNT(*)",
                    "label": "Product Count"
                },
                "size_from": 10,
                "size_to": 80,
                "rotation": "random",
                "color_scheme": "supersetColors",
            })
        })
        if cid:
            chart_ids.append(cid)

    # ── Chart 8: Delivery Status Distribution (Pie Chart) ──
    if ds_orders:
        cid = create_chart(session, {
            "slice_name": "Delivery Status",
            "viz_type": "pie",
            "datasource_id": ds_orders,
            "datasource_type": "table",
            "params": json.dumps({
                "groupby": ["delivery_status"],
                "metric": {
                    "expressionType": "SQL",
                    "sqlExpression": "COUNT(*)",
                    "label": "Order Count"
                },
                "donut": True,
                "show_labels": True,
                "label_type": "key_percent",
            })
        })
        if cid:
            chart_ids.append(cid)

    # ── Chart 9: Average Review Score (Big Number) ──
    if ds_orders:
        cid = create_chart(session, {
            "slice_name": "Average Review Score",
            "viz_type": "big_number_total",
            "datasource_id": ds_orders,
            "datasource_type": "table",
            "params": json.dumps({
                "metric": {
                    "expressionType": "SQL",
                    "sqlExpression": "ROUND(AVG(avg_review_score), 2)",
                    "label": "Avg Review"
                },
            })
        })
        if cid:
            chart_ids.append(cid)

    # ── Chart 10: Top Customer Cities (Table) ──
    if ds_customers:
        cid = create_chart(session, {
            "slice_name": "Top 15 Customer Cities",
            "viz_type": "table",
            "datasource_id": ds_customers,
            "datasource_type": "table",
            "params": json.dumps({
                "groupby": ["customer_city", "customer_state"],
                "metrics": [{
                    "expressionType": "SQL",
                    "sqlExpression": "COUNT(*)",
                    "label": "Total Customers"
                }],
                "row_limit": 15,
                "order_desc": True,
            })
        })
        if cid:
            chart_ids.append(cid)

    return chart_ids


# ============================================================
# 4. Create Dashboard
# ============================================================
def create_dashboard(session, chart_ids):
    """Creates a dashboard and places all charts on it."""
    dashboard_title = "Olist Analytics V2"
    slug = "olist-analytics-v2"

    logger.info(f"Creating dashboard: '{dashboard_title}'...")

    # Build the position layout (2 columns grid)
    position = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": [], "parents": ["ROOT_ID"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": dashboard_title}}
    }

    # Add each chart to the layout
    for idx, chart_id in enumerate(chart_ids):
        row_id = f"ROW-{idx // 2}"
        if row_id not in position["GRID_ID"]["children"]:
            position["GRID_ID"]["children"].append(row_id)
            position[row_id] = {
                "type": "ROW",
                "id": row_id,
                "children": [],
                "parents": ["ROOT_ID", "GRID_ID"],
                "meta": {"background": "BACKGROUND_TRANSPARENT"}
            }

        component_id = f"CHART-{chart_id}"
        position[row_id]["children"].append(component_id)
        position[component_id] = {
            "type": "CHART",
            "id": component_id,
            "children": [],
            "parents": ["ROOT_ID", "GRID_ID", row_id],
            "meta": {
                "chartId": chart_id,
                "width": 6,
                "height": 50
            }
        }

    payload = {
        "dashboard_title": dashboard_title,
        "slug": slug,
        "published": True,
        "position_json": json.dumps(position),
    }

    resp = session.post(f"{SUPERSET_URL}/api/v1/dashboard/", json=payload)
    if resp.status_code == 201:
        dash_id = resp.json().get("id")
        logger.info(f"  [+] Dashboard created! (ID: {dash_id})")
        
        # Link all charts to this dashboard
        for cid in chart_ids:
            session.put(f"{SUPERSET_URL}/api/v1/chart/{cid}", json={"dashboards": [dash_id]})
            
        logger.info(f"  🔗 URL: http://localhost:8088/superset/dashboard/{slug}/")
        return dash_id
    elif resp.status_code == 422:
        logger.warning(f"  [~] Dashboard '{dashboard_title}' already exists.")
        return None
    else:
        logger.error(f"  [-] Failed to create dashboard: {resp.text}")
        return None


# ============================================================
# 5. Main
# ============================================================
def main():
    logger.info("=" * 60)
    logger.info("  Olist Dashboard Builder — Starting...")
    logger.info("=" * 60)

    session = get_auth_session()
    if not session:
        sys.exit(1)

    # Build all charts
    chart_ids = build_all_charts(session)
    logger.info(f"\n  Created {len(chart_ids)} charts total.")

    # Assemble dashboard
    if chart_ids:
        create_dashboard(session, chart_ids)
        logger.info("\n🎉 Dashboard is ready!")
        logger.info("   Open: http://localhost:8088/superset/dashboard/olist-analytics/")
    else:
        logger.warning("No charts were created. Run register_tables.py first!")

    logger.info("=" * 60)


if __name__ == "__main__":
    main()
