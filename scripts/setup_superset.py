"""
Configuration de Superset par API (datasets, graphiques, dashboards)
Crée les graphiques SQL et les trois dashboards (e-commerce, taxi, météo).
"""
import json, os, sys, time, subprocess, requests

SUPERSET_URL  = os.getenv("SUPERSET_URL", "http://localhost:8088")
SUPERSET_USER = os.getenv("SUPERSET_ADMIN_USER", "admin")
SUPERSET_PASS = os.environ["SUPERSET_ADMIN_PASSWORD"]

COLOR_SCHEMES = {
    "ecom":    "bnbColors",
    "taxi":    "googleCategory10c",
    "weather": "airbnb",
}

def _auth(sess):
    r = sess.post(f"{SUPERSET_URL}/api/v1/security/login", json={
        "username": SUPERSET_USER, "password": SUPERSET_PASS,
        "provider": "db", "refresh": True,
    })
    r.raise_for_status()
    sess.headers.update({"Authorization": f"Bearer {r.json()['access_token']}"})
    r2 = sess.get(f"{SUPERSET_URL}/api/v1/security/csrf_token/")
    r2.raise_for_status()
    sess.headers.update({"X-CSRFToken": r2.json()["result"], "Referer": SUPERSET_URL})

def get_db_id(sess):
    r = sess.get(f"{SUPERSET_URL}/api/v1/database/")
    ids = r.json().get("ids", [])
    return ids[0] if ids else None

def get_ds(sess, table):
    r = sess.get(f"{SUPERSET_URL}/api/v1/dataset/")
    for ds in r.json().get("result", []):
        if ds.get("table_name") == table:
            return ds["id"]
    return None

def make_chart(sess, name, viz, ds_id, params):
    payload = {
        "slice_name": name,
        "viz_type": viz,
        "datasource_id": ds_id,
        "datasource_type": "table",
        "params": json.dumps({**params, "viz_type": viz}),
    }
    r = sess.post(f"{SUPERSET_URL}/api/v1/chart/", json=payload)
    if r.ok:
        cid = r.json()["id"]
        print(f"  Chart '{name}' id={cid}")
        return cid
    print(f"  WARN {r.status_code} '{name}': {r.text[:200]}")
    return None

def make_dashboard(sess, title, chart_ids, legend_md):
    positions = {
        "ROOT_ID": {"type":"ROOT","id":"ROOT_ID","children":["GRID_ID"]},
        "GRID_ID": {"type":"GRID","id":"GRID_ID","children":["ROW-1","ROW-2","ROW-leg"],"parents":["ROOT_ID"]},
        "HEADER_ID": {"type":"HEADER","id":"HEADER_ID","meta":{"text":title}},
        "ROW-1": {
            "type":"ROW","id":"ROW-1","children":["CHART-A","CHART-B"],
            "parents":["ROOT_ID","GRID_ID"],
            "meta":{"background":"BACKGROUND_TRANSPARENT"},
        },
        "ROW-2": {
            "type":"ROW","id":"ROW-2","children":["CHART-C","CHART-D"],
            "parents":["ROOT_ID","GRID_ID"],
            "meta":{"background":"BACKGROUND_TRANSPARENT"},
        },
        "ROW-leg": {
            "type":"ROW","id":"ROW-leg","children":["MD-1"],
            "parents":["ROOT_ID","GRID_ID"],
            "meta":{"background":"BACKGROUND_TRANSPARENT"},
        },
        "MD-1": {
            "type":"MARKDOWN","id":"MD-1","children":[],
            "parents":["ROOT_ID","GRID_ID","ROW-leg"],
            "meta":{"width":12,"height":6,"code":legend_md},
        },
    }
    for key, cid in zip(["CHART-A","CHART-B","CHART-C","CHART-D"], chart_ids):
        row = "ROW-1" if key in ("CHART-A","CHART-B") else "ROW-2"
        positions[key] = {
            "type":"CHART","id":key,"children":[],
            "parents":["ROOT_ID","GRID_ID",row],
            "meta":{"chartId":cid,"width":6,"height":50},
        }
    r = sess.post(f"{SUPERSET_URL}/api/v1/dashboard/", json={
        "dashboard_title": title,
        "published": True,
        "position_json": json.dumps(positions),
        "owners": [],
    })
    r.raise_for_status()
    dash_id = r.json()["id"]
    print(f"  Dashboard '{title}' id={dash_id}")
    return dash_id

def link_flask(dash_id, chart_ids):
    ids_str = ", ".join(str(i) for i in chart_ids)
    script = "\n".join([
        "from superset.models.dashboard import Dashboard",
        "from superset.models.slice import Slice",
        "from superset.extensions import db",
        f"dash = db.session.get(Dashboard, {dash_id})",
        f"slices = [s for s in [db.session.get(Slice,i) for i in [{ids_str}]] if s]",
        "dash.slices = slices",
        "db.session.commit()",
        f"print('OK {dash_id}:', len(slices))",
    ])
    b64 = __import__("base64").b64encode(script.encode()).decode()
    result = subprocess.run(
        ["docker","exec","-e","FLASK_APP=superset","superset","bash","-c",
         f"echo {b64} | base64 -d | flask shell"],
        capture_output=True, text=True, timeout=30
    )
    if f"OK {dash_id}" in result.stdout + result.stderr:
        print(f"  Liaison dashboard {dash_id} OK")
    else:
        print(f"  WARN liaison: {(result.stdout+result.stderr)[:150]}")

def legend_md(lines):
    body = "\n".join(f"- {l}" for l in lines)
    return f"### Légende du dashboard\n\n{body}\n\n---\n*Données en temps réel — rafraîchissement automatique*"

# ─── ECOMMERCE ─────────────────────────────────────────────────
def build_ecommerce(sess, ds_id):
    charts = []

    # KPI 1 – CA par minute (Time Series custom SQL)
    charts.append(make_chart(sess,
        "Chiffre d'Affaires par minute (EUR) — transactions completed",
        "echarts_timeseries_line", ds_id, {
            "adhoc_filters": [],
            "granularity_sqla": "ingested_at",
            "time_grain_sqla": "PT1M",
            "metrics": [{
                "expressionType": "SQL",
                "sqlExpression": "SUM(total_amount)",
                "label": "CA (EUR)",
                "hasCustomLabel": True,
            }],
            "groupby": [],
            "adhoc_filters": [{
                "clause": "WHERE",
                "expressionType": "SIMPLE",
                "subject": "status",
                "operator": "==",
                "comparator": "completed",
                "filterOptionName": "f1",
            }],
            "row_limit": 10000,
            "color_scheme": "bnbColors",
            "smooth": True,
            "area": True,
            "opacity": 0.25,
            "show_legend": True,
            "rich_tooltip": True,
            "markerEnabled": True,
            "markerSize": 4,
            "y_axis_format": ",.0f",
            "x_axis_title": "Heure",
            "y_axis_title": "CA (EUR)",
            "legendType": "scroll",
        }
    ))

    # KPI 2 – Répartition CA par catégorie (Pie/Donut)
    charts.append(make_chart(sess,
        "Répartition du CA par catégorie (commandes completed)",
        "pie", ds_id, {
            "metric": {
                "expressionType": "SQL",
                "sqlExpression": "SUM(total_amount)",
                "label": "CA Total (EUR)",
                "hasCustomLabel": True,
            },
            "groupby": ["category"],
            "adhoc_filters": [
                {"clause":"WHERE","expressionType":"SIMPLE","subject":"status",
                 "operator":"==","comparator":"completed","filterOptionName":"f2"},
            ],
            "row_limit": 10,
            "color_scheme": "d3Category10",
            "show_labels": True,
            "labels_outside": True,
            "show_legend": True,
            "donut": True,
            "innerRadius": 40,
            "outerRadius": 70,
            "label_type": "key_percent",
            "legendOrientation": "right",
            "rich_tooltip": True,
        }
    ))

    # KPI 3 – Taux conversion par device (Bar)
    charts.append(make_chart(sess,
        "Taux de conversion par device (%) — 2 dernières heures",
        "dist_bar", ds_id, {
            "metrics": [{
                "expressionType": "SQL",
                "sqlExpression": "ROUND(100.0*SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END)::NUMERIC/NULLIF(COUNT(*),0),1)",
                "label": "Taux conversion (%)",
                "hasCustomLabel": True,
            }],
            "groupby": ["device"],
            "row_limit": 10,
            "order_desc": True,
            "color_scheme": "bnbColors",
            "show_legend": True,
            "y_axis_label": "Taux de conversion (%)",
            "rich_tooltip": True,
        }
    ))

    # KPI 4 – Top 10 produits (Table)
    charts.append(make_chart(sess,
        "Top 10 produits vendus — CA et unités (completed)",
        "table", ds_id, {
            "metrics": [
                {"aggregate":"SUM","column":{"column_name":"quantity"},"expressionType":"SIMPLE","label":"Unités vendues","hasCustomLabel":True},
                {"aggregate":"SUM","column":{"column_name":"total_amount"},"expressionType":"SIMPLE","label":"CA (EUR)","hasCustomLabel":True},
            ],
            "groupby": ["product_name","category"],
            "adhoc_filters": [
                {"clause":"WHERE","expressionType":"SIMPLE","subject":"status",
                 "operator":"==","comparator":"completed","filterOptionName":"f3"},
            ],
            "row_limit": 10,
            "order_desc": True,
            "show_cell_bars": True,
            "color_pn": True,
            "align_pn": True,
            "page_length": 10,
        }
    ))

    return [c for c in charts if c]

# ─── TAXI ──────────────────────────────────────────────────────
def build_taxi(sess, ds_id):
    charts = []

    # KPI 1 – Nb courses par minute (Time Series)
    charts.append(make_chart(sess,
        "Nombre de courses NYC Taxi par minute",
        "echarts_timeseries_line", ds_id, {
            "granularity_sqla": "ingested_at",
            "time_grain_sqla": "PT1M",
            "metrics": [{
                "expressionType": "SQL",
                "sqlExpression": "COUNT(*)",
                "label": "Nombre de trips",
                "hasCustomLabel": True,
            }],
            "groupby": [],
            "adhoc_filters": [],
            "row_limit": 10000,
            "color_scheme": "googleCategory10c",
            "smooth": True,
            "area": True,
            "opacity": 0.2,
            "show_legend": True,
            "rich_tooltip": True,
            "markerEnabled": True,
            "markerSize": 3,
            "y_axis_format": ",.0f",
            "x_axis_title": "Heure",
            "y_axis_title": "Nombre de courses",
        }
    ))

    # KPI 2 – Top 10 zones (Bar horizontal)
    charts.append(make_chart(sess,
        "Top 10 zones de prise en charge — volume et tarif moyen",
        "dist_bar", ds_id, {
            "metrics": [
                {"aggregate":"COUNT","column":{"column_name":"trip_id"},"expressionType":"SIMPLE","label":"Nb courses","hasCustomLabel":True},
            ],
            "groupby": ["pickup_zone"],
            "row_limit": 10,
            "order_desc": True,
            "color_scheme": "googleCategory10c",
            "show_legend": True,
            "y_axis_label": "Nombre de courses",
            "rich_tooltip": True,
        }
    ))

    # KPI 3 – Distribution tranches de tarifs (Bar via trip_category)
    charts.append(make_chart(sess,
        "Distribution des trajets par catégorie de distance",
        "dist_bar", ds_id, {
            "metrics": [
                {"aggregate":"COUNT","column":{"column_name":"trip_id"},"expressionType":"SIMPLE","label":"Nombre de courses","hasCustomLabel":True},
            ],
            "groupby": ["trip_category"],
            "row_limit": 10,
            "order_desc": True,
            "color_scheme": "d3Category10",
            "show_legend": True,
            "y_axis_label": "Nombre de courses",
            "rich_tooltip": True,
        }
    ))

    # KPI 4 – KPIs globaux (Table synthèse)
    charts.append(make_chart(sess,
        "Synthèse KPIs globaux — revenu, pourboire, distance (1h)",
        "table", ds_id, {
            "metrics": [
                {"aggregate":"COUNT","column":{"column_name":"trip_id"},"expressionType":"SIMPLE","label":"Nb courses","hasCustomLabel":True},
                {"aggregate":"SUM","column":{"column_name":"total_amount"},"expressionType":"SIMPLE","label":"Revenu total ($)","hasCustomLabel":True},
                {"aggregate":"AVG","column":{"column_name":"tip_amount"},"expressionType":"SIMPLE","label":"Pourboire moyen ($)","hasCustomLabel":True},
                {"aggregate":"AVG","column":{"column_name":"distance_miles"},"expressionType":"SIMPLE","label":"Distance moy. (mi)","hasCustomLabel":True},
                {"aggregate":"AVG","column":{"column_name":"duration_minutes"},"expressionType":"SIMPLE","label":"Durée moy. (min)","hasCustomLabel":True},
            ],
            "groupby": [],
            "row_limit": 1,
            "show_cell_bars": True,
            "color_pn": True,
            "page_length": 1,
        }
    ))

    return [c for c in charts if c]

# ─── MÉTÉO ─────────────────────────────────────────────────────
def build_weather(sess, ds_id):
    charts = []

    # KPI 1 – Températures actuelles par ville (Bar)
    charts.append(make_chart(sess,
        "Températures actuelles par ville (°C) — dernière mesure",
        "dist_bar", ds_id, {
            "metrics": [
                {"aggregate":"AVG","column":{"column_name":"temperature_c"},"expressionType":"SIMPLE","label":"Température (°C)","hasCustomLabel":True},
            ],
            "groupby": ["city"],
            "row_limit": 20,
            "order_desc": True,
            "color_scheme": "airbnb",
            "show_legend": True,
            "y_axis_label": "Température (°C)",
            "rich_tooltip": True,
        }
    ))

    # KPI 2 – Humidité par ville (Bar)
    charts.append(make_chart(sess,
        "Humidité relative par ville (%) — données en direct",
        "dist_bar", ds_id, {
            "metrics": [
                {"aggregate":"AVG","column":{"column_name":"humidity_pct"},"expressionType":"SIMPLE","label":"Humidité (%)","hasCustomLabel":True},
            ],
            "groupby": ["city"],
            "row_limit": 20,
            "order_desc": True,
            "color_scheme": "d3Category20b",
            "show_legend": True,
            "y_axis_label": "Humidité (%)",
            "rich_tooltip": True,
        }
    ))

    # KPI 3 – Précipitations et vents (Table)
    charts.append(make_chart(sess,
        "Précipitations et vents par ville — dernière observation",
        "table", ds_id, {
            "metrics": [
                {"aggregate":"AVG","column":{"column_name":"wind_speed_kmh"},"expressionType":"SIMPLE","label":"Vent moy. (km/h)","hasCustomLabel":True},
                {"aggregate":"MAX","column":{"column_name":"wind_gusts_kmh"},"expressionType":"SIMPLE","label":"Rafales max (km/h)","hasCustomLabel":True},
                {"aggregate":"SUM","column":{"column_name":"precip_mm"},"expressionType":"SIMPLE","label":"Pluie totale (mm)","hasCustomLabel":True},
                {"aggregate":"AVG","column":{"column_name":"humidity_pct"},"expressionType":"SIMPLE","label":"Humidité moy. (%)","hasCustomLabel":True},
                {"aggregate":"AVG","column":{"column_name":"pressure_hpa"},"expressionType":"SIMPLE","label":"Pression (hPa)","hasCustomLabel":True},
            ],
            "groupby": ["city","country"],
            "row_limit": 20,
            "order_desc": True,
            "show_cell_bars": True,
            "color_pn": True,
            "page_length": 15,
        }
    ))

    # KPI 4 – Conditions météo dominantes (Donut)
    charts.append(make_chart(sess,
        "Conditions météo dominantes — répartition toutes villes",
        "pie", ds_id, {
            "metric": {
                "aggregate":"COUNT","column":{"column_name":"event_id"},
                "expressionType":"SIMPLE","label":"Nb observations","hasCustomLabel":True,
            },
            "groupby": ["weather_description"],
            "row_limit": 10,
            "color_scheme": "d3Category10",
            "show_labels": True,
            "labels_outside": True,
            "show_legend": True,
            "donut": True,
            "innerRadius": 40,
            "outerRadius": 70,
            "label_type": "key_percent",
            "legendOrientation": "right",
            "rich_tooltip": True,
        }
    ))

    return [c for c in charts if c]


def main():
    sys.stdout.reconfigure(encoding="utf-8")
    sess = requests.Session()
    _auth(sess)
    print("[auth] OK")

    db_id  = get_db_id(sess)
    ds_ecom    = get_ds(sess, "ecommerce_events")
    ds_taxi    = get_ds(sess, "taxi_events")
    ds_weather = get_ds(sess, "weather_events")

    # ── E-COMMERCE ──
    print("\n=== E-COMMERCE ===")
    ecom_ids = build_ecommerce(sess, ds_ecom)
    leg_ecom = legend_md([
        "**Courbe CA** : chiffre d'affaires cumulé en euros par minute, transactions *completed* uniquement.",
        "**Donut catégories** : part de chaque catégorie dans le CA total (Électronique, Maison, Sport…).",
        "**Barres conversion** : % de commandes abouties par appareil — desktop, mobile, tablet.",
        "**Tableau Top 10** : produits classés par CA décroissant avec unités vendues et catégorie.",
    ])
    dash_ecom = make_dashboard(sess, "E-Commerce Temps Réel", ecom_ids, leg_ecom)
    link_flask(dash_ecom, ecom_ids)

    # ── TAXI ──
    print("\n=== TAXI ===")
    taxi_ids = build_taxi(sess, ds_taxi)
    leg_taxi = legend_md([
        "**Courbe courses** : nombre de trajets NYC enregistrés par minute en temps réel.",
        "**Barres zones** : top 10 zones de prise en charge par volume de courses.",
        "**Barres distances** : répartition des courses par catégorie — Court (<1mi), Moyen, Long, Très long.",
        "**Tableau KPIs** : synthèse globale — nb courses, revenu total, tarif moyen, pourboire, distance et durée.",
    ])
    dash_taxi = make_dashboard(sess, "NYC Taxi Temps Réel", taxi_ids, leg_taxi)
    link_flask(dash_taxi, taxi_ids)

    # ── MÉTÉO ──
    print("\n=== MÉTÉO ===")
    weather_ids = build_weather(sess, ds_weather)
    leg_weather = legend_md([
        "**Barres températures** : température moyenne actuelle par ville en degrés Celsius.",
        "**Barres humidité** : taux d'humidité moyen par ville (%), classé du plus humide au plus sec.",
        "**Tableau vent/pluie** : vent moyen, rafales maximales, précipitations totales, humidité et pression par ville.",
        "**Donut conditions** : répartition des conditions météo dominantes (Couvert, Ciel dégagé, Pluie…).",
    ])
    dash_weather = make_dashboard(sess, "Météo Temps Réel", weather_ids, leg_weather)
    link_flask(dash_weather, weather_ids)

    print(f"\n=== DONE ===")
    print(f"  E-Commerce : {SUPERSET_URL}/superset/dashboard/{dash_ecom}/")
    print(f"  Taxi       : {SUPERSET_URL}/superset/dashboard/{dash_taxi}/")
    print(f"  Météo      : {SUPERSET_URL}/superset/dashboard/{dash_weather}/")

if __name__ == "__main__":
    main()
