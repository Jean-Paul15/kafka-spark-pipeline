"""
Grafana Dashboards Premium
E-Commerce | Taxi | Meteo — PostgreSQL
Sans emojis, avec legendes, accents, time range correct
"""

import json
import os
import urllib.request
import urllib.error
import base64

GRAFANA_URL = os.getenv("GRAFANA_URL", "http://localhost:3101")
GRAFANA_USER = os.getenv("GRAFANA_ADMIN_USER", "admin")
GRAFANA_PASS = os.environ["GRAFANA_ADMIN_PASSWORD"]
# UID de la source PostgreSQL, fixé dans config/grafana/provisioning/datasources/postgres.yml
DS_UID = os.getenv("GRAFANA_DS_UID", "bigdata-postgres")

# Donnees du 28 avril — on couvre avec now-10d a now
TIME_FROM = "now-10d"
TIME_TO   = "now"


def api_call(method, endpoint, data=None):
    url = f"{GRAFANA_URL}{endpoint}"
    token = base64.b64encode(f"{GRAFANA_USER}:{GRAFANA_PASS}".encode()).decode()
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Basic {token}"
    }
    body = json.dumps(data).encode() if data else None
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            return json.loads(resp.read().decode())
    except urllib.error.HTTPError as e:
        return json.loads(e.read().decode())


def ds():
    return {"type": "postgres", "uid": DS_UID}


def stat_panel(pid, title, sql, unit, color, x, y, w=6, h=4):
    return {
        "id": pid, "type": "stat",
        "title": title,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "options": {
            "reduceOptions": {"calcs": ["lastNotNull"]},
            "colorMode": "background",
            "graphMode": "none",
            "justifyMode": "center",
            "textMode": "auto",
            "orientation": "auto"
        },
        "fieldConfig": {
            "defaults": {
                "unit": unit,
                "color": {"mode": "fixed", "fixedColor": color},
                "thresholds": {"mode": "absolute", "steps": [{"color": color, "value": None}]},
                "mappings": []
            },
            "overrides": []
        },
        "datasource": ds(),
        "targets": [{"rawSql": sql, "format": "table", "refId": "A"}]
    }


# ═══════════════════════════════════════════════════════════════════
# DASHBOARD 1 — E-COMMERCE
# ═══════════════════════════════════════════════════════════════════

ecommerce_dashboard = {
    "dashboard": {
        "id": None,
        "uid": "ecommerce-realtime-v3",
        "title": "E-Commerce — Analyse en Temps Reel",
        "tags": ["ecommerce", "realtime", "assignment3"],
        "timezone": "browser",
        "schemaVersion": 38,
        "refresh": "30s",
        "time": {"from": TIME_FROM, "to": TIME_TO},
        "timepicker": {"refresh_intervals": ["10s", "30s", "1m", "5m", "15m"]},
        "style": "dark",
        "panels": [

            # ── KPI row ──────────────────────────────────────────
            stat_panel(1, "Chiffre d'Affaires Total (EUR)",
                "SELECT NOW() AS time, COALESCE(SUM(total_amount),0) AS val FROM ecommerce_events WHERE status='completed'",
                "currencyEUR", "green", 0, 0),

            stat_panel(2, "Nombre de Transactions",
                "SELECT NOW() AS time, COUNT(*) AS val FROM ecommerce_events",
                "short", "blue", 6, 0),

            stat_panel(3, "Taux de Conversion (%)",
                "SELECT NOW() AS time, ROUND(100.0*SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),1) AS val FROM ecommerce_events",
                "percent", "orange", 12, 0),

            stat_panel(4, "Panier Moyen (EUR)",
                "SELECT NOW() AS time, ROUND(AVG(total_amount),2) AS val FROM ecommerce_events WHERE status='completed'",
                "currencyEUR", "purple", 18, 0),

            # ── Time series CA par minute ─────────────────────────
            {
                "id": 5, "type": "timeseries",
                "title": "Evolution du Chiffre d'Affaires par Minute — Transactions completees",
                "gridPos": {"x": 0, "y": 4, "w": 14, "h": 10},
                "options": {
                    "tooltip": {"mode": "multi", "sort": "desc"},
                    "legend": {
                        "displayMode": "table",
                        "placement": "bottom",
                        "calcs": ["sum", "max", "mean"],
                        "showLegend": True
                    },
                },
                "fieldConfig": {
                    "defaults": {
                        "unit": "currencyEUR",
                        "color": {"mode": "palette-classic"},
                        "custom": {
                            "lineWidth": 2,
                            "fillOpacity": 18,
                            "gradientMode": "opacity",
                            "pointSize": 5,
                            "showPoints": "auto",
                            "spanNulls": True,
                            "lineInterpolation": "smooth",
                            "axisLabel": "CA (EUR)"
                        }
                    },
                    "overrides": [
                        {
                            "matcher": {"id": "byName", "options": "Nb Transactions"},
                            "properties": [
                                {"id": "unit", "value": "short"},
                                {"id": "custom.axisPlacement", "value": "right"},
                                {"id": "custom.axisLabel", "value": "Transactions"},
                                {"id": "color", "value": {"mode": "fixed", "fixedColor": "light-blue"}},
                                {"id": "custom.lineWidth", "value": 1},
                                {"id": "custom.lineStyle", "value": {"dash": [4, 4], "fill": "dash"}}
                            ]
                        }
                    ]
                },
                "datasource": ds(),
                "targets": [
                    {
                        "rawSql": "SELECT date_trunc('minute', ingested_at) AS time, SUM(total_amount) AS \"CA (EUR)\" FROM ecommerce_events WHERE $__timeFilter(ingested_at) AND status='completed' GROUP BY 1 ORDER BY 1",
                        "format": "time_series", "refId": "A"
                    },
                    {
                        "rawSql": "SELECT date_trunc('minute', ingested_at) AS time, COUNT(*) AS \"Nb Transactions\" FROM ecommerce_events WHERE $__timeFilter(ingested_at) GROUP BY 1 ORDER BY 1",
                        "format": "time_series", "refId": "B"
                    }
                ]
            },

            # ── Donut categories ──────────────────────────────────
            {
                "id": 6, "type": "piechart",
                "title": "Repartition du CA par Categorie de Produit",
                "gridPos": {"x": 14, "y": 4, "w": 10, "h": 10},
                "options": {
                    "pieType": "donut",
                    "tooltip": {"mode": "multi"},
                    "legend": {
                        "displayMode": "table",
                        "placement": "right",
                        "values": ["percent", "value"],
                        "showLegend": True
                    },
                    "displayLabels": ["percent"]
                },
                "fieldConfig": {
                    "defaults": {
                        "unit": "currencyEUR",
                        "color": {"mode": "palette-classic"}
                    },
                    "overrides": []
                },
                "datasource": ds(),
                "targets": [{
                    "rawSql": "SELECT category AS metric, ROUND(SUM(total_amount),2) AS value FROM ecommerce_events WHERE status='completed' AND $__timeFilter(ingested_at) GROUP BY category ORDER BY 2 DESC",
                    "format": "table", "refId": "A"
                }]
            },

            # ── Taux conversion par device ─────────────────────────
            {
                "id": 7, "type": "barchart",
                "title": "Taux de Conversion par Appareil (%)",
                "gridPos": {"x": 0, "y": 14, "w": 8, "h": 9},
                "options": {
                    "orientation": "auto",
                    "xTickLabelRotation": 0,
                    "fillOpacity": 75,
                    "gradientMode": "opacity",
                    "tooltip": {"mode": "multi"},
                    "legend": {
                        "displayMode": "list",
                        "placement": "bottom",
                        "showLegend": True
                    },
                    "xField": "Appareil"
                },
                "fieldConfig": {
                    "defaults": {
                        "unit": "percent",
                        "color": {"mode": "palette-classic"},
                        "custom": {"fillOpacity": 75, "lineWidth": 0}
                    },
                    "overrides": []
                },
                "datasource": ds(),
                "targets": [{
                    "rawSql": "SELECT device AS \"Appareil\", ROUND(100.0*SUM(CASE WHEN status='completed' THEN 1 ELSE 0 END)/NULLIF(COUNT(*),0),1) AS \"Taux de conversion (%)\" FROM ecommerce_events WHERE $__timeFilter(ingested_at) GROUP BY device ORDER BY 2 DESC",
                    "format": "table", "refId": "A"
                }]
            },

            # ── Top produits table ─────────────────────────────────
            {
                "id": 8, "type": "table",
                "title": "Top 10 Produits — CA et Unites Vendues",
                "gridPos": {"x": 8, "y": 14, "w": 16, "h": 9},
                "options": {
                    "sortBy": [{"displayName": "CA (EUR)", "desc": True}],
                    "footer": {"show": False},
                    "showHeader": True
                },
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "thresholds"},
                        "custom": {"align": "auto", "cellOptions": {"type": "auto"}, "filterable": True}
                    },
                    "overrides": [
                        {
                            "matcher": {"id": "byName", "options": "CA (EUR)"},
                            "properties": [
                                {"id": "unit", "value": "currencyEUR"},
                                {"id": "custom.cellOptions", "value": {"type": "color-background", "mode": "gradient"}},
                                {"id": "thresholds", "value": {"mode": "percentage", "steps": [
                                    {"color": "blue", "value": None},
                                    {"color": "green", "value": 50},
                                    {"color": "orange", "value": 80}
                                ]}}
                            ]
                        },
                        {
                            "matcher": {"id": "byName", "options": "Remise moy. (%)"},
                            "properties": [{"id": "unit", "value": "percent"}]
                        }
                    ]
                },
                "datasource": ds(),
                "targets": [{
                    "rawSql": "SELECT product_name AS \"Produit\", category AS \"Categorie\", SUM(quantity) AS \"Unites vendues\", ROUND(SUM(total_amount),2) AS \"CA (EUR)\", ROUND(AVG(discount_pct),1) AS \"Remise moy. (%)\" FROM ecommerce_events WHERE status='completed' AND $__timeFilter(ingested_at) GROUP BY product_name, category ORDER BY 4 DESC LIMIT 10",
                    "format": "table", "refId": "A"
                }]
            },
        ],
        "templating": {"list": []},
        "annotations": {"list": []},
        "links": []
    },
    "overwrite": True,
    "folderId": 0
}


# ═══════════════════════════════════════════════════════════════════
# DASHBOARD 2 — TAXI
# ═══════════════════════════════════════════════════════════════════

taxi_dashboard = {
    "dashboard": {
        "id": None,
        "uid": "taxi-realtime-v3",
        "title": "NYC Taxi — Mobilite Urbaine en Temps Reel",
        "tags": ["taxi", "realtime", "assignment3"],
        "timezone": "browser",
        "schemaVersion": 38,
        "refresh": "30s",
        "time": {"from": TIME_FROM, "to": TIME_TO},
        "timepicker": {"refresh_intervals": ["10s", "30s", "1m", "5m"]},
        "style": "dark",
        "panels": [

            stat_panel(1, "Courses Totales",
                "SELECT NOW() AS time, COUNT(*) AS val FROM taxi_events",
                "short", "yellow", 0, 0),

            stat_panel(2, "Revenu Total (USD)",
                "SELECT NOW() AS time, COALESCE(SUM(total_amount),0) AS val FROM taxi_events",
                "currencyUSD", "green", 6, 0),

            stat_panel(3, "Distance Moyenne (mi)",
                "SELECT NOW() AS time, ROUND(AVG(distance_miles),2) AS val FROM taxi_events",
                "lengthmi", "light-blue", 12, 0),

            stat_panel(4, "Duree Moyenne (min)",
                "SELECT NOW() AS time, ROUND(AVG(duration_minutes),1) AS val FROM taxi_events",
                "m", "orange", 18, 0),

            # ── Time series courses + revenus ─────────────────────
            {
                "id": 5, "type": "timeseries",
                "title": "Frequence des Courses et Revenu par Minute — Evolution en Temps Reel",
                "gridPos": {"x": 0, "y": 4, "w": 24, "h": 10},
                "options": {
                    "tooltip": {"mode": "multi", "sort": "desc"},
                    "legend": {
                        "displayMode": "table",
                        "placement": "bottom",
                        "calcs": ["sum", "max", "mean"],
                        "showLegend": True
                    },
                },
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "palette-classic"},
                        "custom": {
                            "lineWidth": 2,
                            "fillOpacity": 12,
                            "gradientMode": "opacity",
                            "showPoints": "auto",
                            "spanNulls": True,
                            "lineInterpolation": "smooth",
                            "pointSize": 4
                        }
                    },
                    "overrides": [
                        {
                            "matcher": {"id": "byName", "options": "Nb Courses"},
                            "properties": [
                                {"id": "color", "value": {"mode": "fixed", "fixedColor": "yellow"}},
                                {"id": "unit", "value": "short"},
                                {"id": "custom.axisLabel", "value": "Nombre de courses"}
                            ]
                        },
                        {
                            "matcher": {"id": "byName", "options": "Revenu (USD)"},
                            "properties": [
                                {"id": "color", "value": {"mode": "fixed", "fixedColor": "green"}},
                                {"id": "unit", "value": "currencyUSD"},
                                {"id": "custom.axisPlacement", "value": "right"},
                                {"id": "custom.axisLabel", "value": "Revenu ($)"}
                            ]
                        }
                    ]
                },
                "datasource": ds(),
                "targets": [
                    {
                        "rawSql": "SELECT date_trunc('minute', ingested_at) AS time, COUNT(*) AS \"Nb Courses\" FROM taxi_events WHERE $__timeFilter(ingested_at) GROUP BY 1 ORDER BY 1",
                        "format": "time_series", "refId": "A"
                    },
                    {
                        "rawSql": "SELECT date_trunc('minute', ingested_at) AS time, ROUND(SUM(total_amount),2) AS \"Revenu (USD)\" FROM taxi_events WHERE $__timeFilter(ingested_at) GROUP BY 1 ORDER BY 1",
                        "format": "time_series", "refId": "B"
                    }
                ]
            },

            # ── Top 10 zones bar chart ─────────────────────────────
            {
                "id": 6, "type": "barchart",
                "title": "Top 10 Zones de Prise en Charge — Volume et Tarif Moyen",
                "gridPos": {"x": 0, "y": 14, "w": 14, "h": 10},
                "options": {
                    "orientation": "horizontal",
                    "fillOpacity": 72,
                    "gradientMode": "opacity",
                    "tooltip": {"mode": "multi"},
                    "legend": {
                        "displayMode": "list",
                        "placement": "bottom",
                        "showLegend": True
                    },
                    "xField": "Zone"
                },
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "palette-classic"},
                        "custom": {"fillOpacity": 72}
                    },
                    "overrides": [
                        {
                            "matcher": {"id": "byName", "options": "Nb Courses"},
                            "properties": [{"id": "unit", "value": "short"}]
                        },
                        {
                            "matcher": {"id": "byName", "options": "Tarif Moyen ($)"},
                            "properties": [
                                {"id": "unit", "value": "currencyUSD"},
                                {"id": "custom.axisPlacement", "value": "right"}
                            ]
                        }
                    ]
                },
                "datasource": ds(),
                "targets": [{
                    "rawSql": "SELECT pickup_zone AS \"Zone\", COUNT(*) AS \"Nb Courses\", ROUND(AVG(total_amount),2) AS \"Tarif Moyen ($)\" FROM taxi_events WHERE $__timeFilter(ingested_at) GROUP BY pickup_zone ORDER BY 2 DESC LIMIT 10",
                    "format": "table", "refId": "A"
                }]
            },

            # ── Distribution categories trajet ─────────────────────
            {
                "id": 7, "type": "barchart",
                "title": "Distribution des Trajets par Categorie de Distance",
                "gridPos": {"x": 14, "y": 14, "w": 10, "h": 10},
                "options": {
                    "orientation": "auto",
                    "fillOpacity": 78,
                    "gradientMode": "opacity",
                    "tooltip": {"mode": "multi"},
                    "legend": {
                        "displayMode": "list",
                        "placement": "bottom",
                        "showLegend": True
                    },
                    "xField": "Categorie"
                },
                "fieldConfig": {
                    "defaults": {
                        "color": {"mode": "palette-classic"},
                        "custom": {"fillOpacity": 78}
                    },
                    "overrides": [
                        {
                            "matcher": {"id": "byName", "options": "Nb Courses"},
                            "properties": [{"id": "unit", "value": "short"}]
                        },
                        {
                            "matcher": {"id": "byName", "options": "Tarif Moyen ($)"},
                            "properties": [
                                {"id": "unit", "value": "currencyUSD"},
                                {"id": "custom.axisPlacement", "value": "right"}
                            ]
                        }
                    ]
                },
                "datasource": ds(),
                "targets": [{
                    "rawSql": "SELECT trip_category AS \"Categorie\", COUNT(*) AS \"Nb Courses\", ROUND(AVG(total_amount),2) AS \"Tarif Moyen ($)\" FROM taxi_events WHERE $__timeFilter(ingested_at) AND trip_category IS NOT NULL GROUP BY trip_category ORDER BY 2 DESC",
                    "format": "table", "refId": "A"
                }]
            },
        ],
        "templating": {"list": []},
        "annotations": {"list": []},
        "links": []
    },
    "overwrite": True,
    "folderId": 0
}


# ═══════════════════════════════════════════════════════════════════
# DASHBOARD 3 — METEO
# ═══════════════════════════════════════════════════════════════════

weather_dashboard = {
    "dashboard": {
        "id": None,
        "uid": "weather-realtime-v3",
        "title": "Meteo Mondiale — Surveillance en Temps Reel",
        "tags": ["weather", "realtime", "assignment3"],
        "timezone": "browser",
        "schemaVersion": 38,
        "refresh": "30s",
        "time": {"from": TIME_FROM, "to": TIME_TO},
        "timepicker": {"refresh_intervals": ["10s", "30s", "1m", "5m"]},
        "style": "dark",
        "panels": [

            {
                "id": 1, "type": "stat",
                "title": "Temperature Moyenne Globale (degC)",
                "gridPos": {"x": 0, "y": 0, "w": 6, "h": 4},
                "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "background", "graphMode": "none", "justifyMode": "center"},
                "fieldConfig": {
                    "defaults": {
                        "unit": "celsius", "decimals": 1,
                        "color": {"mode": "thresholds"},
                        "thresholds": {"mode": "absolute", "steps": [
                            {"color": "light-blue", "value": None},
                            {"color": "green", "value": 10},
                            {"color": "yellow", "value": 20},
                            {"color": "red", "value": 30}
                        ]},
                        "mappings": []
                    }, "overrides": []
                },
                "datasource": ds(),
                "targets": [{"rawSql": "SELECT NOW() AS time, ROUND(AVG(temperature_c),1) AS val FROM weather_events WHERE ingested_at >= NOW() - INTERVAL '30 days'", "format": "table", "refId": "A"}]
            },

            {
                "id": 2, "type": "stat",
                "title": "Humidite Moyenne (%)",
                "gridPos": {"x": 6, "y": 0, "w": 6, "h": 4},
                "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "background", "graphMode": "none", "justifyMode": "center"},
                "fieldConfig": {
                    "defaults": {
                        "unit": "percent", "decimals": 0,
                        "color": {"mode": "thresholds"},
                        "thresholds": {"mode": "absolute", "steps": [
                            {"color": "green", "value": None},
                            {"color": "yellow", "value": 60},
                            {"color": "blue", "value": 80}
                        ]},
                        "mappings": []
                    }, "overrides": []
                },
                "datasource": ds(),
                "targets": [{"rawSql": "SELECT NOW() AS time, ROUND(AVG(humidity_pct),0) AS val FROM weather_events WHERE ingested_at >= NOW() - INTERVAL '30 days'", "format": "table", "refId": "A"}]
            },

            {
                "id": 3, "type": "stat",
                "title": "Vitesse du Vent Moyenne (km/h)",
                "gridPos": {"x": 12, "y": 0, "w": 6, "h": 4},
                "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "background", "graphMode": "none", "justifyMode": "center"},
                "fieldConfig": {
                    "defaults": {
                        "unit": "velocitykmh", "decimals": 1,
                        "color": {"mode": "thresholds"},
                        "thresholds": {"mode": "absolute", "steps": [
                            {"color": "green", "value": None},
                            {"color": "yellow", "value": 30},
                            {"color": "orange", "value": 60},
                            {"color": "red", "value": 90}
                        ]},
                        "mappings": []
                    }, "overrides": []
                },
                "datasource": ds(),
                "targets": [{"rawSql": "SELECT NOW() AS time, ROUND(AVG(wind_speed_kmh),1) AS val FROM weather_events WHERE ingested_at >= NOW() - INTERVAL '30 days'", "format": "table", "refId": "A"}]
            },

            {
                "id": 4, "type": "stat",
                "title": "Villes Surveillees",
                "gridPos": {"x": 18, "y": 0, "w": 6, "h": 4},
                "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "background", "graphMode": "none", "justifyMode": "center"},
                "fieldConfig": {
                    "defaults": {
                        "unit": "short",
                        "color": {"mode": "fixed", "fixedColor": "light-blue"},
                        "thresholds": {"mode": "absolute", "steps": [{"color": "light-blue", "value": None}]},
                        "mappings": []
                    }, "overrides": []
                },
                "datasource": ds(),
                "targets": [{"rawSql": "SELECT NOW() AS time, COUNT(DISTINCT city) AS val FROM weather_events WHERE ingested_at >= NOW() - INTERVAL '30 days'", "format": "table", "refId": "A"}]
            },

            # ── Time series temperature par ville ──────────────────
            {
                "id": 5, "type": "timeseries",
                "title": "Evolution des Temperatures par Ville — Serie Temporelle (T1 a T2)",
                "gridPos": {"x": 0, "y": 4, "w": 24, "h": 11},
                "options": {
                    "tooltip": {"mode": "multi", "sort": "desc"},
                    "legend": {
                        "displayMode": "table",
                        "placement": "right",
                        "calcs": ["lastNotNull", "min", "max", "mean"],
                        "showLegend": True,
                        "width": 280
                    },
                },
                "fieldConfig": {
                    "defaults": {
                        "unit": "celsius",
                        "color": {"mode": "palette-classic"},
                        "custom": {
                            "lineWidth": 2,
                            "fillOpacity": 8,
                            "gradientMode": "opacity",
                            "showPoints": "auto",
                            "spanNulls": True,
                            "lineInterpolation": "smooth",
                            "pointSize": 5,
                            "axisLabel": "Temperature (degC)"
                        }
                    },
                    "overrides": []
                },
                "datasource": ds(),
                "targets": [{
                    "rawSql": "SELECT ingested_at AS time, city AS metric, temperature_c AS value FROM weather_events WHERE $__timeFilter(ingested_at) ORDER BY ingested_at",
                    "format": "time_series", "refId": "A"
                }]
            },

            # ── Temperatures par ville bar chart ───────────────────
            {
                "id": 6, "type": "barchart",
                "title": "Temperatures Actuelles par Ville (degC) — Derniere Mesure",
                "gridPos": {"x": 0, "y": 15, "w": 12, "h": 9},
                "options": {
                    "orientation": "horizontal",
                    "fillOpacity": 75,
                    "gradientMode": "opacity",
                    "tooltip": {"mode": "multi"},
                    "legend": {
                        "displayMode": "list",
                        "placement": "bottom",
                        "showLegend": True
                    },
                    "xField": "Ville"
                },
                "fieldConfig": {
                    "defaults": {
                        "unit": "celsius",
                        "color": {"mode": "continuous-BlYlRd"},
                        "custom": {"fillOpacity": 75}
                    },
                    "overrides": [
                        {
                            "matcher": {"id": "byName", "options": "Humidite (%)"},
                            "properties": [
                                {"id": "unit", "value": "percent"},
                                {"id": "custom.axisPlacement", "value": "right"}
                            ]
                        }
                    ]
                },
                "datasource": ds(),
                "targets": [{
                    "rawSql": "SELECT DISTINCT ON (city) city AS \"Ville\", temperature_c AS \"Temperature (degC)\", humidity_pct AS \"Humidite (%)\" FROM weather_events ORDER BY city, ingested_at DESC",
                    "format": "table", "refId": "A"
                }]
            },

            # ── Conditions pie chart ───────────────────────────────
            {
                "id": 7, "type": "piechart",
                "title": "Distribution des Conditions Meteorologiques",
                "gridPos": {"x": 12, "y": 15, "w": 12, "h": 9},
                "options": {
                    "pieType": "donut",
                    "tooltip": {"mode": "multi"},
                    "legend": {
                        "displayMode": "table",
                        "placement": "right",
                        "values": ["percent", "value"],
                        "showLegend": True
                    },
                    "displayLabels": ["name", "percent"]
                },
                "fieldConfig": {
                    "defaults": {
                        "unit": "short",
                        "color": {"mode": "palette-classic"}
                    },
                    "overrides": []
                },
                "datasource": ds(),
                "targets": [{
                    "rawSql": "SELECT weather_description AS metric, COUNT(*) AS value FROM weather_events WHERE $__timeFilter(ingested_at) AND weather_description IS NOT NULL GROUP BY weather_description ORDER BY 2 DESC LIMIT 10",
                    "format": "table", "refId": "A"
                }]
            },

            # ── Precipitations et vents table ──────────────────────
            {
                "id": 8, "type": "table",
                "title": "Precipitations, Vents et Pression par Ville — Derniere Observation",
                "gridPos": {"x": 0, "y": 24, "w": 24, "h": 8},
                "options": {
                    "sortBy": [{"displayName": "Vent (km/h)", "desc": True}],
                    "footer": {"show": False},
                    "showHeader": True
                },
                "fieldConfig": {
                    "defaults": {
                        "custom": {"align": "center", "cellOptions": {"type": "auto"}, "filterable": True}
                    },
                    "overrides": [
                        {
                            "matcher": {"id": "byName", "options": "Temperature (degC)"},
                            "properties": [
                                {"id": "unit", "value": "celsius"},
                                {"id": "custom.cellOptions", "value": {"type": "color-background", "mode": "gradient"}},
                                {"id": "thresholds", "value": {"mode": "absolute", "steps": [
                                    {"color": "light-blue", "value": None},
                                    {"color": "green", "value": 10},
                                    {"color": "yellow", "value": 20},
                                    {"color": "red", "value": 30}
                                ]}}
                            ]
                        },
                        {
                            "matcher": {"id": "byName", "options": "Vent (km/h)"},
                            "properties": [{"id": "unit", "value": "velocitykmh"}]
                        },
                        {
                            "matcher": {"id": "byName", "options": "Pression (hPa)"},
                            "properties": [{"id": "unit", "value": "pressurebar"}]
                        },
                        {
                            "matcher": {"id": "byName", "options": "Precipitations (mm)"},
                            "properties": [{"id": "unit", "value": "lengthmm"}]
                        }
                    ]
                },
                "datasource": ds(),
                "targets": [{
                    "rawSql": "SELECT DISTINCT ON (city) city AS \"Ville\", country AS \"Pays\", temperature_c AS \"Temperature (degC)\", wind_speed_kmh AS \"Vent (km/h)\", wind_gusts_kmh AS \"Rafales (km/h)\", precip_mm AS \"Precipitations (mm)\", pressure_hpa AS \"Pression (hPa)\", weather_description AS \"Condition\" FROM weather_events ORDER BY city, ingested_at DESC",
                    "format": "table", "refId": "A"
                }]
            },
        ],
        "templating": {"list": []},
        "annotations": {"list": []},
        "links": []
    },
    "overwrite": True,
    "folderId": 0
}


def create_all():
    for name, payload in [("E-Commerce", ecommerce_dashboard), ("Taxi", taxi_dashboard), ("Meteo", weather_dashboard)]:
        r = api_call("POST", "/api/dashboards/db", payload)
        if r.get("status") == "success":
            print("OK {} -> http://localhost:3000{}".format(name, r.get("url", "")))
        else:
            print("ERREUR {} : {}".format(name, r))


if __name__ == "__main__":
    create_all()
