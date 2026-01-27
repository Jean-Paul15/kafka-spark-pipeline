"""
=============================================================
 PRODUCER 3 – Données Météo en Temps Réel
=============================================================
 Topic Kafka : weather_realtime
 API         : Open-Meteo (https://open-meteo.com/)
               ✅ GRATUITE · sans clé API · sans inscription
               ✅ Données réelles toutes les ~15 min
               ✅ Conforme RGPD · usage académique libre

 Villes suivies : 10 grandes villes françaises + NYC + Londres
 Intervalle fetch : toutes les 60 secondes (paramétrable)

 Lancement :
   pip install kafka-python requests
   python weather_producer.py

 API docs : https://open-meteo.com/en/docs
=============================================================
"""

import json
import os
import time
import uuid
from datetime import datetime

import requests
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ─── CONFIG ──────────────────────────────────────────────
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC          = "weather_realtime"
FETCH_INTERVAL = 60   # secondes entre chaque cycle complet

# ─── VILLES SURVEILLÉES ──────────────────────────────────
CITIES = [
    # France
    {"name": "Paris",        "country": "FR", "lat": 48.8566, "lon":  2.3522},
    {"name": "Lyon",         "country": "FR", "lat": 45.7640, "lon":  4.8357},
    {"name": "Marseille",    "country": "FR", "lat": 43.2965, "lon":  5.3698},
    {"name": "Bordeaux",     "country": "FR", "lat": 44.8378, "lon": -0.5792},
    {"name": "Toulouse",     "country": "FR", "lat": 43.6047, "lon":  1.4442},
    {"name": "Lille",        "country": "FR", "lat": 50.6292, "lon":  3.0573},
    {"name": "Nantes",       "country": "FR", "lat": 47.2184, "lon": -1.5536},
    {"name": "Strasbourg",   "country": "FR", "lat": 48.5734, "lon":  7.7521},
    # International
    {"name": "New York",     "country": "US", "lat": 40.7128, "lon": -74.0060},
    {"name": "London",       "country": "GB", "lat": 51.5074, "lon": -0.1278},
    {"name": "Berlin",       "country": "DE", "lat": 52.5200, "lon": 13.4050},
    {"name": "Tokyo",        "country": "JP", "lat": 35.6762, "lon": 139.6503},
]

# Variables météo demandées à l'API Open-Meteo
CURRENT_VARIABLES = [
    "temperature_2m",           # Température à 2m (°C)
    "relative_humidity_2m",     # Humidité relative (%)
    "apparent_temperature",     # Température ressentie (°C)
    "precipitation",            # Précipitations (mm)
    "rain",                     # Pluie (mm)
    "snowfall",                 # Neige (cm)
    "weather_code",             # WMO weather code
    "cloud_cover",              # Couverture nuageuse (%)
    "wind_speed_10m",           # Vent à 10m (km/h)
    "wind_direction_10m",       # Direction vent (°)
    "wind_gusts_10m",           # Rafales (km/h)
    "surface_pressure",         # Pression atmosphérique (hPa)
    "visibility",               # Visibilité (m)
    "uv_index",                 # Indice UV
    "is_day",                   # Jour/Nuit (0/1)
]

# WMO Weather Codes → description lisible
WMO_CODES = {
    0: "Ciel dégagé",         1: "Principalement dégagé",  2: "Partiellement nuageux",
    3: "Couvert",             45: "Brouillard",             48: "Brouillard givrant",
    51: "Bruine légère",      53: "Bruine modérée",         55: "Bruine dense",
    61: "Pluie légère",       63: "Pluie modérée",          65: "Pluie forte",
    71: "Neige légère",       73: "Neige modérée",          75: "Neige forte",
    77: "Grains de neige",    80: "Averses légères",        81: "Averses modérées",
    82: "Averses violentes",  85: "Averses de neige",       86: "Averses de neige fortes",
    95: "Orage",              96: "Orage avec grêle",       99: "Orage violent avec grêle",
}

def wind_direction_label(degrees: float) -> str:
    """Convertit les degrés en point cardinal."""
    dirs = ["N","NNE","NE","ENE","E","ESE","SE","SSE",
            "S","SSO","SO","OSO","O","ONO","NO","NNO"]
    return dirs[round(degrees / 22.5) % 16]

# ─── FETCH MÉTÉO RÉELLE (Open-Meteo) ─────────────────────
def fetch_weather(city: dict) -> dict | None:
    """
    Appelle l'API Open-Meteo GRATUITEMENT.
    Docs : https://open-meteo.com/en/docs
    """
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude":         city["lat"],
        "longitude":        city["lon"],
        "current":          ",".join(CURRENT_VARIABLES),
        "wind_speed_unit":  "kmh",
        "timezone":         "auto",
    }
    try:
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        curr = data["current"]

        wmo_code    = int(curr.get("weather_code", 0))
        wind_deg    = float(curr.get("wind_direction_10m", 0))
        temp        = float(curr.get("temperature_2m", 0))
        feels_like  = float(curr.get("apparent_temperature", 0))
        humidity    = int(curr.get("relative_humidity_2m", 0))
        wind_speed  = float(curr.get("wind_speed_10m", 0))
        pressure    = float(curr.get("surface_pressure", 1013))
        visibility  = float(curr.get("visibility", 10000))
        precip      = float(curr.get("precipitation", 0))
        cloud_cover = int(curr.get("cloud_cover", 0))
        wind_gusts  = float(curr.get("wind_gusts_10m", 0))
        uv_index    = float(curr.get("uv_index", 0))
        is_day      = bool(curr.get("is_day", 1))

        # Alertes automatiques
        alerts = []
        if wind_gusts > 80:     alerts.append("ALERTE_VENT_FORT")
        if temp < -5:           alerts.append("ALERTE_GRAND_FROID")
        if temp > 35:           alerts.append("ALERTE_CANICULE")
        if precip > 10:         alerts.append("ALERTE_FORTES_PLUIES")
        if visibility < 1000:   alerts.append("ALERTE_BROUILLARD")
        if wmo_code >= 95:      alerts.append("ALERTE_ORAGE")

        return {
            "event_id":      str(uuid.uuid4()),
            "fetched_at":    datetime.utcnow().isoformat() + "Z",
            "api_time":      curr.get("time", ""),
            "location": {
                "city":      city["name"],
                "country":   city["country"],
                "latitude":  city["lat"],
                "longitude": city["lon"],
                "timezone":  data.get("timezone", "UTC"),
            },
            "current": {
                "temperature_c":      temp,
                "feels_like_c":       feels_like,
                "humidity_pct":       humidity,
                "pressure_hpa":       pressure,
                "visibility_m":       visibility,
                "cloud_cover_pct":    cloud_cover,
                "uv_index":           uv_index,
                "is_day":             is_day,
                "weather_code":       wmo_code,
                "weather_description": WMO_CODES.get(wmo_code, f"Code {wmo_code}"),
            },
            "precipitation": {
                "total_mm":           precip,
                "rain_mm":            float(curr.get("rain", 0)),
                "snowfall_cm":        float(curr.get("snowfall", 0)),
            },
            "wind": {
                "speed_kmh":          wind_speed,
                "direction_deg":      wind_deg,
                "direction_label":    wind_direction_label(wind_deg),
                "gusts_kmh":          wind_gusts,
            },
            "alerts":        alerts,
            "alert_count":   len(alerts),
        }

    except requests.RequestException as e:
        print(f"  ⚠️  Erreur API pour {city['name']} : {e}")
        return None

# ─── MAIN ────────────────────────────────────────────────
def main():
    import sys
    sys.stdout.reconfigure(encoding='utf-8')
    print(f"[weather] Connexion a Kafka ({KAFKA_BROKER}) ...")
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all", retries=3,
            )
            print(f"[weather] Connecte -- topic '{TOPIC}' | {len(CITIES)} villes | intervalle {FETCH_INTERVAL}s")
            break
        except NoBrokersAvailable:
            print("[weather] Kafka indisponible, retry dans 5 s ..."); time.sleep(5)

    cycle = 0
    try:
        while True:
            cycle += 1
            ts = datetime.utcnow().strftime("%H:%M:%S")
            print(f"[weather] Cycle #{cycle} -- {ts} UTC")

            sent = 0
            for city in CITIES:
                event = fetch_weather(city)
                if event:
                    producer.send(TOPIC, key=f"{city['name']}_{cycle}", value=event)
                    sent += 1
                    time.sleep(0.5)

            producer.flush()
            print(f"[weather] {sent}/{len(CITIES)} villes publiees -- prochain fetch dans {FETCH_INTERVAL}s")
            time.sleep(FETCH_INTERVAL)

    except KeyboardInterrupt:
        print(f"[weather] Arret apres {cycle} cycles.")
    finally:
        producer.flush(); producer.close()

if __name__ == "__main__":
    main()
