"""
=============================================================
 PRODUCER 2 – NYC Taxi Trips (TLC Open Data)
=============================================================
 Topic Kafka : nyc_taxi_trips
 Source      : NYC TLC Trip Record Data (données publiques)
               https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
 Fichier     : Yellow Taxi Trip Records (Parquet mensuel ~300 MB)
               URL directe : voir NYC_TAXI_PARQUET_URL ci-dessous

 Mode 1 – REPLAY (défaut)  : rejoue un fichier Parquet téléchargé
 Mode 2 – SIMULATION        : génère des trips synthétiques inspirés
                              des vraies distributions NYC

 Lancement :
   pip install kafka-python pandas pyarrow requests tqdm
   python nyc_taxi_producer.py --mode replay    # télécharge et rejoue
   python nyc_taxi_producer.py --mode simulate  # 100 % synthétique
=============================================================
"""

import argparse
import json
import os
import random
import time
import uuid
from datetime import datetime, timedelta

import pandas as pd
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ─── CONFIG ──────────────────────────────────────────────
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC        = "nyc_taxi_trips"
SEND_INTERVAL = (0.2, 1.0)

# NYC TLC Open Data – Yellow Taxi janvier 2024 (fichier Parquet ~320 MB)
NYC_TAXI_PARQUET_URL = (
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet"
)
# Green Taxi (plus léger ~50 MB) :
# "https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2024-01.parquet"

LOCAL_PARQUET = "yellow_tripdata_2024-01.parquet"

# ─── ZONES NYC (extrait des 263 zones TLC) ───────────────
NYC_ZONES = {
    1:  "Newark Airport",       4:  "Alphabet City",
    7:  "Astoria",              13: "Battery Park",
    24: "Bloomingdale",         43: "Central Park",
    45: "Chelsea",              48: "Clinton East",
    50: "Clinton Hill",         68: "East Chelsea",
    79: "East Village",         87: "Financial District N",
    88: "Financial District S", 107:"GWB Bridge",
    114:"Greenwich Village N",  116:"Hamilton Heights",
    127:"Jackson Heights",      132:"JFK Airport",
    138:"LaGuardia Airport",    140:"Lenox Hill",
    142:"Lincoln Square E",     143:"Lincoln Square W",
    144:"Little Italy/NoLiTa",  151:"Manhattan Valley",
    161:"Midtown Center",       162:"Midtown East",
    163:"Midtown North",        164:"Midtown South",
    170:"Murray Hill",          186:"Penn Station/Madison Sq W",
    202:"SoHo",                 209:"Stuyvesant Heights",
    230:"Times Sq/Theatre District", 231:"TriBeCa/Civic Center",
    234:"Union Sq",             238:"Upper East Side N",
    239:"Upper East Side S",    243:"Upper West Side N",
    244:"Upper West Side S",    261:"World Trade Center",
    262:"Yorkville E",          263:"Yorkville W",
}

PAYMENT_TYPES = {1: "credit_card", 2: "cash", 3: "no_charge", 4: "dispute", 5: "unknown"}

# ─── MODE SIMULATION ─────────────────────────────────────
def simulate_trip() -> dict:
    """Génère un trip synthétique basé sur les vraies distributions NYC."""
    pickup_zone_id  = random.choice(list(NYC_ZONES.keys()))
    dropoff_zone_id = random.choice(list(NYC_ZONES.keys()))

    # Distribution réaliste des heures (peak matin + soir)
    hour_weights = [1,1,1,1,1,2, 4,6,8,7,5,5, 6,5,5,5,6,8, 9,8,7,5,3,2]
    hour = random.choices(range(24), weights=hour_weights)[0]
    minute = random.randint(0, 59)
    pickup_dt = datetime.now().replace(hour=hour, minute=minute, second=0, microsecond=0)

    # Distance & durée corrélées
    distance_miles = round(random.expovariate(0.5) + 0.3, 2)
    duration_min   = int(distance_miles * random.uniform(3, 6) + random.uniform(2, 8))
    dropoff_dt     = pickup_dt + timedelta(minutes=duration_min)

    # Tarif réaliste
    base_fare    = round(3.0 + distance_miles * 2.5, 2)
    tip          = round(base_fare * random.uniform(0, 0.25), 2) if random.random() > 0.3 else 0.0
    tolls        = round(random.choice([0, 0, 0, 6.55, 8.00]), 2)
    total_amount = round(base_fare + tip + tolls + 0.5 + 0.3, 2)  # + surcharges

    payment_type_id = random.choices([1, 2, 3, 4], weights=[65, 30, 3, 2])[0]

    return {
        "trip_id":           str(uuid.uuid4()),
        "ingestion_ts":      datetime.utcnow().isoformat() + "Z",
        "vendor_id":         random.choice([1, 2]),
        "pickup": {
            "datetime":      pickup_dt.isoformat(),
            "zone_id":       pickup_zone_id,
            "zone_name":     NYC_ZONES[pickup_zone_id],
            "longitude":     round(random.uniform(-74.05, -73.75), 6),
            "latitude":      round(random.uniform(40.63, 40.85), 6),
        },
        "dropoff": {
            "datetime":      dropoff_dt.isoformat(),
            "zone_id":       dropoff_zone_id,
            "zone_name":     NYC_ZONES[dropoff_zone_id],
            "longitude":     round(random.uniform(-74.05, -73.75), 6),
            "latitude":      round(random.uniform(40.63, 40.85), 6),
        },
        "trip": {
            "passenger_count":  random.choices([1,2,3,4,5,6], weights=[60,20,8,5,4,3])[0],
            "distance_miles":   distance_miles,
            "duration_minutes": duration_min,
            "rate_code":        random.choices([1,2,3,4,5,6], weights=[88,5,1,1,4,1])[0],
        },
        "fare": {
            "base_fare":        base_fare,
            "tip_amount":       tip,
            "tolls_amount":     tolls,
            "total_amount":     total_amount,
            "payment_type":     PAYMENT_TYPES[payment_type_id],
            "currency":         "USD",
        },
        "extra": {
            "store_and_fwd":    random.choice(["Y", "N"]),
            "congestion_surcharge": round(random.choice([0, 2.5]), 2),
        },
    }

# ─── MODE REPLAY (vraies données TLC) ────────────────────
def download_parquet(url: str, dest: str):
    import requests
    from tqdm import tqdm
    print(f"📥  Téléchargement : {url}")
    r = requests.get(url, stream=True, timeout=60)
    total = int(r.headers.get("content-length", 0))
    with open(dest, "wb") as f, tqdm(total=total, unit="B", unit_scale=True) as bar:
        for chunk in r.iter_content(chunk_size=8192):
            f.write(chunk)
            bar.update(len(chunk))
    print(f"✅  Sauvegardé → {dest}")

def replay_parquet(producer: KafkaProducer, path: str):
    print(f"📂  Chargement Parquet : {path}")
    df = pd.read_parquet(path)
    print(f"    {len(df):,} lignes · colonnes : {list(df.columns)}")
    df = df.sample(frac=1).reset_index(drop=True)  # shuffle

    for i, row in df.iterrows():
        event = {
            "trip_id":      str(uuid.uuid4()),
            "ingestion_ts": datetime.utcnow().isoformat() + "Z",
            "vendor_id":    int(row.get("VendorID", 0)),
            "pickup": {
                "datetime":  str(row.get("tpep_pickup_datetime", "")),
                "zone_id":   int(row.get("PULocationID", 0)),
                "zone_name": NYC_ZONES.get(int(row.get("PULocationID", 0)), "Unknown"),
            },
            "dropoff": {
                "datetime":  str(row.get("tpep_dropoff_datetime", "")),
                "zone_id":   int(row.get("DOLocationID", 0)),
                "zone_name": NYC_ZONES.get(int(row.get("DOLocationID", 0)), "Unknown"),
            },
            "trip": {
                "passenger_count":  int(row.get("passenger_count", 1) or 1),
                "distance_miles":   float(row.get("trip_distance", 0)),
                "duration_minutes": 0,  # calculé à l'ingestion
                "rate_code":        int(row.get("RatecodeID", 1) or 1),
            },
            "fare": {
                "base_fare":    float(row.get("fare_amount", 0)),
                "tip_amount":   float(row.get("tip_amount", 0)),
                "tolls_amount": float(row.get("tolls_amount", 0)),
                "total_amount": float(row.get("total_amount", 0)),
                "payment_type": PAYMENT_TYPES.get(int(row.get("payment_type", 5) or 5), "unknown"),
                "currency": "USD",
            },
        }
        producer.send(TOPIC, key=event["trip_id"], value=event)
        pt = event["payment_type"] if isinstance(event.get("payment_type"), str) else event["fare"]["payment_type"]
        print(f"[{i+1:>6}] 🚕 {event['pickup']['zone_name']:<28} → {event['dropoff']['zone_name']:<28} "
              f"${event['fare']['total_amount']:>7.2f}")
        time.sleep(random.uniform(*SEND_INTERVAL))

# ─── MAIN ────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="NYC Taxi Kafka Producer")
    parser.add_argument("--mode", choices=["replay", "simulate"], default="simulate",
                        help="replay = vraies données TLC | simulate = synthétique")
    parser.add_argument("--download", action="store_true",
                        help="Télécharge le fichier Parquet avant de rejouer")
    args = parser.parse_args()

    import sys
    sys.stdout.reconfigure(encoding='utf-8')
    print(f"[taxi] Connexion a Kafka ({KAFKA_BROKER}) - mode : {args.mode}")
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v, default=str, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all", retries=3,
            )
            print(f"[taxi] Connecte -- topic '{TOPIC}'")
            break
        except NoBrokersAvailable:
            print("[taxi] Kafka indisponible, retry dans 5 s ..."); time.sleep(5)

    try:
        if args.mode == "replay":
            if args.download or not os.path.exists(LOCAL_PARQUET):
                download_parquet(NYC_TAXI_PARQUET_URL, LOCAL_PARQUET)
            replay_parquet(producer, LOCAL_PARQUET)
        else:
            count = 0
            while True:
                event = simulate_trip()
                producer.send(TOPIC, key=event["trip_id"], value=event)
                count += 1
                if count % 50 == 0:
                    dist = event["trip"]["distance_miles"]
                    amt  = event["fare"]["total_amount"]
                    pu   = event["pickup"]["zone_name"]
                    print(f"[taxi] {count:>5} msgs | {pu} | {dist:.1f} mi | ${amt:.2f}")
                time.sleep(random.uniform(*SEND_INTERVAL))

    except KeyboardInterrupt:
        print("[taxi] Arret du producer NYC Taxi.")
    finally:
        producer.flush(); producer.close()

if __name__ == "__main__":
    main()
