"""
=============================================================
 PRODUCER 1 – Transactions E-Commerce Simulées
=============================================================
 Topic Kafka : ecommerce_transactions
 Fréquence   : 1 message / 0.5–2 s (configurable)
 Données     : 100 % synthétiques (Faker + random)

 Lancement :
   pip install kafka-python faker
   python ecommerce_producer.py
=============================================================
"""

import json
import os
import random
import time
import uuid
from datetime import datetime

from faker import Faker
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# ─── CONFIG ──────────────────────────────────────────────
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC         = "ecommerce_transactions"
SEND_INTERVAL = (0.5, 2.0)         # secondes entre chaque message
NUM_MESSAGES  = None               # None = infini

fake = Faker("fr_FR")

# ─── DONNÉES DE RÉFÉRENCE ────────────────────────────────
CATEGORIES = {
    "Électronique":   ["Smartphone Samsung", "iPhone 15", "MacBook Pro", "AirPods",
                       "Tablette iPad", "Écouteurs Sony", "Montre connectée Garmin"],
    "Mode":           ["Sneakers Nike", "Veste Zara", "Robe H&M", "Jean Levi's",
                       "Sac Louis Vuitton", "Lunettes Ray-Ban", "Parfum Chanel"],
    "Maison":         ["Aspirateur Dyson", "Cafetière Nespresso", "Robot Thermomix",
                       "Lampe Ikea", "Matelas Emma", "Coussin déco"],
    "Alimentation":   ["Pack eau Evian", "Café Grand Cru", "Chocolat Valrhona",
                       "Huile d'olive BIO", "Vin Bordeaux AOC"],
    "Sport":          ["Vélo Decathlon", "Tapis yoga", "Gants boxe", "Raquette tennis",
                       "Chaussures trail", "Sac de sport Adidas"],
    "Livres":         ["Python Avancé", "Clean Code", "Dune", "Harry Potter T1",
                       "Le Petit Prince", "Design Patterns"],
}

PAYMENT_METHODS = ["carte_visa", "carte_mastercard", "paypal", "apple_pay",
                   "google_pay", "virement_bancaire", "chèque_cadeau"]

STATUSES  = ["completed", "completed", "completed", "pending", "failed", "refunded"]
# Poids : majorité de completed

CITIES = [
    ("Paris", "75"),  ("Lyon", "69"), ("Marseille", "13"), ("Bordeaux", "33"),
    ("Lille", "59"),  ("Nantes", "44"), ("Toulouse", "31"), ("Nice", "06"),
    ("Strasbourg", "67"), ("Montpellier", "34"),
]

# ─── GÉNÉRATEUR D'ÉVÉNEMENT ──────────────────────────────
def generate_transaction() -> dict:
    category  = random.choice(list(CATEGORIES.keys()))
    product   = random.choice(CATEGORIES[category])
    quantity  = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 12, 8, 5])[0]

    # Prix réaliste par catégorie
    price_range = {
        "Électronique": (29.99,  2499.99),
        "Mode":         (9.99,    499.99),
        "Maison":       (14.99,   899.99),
        "Alimentation": (2.99,     59.99),
        "Sport":        (9.99,    699.99),
        "Livres":       (7.99,     34.99),
    }
    unit_price   = round(random.uniform(*price_range[category]), 2)
    discount_pct = random.choices([0, 5, 10, 15, 20, 30], weights=[50, 15, 15, 10, 7, 3])[0]
    total_price  = round(unit_price * quantity * (1 - discount_pct / 100), 2)

    city, dept = random.choice(CITIES)

    return {
        "transaction_id":  str(uuid.uuid4()),
        "timestamp":       datetime.utcnow().isoformat() + "Z",
        "customer": {
            "id":          str(uuid.uuid4())[:8],
            "name":        fake.name(),
            "email":       fake.email(),
            "age_group":   random.choice(["18-25", "26-35", "36-45", "46-55", "55+"]),
            "city":        city,
            "department":  dept,
            "is_premium":  random.random() < 0.25,
        },
        "product": {
            "id":          f"PROD-{random.randint(1000, 9999)}",
            "name":        product,
            "category":    category,
            "unit_price":  unit_price,
            "quantity":    quantity,
            "discount_pct": discount_pct,
        },
        "payment": {
            "method":      random.choice(PAYMENT_METHODS),
            "total_amount": total_price,
            "currency":    "EUR",
            "status":      random.choice(STATUSES),
        },
        "session": {
            "device":      random.choice(["mobile", "desktop", "tablet"]),
            "browser":     random.choice(["Chrome", "Safari", "Firefox", "Edge"]),
            "source":      random.choice(["direct", "google", "facebook", "email", "instagram"]),
        },
    }

# ─── MAIN ────────────────────────────────────────────────
def main():
    import sys
    sys.stdout.reconfigure(encoding='utf-8')
    print(f"[ecommerce] Connexion a Kafka ({KAFKA_BROKER}) ...")
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                retries=3,
            )
            print(f"[ecommerce] Connecte -- envoi vers le topic '{TOPIC}'")
            break
        except NoBrokersAvailable:
            print("[ecommerce] Kafka non disponible, retry dans 5 s ...")
            time.sleep(5)

    count = 0
    try:
        while NUM_MESSAGES is None or count < NUM_MESSAGES:
            event = generate_transaction()
            producer.send(
                TOPIC,
                key=event["transaction_id"],
                value=event,
            )
            count += 1
            if count % 50 == 0:
                category = event["product"]["category"]
                amount   = event["payment"]["total_amount"]
                status   = event["payment"]["status"]
                print(f"[ecommerce] {count:>5} msgs | {category} | {amount:.2f} EUR | {status}")
            time.sleep(random.uniform(*SEND_INTERVAL))

    except KeyboardInterrupt:
        print(f"[ecommerce] Arret -- {count} transactions envoyees.")
    finally:
        producer.flush()
        producer.close()

if __name__ == "__main__":
    main()
