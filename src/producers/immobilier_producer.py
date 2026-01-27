# =============================================================================
# PRODUCTEUR KAFKA - Simulation Données Immobilières Temps Réel
# Génère des événements pour 3 topics: annonces, prix, transactions
# =============================================================================

import os
import json
import time
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

# Configuration depuis variables d'environnement
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = os.getenv("TOPIC", "annonces")
PRODUCER_TYPE = os.getenv("PRODUCER_TYPE", "annonces")

# Initialisation Faker pour données réalistes
fake = Faker("fr_FR")

# =============================================================================
# DONNÉES IMMOBILIÈRES 
# Prix en XOF (Franc CFA BCEAO) 
# =============================================================================

VILLES = [
    {"nom": "Lome", "region": "Maritime", "prix_m2": 475000},
    {"nom": "Kara", "region": "Kara", "prix_m2": 185000},
    {"nom": "Sokode", "region": "Centrale", "prix_m2": 165000},
    {"nom": "Kpalime", "region": "Plateaux", "prix_m2": 195000},
    {"nom": "Atakpame", "region": "Plateaux", "prix_m2": 155000},
    {"nom": "Dapaong", "region": "Savanes", "prix_m2": 125000},
    {"nom": "Tsevie", "region": "Maritime", "prix_m2": 210000},
    {"nom": "Notse", "region": "Plateaux", "prix_m2": 140000},
    {"nom": "Anie", "region": "Plateaux", "prix_m2": 130000},
    {"nom": "Tabligbo", "region": "Maritime", "prix_m2": 175000},
]

# Quartiers populaires de 
QUARTIERS_LOME = [
    "Tokoin", "Adidogome", "Nyekonakpoe", "Be", "Agoe", 
    "Kodjoviakope", "Amoutive", "Hedzranawoe", "Baguida", "Aflao"
]

TYPES_BIENS = ["Appartement", "Maison", "Villa", "Studio", "Duplex", "Terrain"]
STATUTS = ["Disponible", "Sous compromis", "Vendu", "Reserve"]
SOURCES = ["Agence Immobiliere Lome", "CoinAfrique", "Afribaba", "Particulier", "Promoteur Immo"]


def create_producer():
    """Crée une instance du producteur Kafka avec retry."""
    max_retries = 30
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
            )
            print(f"[OK] Connexion Kafka établie: {KAFKA_BROKER}")
            return producer
        except Exception as e:
            print(f"[ATTENTE] Kafka non disponible (tentative {attempt + 1}/{max_retries})")
            time.sleep(2)
    raise Exception("Impossible de se connecter a Kafka")


def generate_annonce():
    """Génère une annonce immobilière togolaise aléatoire."""
    ville = random.choice(VILLES)
    surface = random.randint(25, 350)
    prix_base = ville["prix_m2"] * surface
    # Variation de prix selon le marche local
    prix = int(prix_base * random.uniform(0.80, 1.30))
    
    # Quartier specifique pour Lome
    quartier = random.choice(QUARTIERS_LOME) if ville["nom"] == "Lome" else ville["nom"]
    
    return {
        "id_annonce": fake.uuid4()[:8],
        "timestamp": datetime.now().isoformat(),
        "source": random.choice(SOURCES),
        "type_bien": random.choice(TYPES_BIENS),
        "ville": ville["nom"],
        "region": ville["region"],
        "quartier": quartier,
        "surface_m2": surface,
        "nb_pieces": max(1, surface // 20),
        "prix_xof": prix,
        "prix_m2_xof": int(prix / surface),
        "statut": "Disponible",
        "commodites": random.sample(["Eau courante", "Electricite", "Cloture", "Gardien", "Groupe electrogene", "Forage"], k=random.randint(2, 5)),
    }


def generate_prix():
    """Génère une mise à jour de prix du marché togolais."""
    ville = random.choice(VILLES)
    # Variation typique du marché immobilier
    variation = random.uniform(-0.03, 0.06)
    
    return {
        "id_update": fake.uuid4()[:8],
        "timestamp": datetime.now().isoformat(),
        "ville": ville["nom"],
        "region": ville["region"],
        "ancien_prix_m2_xof": ville["prix_m2"],
        "nouveau_prix_m2_xof": int(ville["prix_m2"] * (1 + variation)),
        "variation_pct": round(variation * 100, 2),
        "tendance": "hausse" if variation > 0 else "baisse",
        "source_indice": random.choice(["INSEED Togo", "Chambre Notaires", "OTR", "Agences Lome"]),
    }


def generate_transaction():
    """Génère une transaction immobilière togolaise."""
    ville = random.choice(VILLES)
    surface = random.randint(30, 300)
    prix = int(ville["prix_m2"] * surface * random.uniform(0.85, 1.20))
    
    return {
        "id_transaction": fake.uuid4()[:8],
        "timestamp": datetime.now().isoformat(),
        "type_bien": random.choice(TYPES_BIENS),
        "ville": ville["nom"],
        "region": ville["region"],
        "surface_m2": surface,
        "prix_vente_xof": prix,
        "prix_m2_final_xof": int(prix / surface),
        "duree_vente_jours": random.randint(20, 150),
        "type_acheteur": random.choice(["Primo-accedant", "Investisseur", "Diaspora", "Entreprise", "Fonctionnaire"]),
        "financement": random.choice(["Credit CNSS", "Comptant", "Mixte", "Virement Diaspora"]),
    }


def run_producer():
    """Boucle principale du producteur."""
    producer = create_producer()
    generators = {
        "annonces": generate_annonce,
        "prix": generate_prix,
        "transactions": generate_transaction,
    }
    
    generator = generators.get(PRODUCER_TYPE, generate_annonce)
    interval_map = {"annonces": 1.5, "prix": 3.0, "transactions": 2.0}
    interval = interval_map.get(PRODUCER_TYPE, 2.0)
    
    print(f"[DEMARRAGE] Producteur {PRODUCER_TYPE} -> Topic: {TOPIC}")
    print(f"[CONFIG] Intervalle: {interval}s")
    
    count = 0
    while True:
        try:
            data = generator()
            key = data.get("ville", "unknown")
            producer.send(TOPIC, key=key, value=data)
            producer.flush()
            count += 1
            print(f"[{PRODUCER_TYPE.upper()}] #{count} | {key} | {json.dumps(data, ensure_ascii=False)[:100]}...")
            time.sleep(interval + random.uniform(-0.5, 0.5))
        except Exception as e:
            print(f"[ERREUR] {e}")
            time.sleep(5)


if __name__ == "__main__":
    run_producer()
