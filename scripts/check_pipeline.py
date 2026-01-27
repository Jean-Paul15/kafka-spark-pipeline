#!/usr/bin/env python3
"""
=============================================================
 check_pipeline.py – Diagnostic de la stack Big Data
=============================================================
 Lance ce script pour vérifier que tous les services
 sont bien démarrés et accessibles.

 Utilisation :
   python check_pipeline.py
=============================================================
"""

import json
import os
import socket
import sys
import time

# ─── COULEURS TERMINAL ───────────────────────────────────
OK    = "\033[92m✅\033[0m"
FAIL  = "\033[91m❌\033[0m"
WARN  = "\033[93m⚠️ \033[0m"
INFO  = "\033[94mℹ️ \033[0m"

def check_port(host: str, port: int, label: str, timeout: float = 2.0) -> bool:
    try:
        s = socket.create_connection((host, port), timeout=timeout)
        s.close()
        print(f"  {OK}  {label:<40} {host}:{port}")
        return True
    except (ConnectionRefusedError, OSError, socket.timeout):
        print(f"  {FAIL}  {label:<40} {host}:{port}  — NON ACCESSIBLE")
        return False

def check_http(url: str, label: str) -> bool:
    try:
        import urllib.request
        req = urllib.request.urlopen(url, timeout=3)
        code = req.getcode()
        ok = code < 400
        icon = OK if ok else FAIL
        print(f"  {icon}  {label:<40} {url}  [{code}]")
        return ok
    except Exception as e:
        print(f"  {FAIL}  {label:<40} {url}  — {e}")
        return False

def check_kafka() -> bool:
    try:
        from kafka import KafkaAdminClient
        admin = KafkaAdminClient(bootstrap_servers="localhost:9092", request_timeout_ms=3000)
        topics = admin.list_topics()
        print(f"  {OK}  Kafka broker                              Topics : {len(topics)}")
        if topics:
            for t in sorted(topics)[:6]:
                print(f"       {INFO}  topic: {t}")
        admin.close()
        return True
    except ImportError:
        print(f"  {WARN}  kafka-python non installé — pip install kafka-python")
        return False
    except Exception as e:
        print(f"  {FAIL}  Kafka broker                              {e}")
        return False

def check_postgres() -> bool:
    try:
        import psycopg2
        conn = psycopg2.connect(
            host="localhost", port=5432,
            dbname=os.getenv("POSTGRES_DB", "bigdata"),
            user=os.getenv("POSTGRES_USER", "bigdata"),
            password=os.environ["POSTGRES_PASSWORD"],
            connect_timeout=3
        )
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema='public'")
        n_tables = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM ecommerce_events")
        n_rows = cur.fetchone()[0]
        print(f"  {OK}  PostgreSQL                                Tables: {n_tables} · ecommerce_events: {n_rows:,} lignes")
        conn.close()
        return True
    except ImportError:
        print(f"  {WARN}  psycopg2 non installé — pip install psycopg2-binary")
        return False
    except Exception as e:
        print(f"  {FAIL}  PostgreSQL                                {e}")
        return False

def check_openmeteo() -> bool:
    try:
        import urllib.request
        url = "https://api.open-meteo.com/v1/forecast?latitude=48.85&longitude=2.35&current=temperature_2m"
        data = json.loads(urllib.request.urlopen(url, timeout=5).read())
        temp = data["current"]["temperature_2m"]
        print(f"  {OK}  API Open-Meteo (météo réelle)              Paris: {temp}°C en ce moment")
        return True
    except Exception as e:
        print(f"  {FAIL}  API Open-Meteo                             {e}")
        return False

# ─── MAIN ────────────────────────────────────────────────
def main():
    print(f"\n{'═'*65}")
    print("  🔍  Diagnostic Pipeline Big Data")
    print(f"{'═'*65}\n")

    results = []

    print("📦  Services Docker (ports)")
    print("─"*65)
    results += [
        check_port("localhost", 9092, "Apache Kafka"),
        check_port("localhost", 2181, "Zookeeper"),
        check_port("localhost", 9870, "HDFS NameNode"),
        check_port("localhost", 9864, "HDFS DataNode"),
        check_port("localhost", 5432, "PostgreSQL"),
        check_port("localhost", 3000, "Grafana"),
        check_port("localhost", 8080, "Kafka UI"),
        check_port("localhost", 8090, "Spark Master"),
    ]

    print("\n🌐  Interfaces Web")
    print("─"*65)
    results += [
        check_http("http://localhost:9870/", "HDFS WebUI"),
        check_http("http://localhost:3000/api/health", "Grafana Health"),
        check_http("http://localhost:8080/", "Kafka UI"),
    ]

    print("\n🔌  Connexions applicatives")
    print("─"*65)
    results += [
        check_kafka(),
        check_postgres(),
        check_openmeteo(),
    ]

    # ── Résumé ────────────────────────────────────────────
    n_ok   = sum(1 for r in results if r)
    n_fail = len(results) - n_ok
    pct    = int(100 * n_ok / len(results))

    print(f"\n{'═'*65}")
    print(f"  Résultat : {n_ok}/{len(results)} checks OK ({pct}%)")
    if n_fail == 0:
        print(f"  {OK}  Stack 100% opérationnelle — vous pouvez commencer !")
    elif n_fail <= 3:
        print(f"  {WARN}  Quelques services non démarrés. Vérifiez : docker compose ps")
    else:
        print(f"  {FAIL}  Stack non opérationnelle. Relancez : docker compose up -d")
    print(f"{'═'*65}\n")

    return 0 if n_fail == 0 else 1

if __name__ == "__main__":
    sys.exit(main())
