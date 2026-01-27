#!/bin/bash
# =============================================================
#  init-hdfs.sh – Initialisation des répertoires HDFS
#  Exécuté au démarrage du NameNode
# =============================================================

set -e

echo "⏳  Attente que le NameNode soit prêt…"
until hdfs dfs -ls / > /dev/null 2>&1; do
    sleep 3
done

echo "✅  NameNode prêt — création des répertoires HDFS…"

# ── Données brutes (écriture par Spark Streaming) ─────────
hdfs dfs -mkdir -p /data/raw/ecommerce
hdfs dfs -mkdir -p /data/raw/taxi
hdfs dfs -mkdir -p /data/raw/weather

# ── Données traitées (résultats Spark Batch) ──────────────
hdfs dfs -mkdir -p /data/processed/ecommerce
hdfs dfs -mkdir -p /data/processed/taxi
hdfs dfs -mkdir -p /data/processed/weather

# ── Archives (données historiques) ────────────────────────
hdfs dfs -mkdir -p /data/archive

# ── Checkpoints Spark Streaming ───────────────────────────
hdfs dfs -mkdir -p /checkpoints

# ── Permissions ouvertes (environnement pédagogique) ──────
hdfs dfs -chmod -R 777 /data
hdfs dfs -chmod -R 777 /checkpoints

echo "📂  Structure HDFS créée :"
hdfs dfs -ls -R /data

echo "✅  Initialisation HDFS terminée."
