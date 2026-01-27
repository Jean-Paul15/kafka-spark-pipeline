#!/bin/bash
# =============================================================================
# ENTRYPOINT SPARK - Gestion des modes master / worker / immobilier / postgres / idle
# Image apache/spark officielle
# =============================================================================

set -e

SPARK_MODE=${SPARK_MODE:-master}
SPARK_HOME=${SPARK_HOME:-/opt/spark}

echo "[SPARK] =============================================="
echo "[SPARK] Demarrage en mode: $SPARK_MODE"
echo "[SPARK] SPARK_HOME: $SPARK_HOME"
echo "[SPARK] =============================================="

case "$SPARK_MODE" in
    master)
        echo "[SPARK] Lancement du Master sur port 7077..."
        # Demarrage du master en foreground
        exec $SPARK_HOME/bin/spark-class org.apache.spark.deploy.master.Master \
            --host 0.0.0.0 \
            --port 7077 \
            --webui-port 8080
        ;;
    worker)
        echo "[SPARK] Attente du Master..."
        sleep 15
        echo "[SPARK] Lancement du Worker..."
        echo "[SPARK] Connexion au Master: $SPARK_MASTER_URL"
        # Demarrage du worker en foreground
        exec $SPARK_HOME/bin/spark-class org.apache.spark.deploy.worker.Worker \
            $SPARK_MASTER_URL \
            --webui-port 8081
        ;;
    immobilier)
        echo "[SPARK] Attente du cluster Spark (45s)..."
        sleep 45
        echo "[SPARK] Lancement du job Streaming Immobilier -> Redis..."
        ;;
    postgres)
        echo "[SPARK] Attente du cluster Spark (45s)..."
        sleep 45
        echo "[SPARK] Lancement du job Streaming Kafka -> PostgreSQL..."
        exec $SPARK_HOME/bin/spark-submit             --master spark://spark-master:7077             --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.postgresql:postgresql:42.6.0             --conf spark.driver.extraJavaOptions="-Divy.cache.dir=/tmp -Divy.home=/tmp"             --conf spark.driver.host=spark-postgres             /opt/spark-apps/kafka_to_postgres.py
        ;;
    idle)
        # Conteneur client pour lancer manuellement les jobs batch (docker exec)
        exec sleep infinity
        ;;
    *)
        echo "[ERREUR] Mode inconnu: $SPARK_MODE"
        exit 1
        ;;
esac
