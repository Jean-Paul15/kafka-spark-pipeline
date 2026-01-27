# =============================================================================
# SPARK STREAMING - Point d'Entrée Principal
# Orchestration des 3 flux Kafka et écriture vers Redis
# =============================================================================

import os
import sys
from pyspark.sql.functions import current_timestamp, lit

# Import des modules de traitement
from streaming_core import (
    create_spark_session,
    read_kafka_stream,
    get_annonces_schema,
    get_prix_schema,
    get_transactions_schema,
    process_annonces_stream,
    process_prix_stream,
    process_transactions_stream,
)
from redis_storage import create_redis_storage


# Instance globale Redis
redis_storage = None


def init_redis():
    """Initialise la connexion Redis."""
    global redis_storage
    try:
        redis_storage = create_redis_storage()
        print("[REDIS] Stockage initialisé avec succès")
    except Exception as e:
        print(f"[REDIS] Erreur initialisation: {e}")
        redis_storage = None


def write_to_redis_and_console(df, batch_id, output_name):
    """Écrit le batch vers Redis et la console."""
    if df.count() > 0:
        print(f"\n{'='*60}")
        print(f"[BATCH {batch_id}] {output_name.upper()}")
        print(f"{'='*60}")
        df.show(truncate=False)
        
        # Stockage dans Redis
        if redis_storage:
            try:
                rows = df.collect()
                for row in rows:
                    data_dict = row.asDict()
                    # Convertir les types non-serialisables
                    for key, value in data_dict.items():
                        if hasattr(value, 'isoformat'):
                            data_dict[key] = value.isoformat()
                        elif value is None:
                            data_dict[key] = ""
                        else:
                            data_dict[key] = str(value)
                    
                    # Stocker selon le type
                    if output_name == "annonces_agg":
                        redis_storage.store_annonces_aggregation(data_dict)
                        if "ville" in data_dict:
                            redis_storage.store_city_stats(data_dict["ville"], data_dict)
                    elif output_name == "prix_agg":
                        redis_storage.store_prix_aggregation(data_dict)
                    elif output_name == "transactions_agg":
                        redis_storage.store_transactions_aggregation(data_dict)
                
                print(f"[REDIS] {len(rows)} enregistrements stockes")
            except Exception as e:
                print(f"[REDIS] Erreur stockage: {e}")


def main():
    """Point d'entree principal du job Spark Streaming."""
    print("\n" + "="*70)
    print("   SPARK STREAMING - ANALYTICS IMMOBILIER TEMPS REEL")
    print("   Traitement de 3 sources Kafka: annonces, prix, transactions")
    print("   Stockage des resultats dans Redis")
    print("="*70 + "\n")
    
    # Initialisation Redis
    init_redis()
    
    # Création de la session Spark
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    print("[SPARK] Session initialisée")
    print("[SPARK] Connexion aux topics Kafka...")
    
    # Lecture des 3 streams Kafka
    annonces_stream = read_kafka_stream(spark, "annonces", get_annonces_schema())
    prix_stream = read_kafka_stream(spark, "prix", get_prix_schema())
    transactions_stream = read_kafka_stream(spark, "transactions", get_transactions_schema())
    
    print("[SPARK] Streams Kafka connectés")
    
    # Traitement des streams
    annonces_agg = process_annonces_stream(annonces_stream)
    prix_agg = process_prix_stream(prix_stream)
    transactions_agg = process_transactions_stream(transactions_stream)
    
    print("[SPARK] Pipelines de traitement configurés")
    print("[SPARK] Démarrage des queries streaming...\n")
    
    # Query 1: Annonces agregees
    query_annonces = (
        annonces_agg.writeStream
        .outputMode("update")
        .foreachBatch(lambda df, bid: write_to_redis_and_console(df, bid, "annonces_agg"))
        .option("checkpointLocation", "/tmp/checkpoint/annonces")
        .trigger(processingTime="10 seconds")
        .start()
    )
    
    # Query 2: Prix agreges
    query_prix = (
        prix_agg.writeStream
        .outputMode("update")
        .foreachBatch(lambda df, bid: write_to_redis_and_console(df, bid, "prix_agg"))
        .option("checkpointLocation", "/tmp/checkpoint/prix")
        .trigger(processingTime="10 seconds")
        .start()
    )
    
    # Query 3: Transactions agregees
    query_transactions = (
        transactions_agg.writeStream
        .outputMode("update")
        .foreachBatch(lambda df, bid: write_to_redis_and_console(df, bid, "transactions_agg"))
        .option("checkpointLocation", "/tmp/checkpoint/transactions")
        .trigger(processingTime="10 seconds")
        .start()
    )
    
    print("[SPARK] Toutes les queries sont actives!")
    print("[SPARK] Traitement en cours... (Ctrl+C pour arreter)")
    
    # Attente de la fin des queries
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
