"""
=============================================================
 SPARK STREAMING CONSUMER – Kafka → PostgreSQL (Grafana)
=============================================================
 Lit les 3 topics en parallèle et alimente PostgreSQL
 en temps réel pour visualisation Grafana.

 Topics consommés :
   - ecommerce_transactions  → table pg: ecommerce_events
   - nyc_taxi_trips          → table pg: taxi_events
   - weather_realtime        → table pg: weather_events
=============================================================
 SPARK STREAMING – Kafka → PostgreSQL
 CORRECTIONS :
  1. Package kafka 3.5.4 (doit matcher la version Spark)
  2. KAFKA_BROKER : port 29092 (interne Docker)
  3. startingOffsets : "earliest" (lit tout l historique Kafka)
  4. Checkpoint : /tmp/local (pas besoin de HDFS)
  5. trigger 10s + print par batch pour debug

 COMMANDE SPARK-SUBMIT CORRIGEE :
   docker exec spark-master spark-submit \
     --master spark://spark-master:7077 \
     --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4,org.postgresql:postgresql:42.6.0 \
     /opt/bitnami/spark/apps/kafka_to_postgres.py
=============================================================
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, get_json_object, current_timestamp,
    when, round as spark_round,
)

KAFKA_BROKER    = os.getenv("KAFKA_BOOTSTRAP_INTERNAL", "kafka:29092")
PG_HOST         = os.getenv("POSTGRES_HOST", "postgres")
PG_DB           = os.getenv("POSTGRES_DB", "bigdata")
PG_URL          = f"jdbc:postgresql://{PG_HOST}:5432/{PG_DB}"
PG_USER         = os.getenv("POSTGRES_USER", "bigdata")
PG_PASSWORD     = os.environ["POSTGRES_PASSWORD"]
PG_DRIVER       = "org.postgresql.Driver"
CHECKPOINT_BASE = os.getenv("CHECKPOINT_BASE", "/tmp/spark_checkpoints")

spark = (SparkSession.builder
         .appName("KafkaToPostgres")
         .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
         .getOrCreate())
spark.sparkContext.setLogLevel("WARN")
print("SparkSession OK")

def read_topic(topic):
    return (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKER)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", 2000)
            .load()
            .select(col("value").cast("string").alias("json_str"),
                    col("timestamp").alias("kafka_ts")))

def write_to_postgres(df, table):
    def write_batch(batch_df, batch_id):
        n = batch_df.count()
        if n > 0:
            print(f"  Batch {batch_id} -> {table} : {n} lignes")
            (batch_df.write
             .format("jdbc")
             .option("url", PG_URL)
             .option("dbtable", table)
             .option("user", PG_USER)
             .option("password", PG_PASSWORD)
             .option("driver", PG_DRIVER)
             .mode("append")
             .save())
    return (df.writeStream
            .foreachBatch(write_batch)
            .option("checkpointLocation", f"{CHECKPOINT_BASE}/{table}")
            .trigger(processingTime="10 seconds"))

# ─── ECOMMERCE ────────────────────────────────────────────
ecom_df = read_topic("ecommerce_transactions").select(
    get_json_object("json_str", "$.transaction_id").alias("transaction_id"),
    get_json_object("json_str", "$.timestamp").cast("timestamp").alias("event_time"),
    get_json_object("json_str", "$.customer.city").alias("city"),
    get_json_object("json_str", "$.customer.age_group").alias("age_group"),
    get_json_object("json_str", "$.customer.is_premium").cast("boolean").alias("is_premium"),
    get_json_object("json_str", "$.product.category").alias("category"),
    get_json_object("json_str", "$.product.name").alias("product_name"),
    get_json_object("json_str", "$.product.unit_price").cast("double").alias("unit_price"),
    get_json_object("json_str", "$.product.quantity").cast("int").alias("quantity"),
    get_json_object("json_str", "$.product.discount_pct").cast("int").alias("discount_pct"),
    get_json_object("json_str", "$.payment.total_amount").cast("double").alias("total_amount"),
    get_json_object("json_str", "$.payment.method").alias("payment_method"),
    get_json_object("json_str", "$.payment.status").alias("status"),
    get_json_object("json_str", "$.session.device").alias("device"),
    get_json_object("json_str", "$.session.source").alias("acquisition_source"),
    current_timestamp().alias("ingested_at"),
).withColumn(
    # Transformation 1 : classification par tranche de CA
    "revenue_tier",
    when(col("total_amount") >= 500, "Premium")
    .when(col("total_amount") >= 100, "Standard")
    .when(col("total_amount") >= 20,  "Low")
    .otherwise("Micro")
)

# ─── TAXI ─────────────────────────────────────────────────
taxi_df = read_topic("nyc_taxi_trips").select(
    get_json_object("json_str", "$.trip_id").alias("trip_id"),
    get_json_object("json_str", "$.pickup.datetime").cast("timestamp").alias("pickup_time"),
    get_json_object("json_str", "$.dropoff.datetime").cast("timestamp").alias("dropoff_time"),
    get_json_object("json_str", "$.pickup.zone_name").alias("pickup_zone"),
    get_json_object("json_str", "$.dropoff.zone_name").alias("dropoff_zone"),
    get_json_object("json_str", "$.pickup.latitude").cast("double").alias("pickup_lat"),
    get_json_object("json_str", "$.pickup.longitude").cast("double").alias("pickup_lon"),
    get_json_object("json_str", "$.dropoff.latitude").cast("double").alias("dropoff_lat"),
    get_json_object("json_str", "$.dropoff.longitude").cast("double").alias("dropoff_lon"),
    get_json_object("json_str", "$.trip.passenger_count").cast("int").alias("passenger_count"),
    get_json_object("json_str", "$.trip.distance_miles").cast("double").alias("distance_miles"),
    get_json_object("json_str", "$.trip.duration_minutes").cast("int").alias("duration_minutes"),
    get_json_object("json_str", "$.fare.base_fare").cast("double").alias("base_fare"),
    get_json_object("json_str", "$.fare.tip_amount").cast("double").alias("tip_amount"),
    get_json_object("json_str", "$.fare.total_amount").cast("double").alias("total_amount"),
    get_json_object("json_str", "$.fare.payment_type").alias("payment_type"),
    current_timestamp().alias("ingested_at"),
).withColumn(
    # Transformation 2 : efficacite tarifaire USD/mile
    "price_efficiency",
    spark_round(
        col("total_amount") / col("distance_miles"),
        2
    )
).withColumn(
    "trip_category",
    when(col("distance_miles") < 1,  "Court (<1mi)")
    .when(col("distance_miles") < 5,  "Moyen (1-5mi)")
    .when(col("distance_miles") < 15, "Long (5-15mi)")
    .otherwise("Tres long (>15mi)")
)

# ─── METEO ────────────────────────────────────────────────
weather_df = read_topic("weather_realtime").select(
    get_json_object("json_str", "$.event_id").alias("event_id"),
    get_json_object("json_str", "$.fetched_at").cast("timestamp").alias("fetched_at"),
    get_json_object("json_str", "$.location.city").alias("city"),
    get_json_object("json_str", "$.location.country").alias("country"),
    get_json_object("json_str", "$.location.latitude").cast("double").alias("latitude"),
    get_json_object("json_str", "$.location.longitude").cast("double").alias("longitude"),
    get_json_object("json_str", "$.current.temperature_c").cast("double").alias("temperature_c"),
    get_json_object("json_str", "$.current.feels_like_c").cast("double").alias("feels_like_c"),
    get_json_object("json_str", "$.current.humidity_pct").cast("int").alias("humidity_pct"),
    get_json_object("json_str", "$.current.pressure_hpa").cast("double").alias("pressure_hpa"),
    get_json_object("json_str", "$.current.cloud_cover_pct").cast("int").alias("cloud_cover_pct"),
    get_json_object("json_str", "$.current.visibility_m").cast("double").alias("visibility_m"),
    get_json_object("json_str", "$.current.uv_index").cast("double").alias("uv_index"),
    get_json_object("json_str", "$.current.weather_description").alias("weather_description"),
    get_json_object("json_str", "$.precipitation.total_mm").cast("double").alias("precip_mm"),
    get_json_object("json_str", "$.wind.speed_kmh").cast("double").alias("wind_speed_kmh"),
    get_json_object("json_str", "$.wind.direction_label").alias("wind_direction"),
    get_json_object("json_str", "$.wind.gusts_kmh").cast("double").alias("wind_gusts_kmh"),
    get_json_object("json_str", "$.alert_count").cast("int").alias("alert_count"),
    get_json_object("json_str", "$.alerts").alias("alerts_json"),
    current_timestamp().alias("ingested_at"),
)

# ─── START ────────────────────────────────────────────────
print("Demarrage des 3 streams...")
q1 = write_to_postgres(ecom_df,    "ecommerce_events").start()
q2 = write_to_postgres(taxi_df,    "taxi_events").start()
q3 = write_to_postgres(weather_df, "weather_events").start()
print("3 streams actifs - en attente de donnees...")
spark.streams.awaitAnyTermination()