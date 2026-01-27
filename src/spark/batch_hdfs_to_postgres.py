"""
=============================================================
 SPARK BATCH ETL – HDFS → PostgreSQL
=============================================================
 Lit les données brutes Parquet depuis HDFS,
 effectue les transformations et calcule les KPIs,
 puis charge les résultats dans PostgreSQL.

 Lancement :
   spark-submit \
     --master spark://spark-master:7077 \
     --packages org.postgresql:postgresql:42.6.0 \
     batch_hdfs_to_postgres.py --source ecommerce
     batch_hdfs_to_postgres.py --source taxi
     batch_hdfs_to_postgres.py --source weather
     batch_hdfs_to_postgres.py --source all
=============================================================
"""

import argparse
import os
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window

# ─── CONFIG ──────────────────────────────────────────────
HDFS_BASE = "hdfs://namenode:9000/data/raw"
PG_URL    = "jdbc:postgresql://{}:5432/{}".format(
    os.getenv("POSTGRES_HOST", "postgres"), os.getenv("POSTGRES_DB", "bigdata"))
PG_PROPS  = {
    "user":     os.getenv("POSTGRES_USER", "bigdata"),
    "password": os.environ["POSTGRES_PASSWORD"],
    "driver":   "org.postgresql.Driver",
}

def get_spark() -> SparkSession:
    return (SparkSession.builder
            .appName("BatchETL_HDFS_to_Postgres")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .getOrCreate())

def write_pg(df: DataFrame, table: str, mode: str = "overwrite"):
    """Écrit un DataFrame dans PostgreSQL."""
    print(f"  📤  Écriture → {table} ({df.count()} lignes) …")
    (df.write
       .format("jdbc")
       .option("url", PG_URL)
       .option("dbtable", table)
       .option("user",     PG_PROPS["user"])
       .option("password", PG_PROPS["password"])
       .option("driver",   PG_PROPS["driver"])
       .mode(mode)
       .save())
    print(f"  ✅  {table} chargée.")

# ═══════════════════════════════════════════════════════════
#  ETL E-COMMERCE
# ═══════════════════════════════════════════════════════════
def etl_ecommerce(spark: SparkSession):
    print("\n🛒  ETL E-Commerce …")

    # ── Lecture ──────────────────────────────────────────
    df = spark.read.parquet(f"{HDFS_BASE}/ecommerce/")
    print(f"  📂  {df.count():,} événements lus depuis HDFS")
    df.printSchema()

    # ── Nettoyage ─────────────────────────────────────────
    df = (df
        .dropna(subset=["transaction_id", "total_amount", "status"])
        .filter(F.col("total_amount") > 0)
        .filter(F.col("status").isin("completed", "pending", "failed", "refunded"))
        .withColumn("event_date",
            F.to_date(F.col("event_time")))
        .withColumn("event_hour",
            F.hour(F.col("event_time")))
        .withColumn("revenue_net",
            F.when(F.col("status") == "completed", F.col("total_amount")).otherwise(0.0))
    )

    # ── KPI 1 : CA quotidien par catégorie ────────────────
    kpi_daily_cat = (df
        .groupBy("event_date", "category")
        .agg(
            F.sum("revenue_net").alias("revenue"),
            F.count("*").alias("transactions"),
            F.countDistinct("transaction_id").alias("unique_transactions"),
            F.avg("total_amount").alias("avg_basket"),
            F.sum("quantity").alias("items_sold"),
            F.sum(F.when(F.col("status") == "completed", 1).otherwise(0)).alias("completed"),
            F.sum(F.when(F.col("status") == "failed",    1).otherwise(0)).alias("failed"),
        )
        .withColumn("conversion_rate",
            F.round(F.col("completed") / F.col("transactions"), 4))
        .withColumn("computed_at", F.current_timestamp())
        .orderBy("event_date", "category")
    )
    write_pg(kpi_daily_cat, "kpi_ecom_daily_category")

    # ── KPI 2 : Panier moyen par ville + device ───────────
    kpi_city_device = (df
        .filter(F.col("status") == "completed")
        .groupBy("city", "device")
        .agg(
            F.avg("total_amount").alias("avg_basket"),
            F.sum("revenue_net").alias("total_revenue"),
            F.count("*").alias("nb_transactions"),
            F.avg("discount_pct").alias("avg_discount"),
            F.sum(F.when(F.col("is_premium"), 1).otherwise(0)).alias("premium_customers"),
        )
        .withColumn("premium_rate",
            F.round(F.col("premium_customers") / F.col("nb_transactions"), 4))
        .withColumn("computed_at", F.current_timestamp())
        .orderBy(F.desc("total_revenue"))
    )
    write_pg(kpi_city_device, "kpi_ecom_city_device")

    # ── KPI 3 : Heatmap commandes par heure ───────────────
    kpi_hourly = (df
        .groupBy("event_date", "event_hour")
        .agg(
            F.count("*").alias("nb_orders"),
            F.sum("revenue_net").alias("revenue"),
        )
        .withColumn("computed_at", F.current_timestamp())
        .orderBy("event_date", "event_hour")
    )
    write_pg(kpi_hourly, "kpi_ecom_hourly")

    # ── KPI 4 : Top 20 produits ───────────────────────────
    kpi_top_products = (df
        .filter(F.col("status") == "completed")
        .groupBy("product_name", "category")
        .agg(
            F.sum("revenue_net").alias("total_revenue"),
            F.sum("quantity").alias("units_sold"),
            F.count("*").alias("nb_orders"),
            F.avg("total_amount").alias("avg_price"),
        )
        .withColumn("rank",
            F.rank().over(Window.orderBy(F.desc("total_revenue"))))
        .filter(F.col("rank") <= 20)
        .withColumn("computed_at", F.current_timestamp())
    )
    write_pg(kpi_top_products, "kpi_ecom_top_products")

    print("  ✅  ETL E-Commerce terminé — 4 tables KPI chargées")


# ═══════════════════════════════════════════════════════════
#  ETL NYC TAXI
# ═══════════════════════════════════════════════════════════
def etl_taxi(spark: SparkSession):
    print("\n🚕  ETL NYC Taxi …")

    df = spark.read.parquet(f"{HDFS_BASE}/taxi/")
    print(f"  📂  {df.count():,} trips lus depuis HDFS")

    # ── Nettoyage ─────────────────────────────────────────
    df = (df
        .dropna(subset=["trip_id", "total_amount"])
        .filter(F.col("total_amount") > 0)
        .filter(F.col("distance_miles") > 0)
        .filter(F.col("distance_miles") < 100)  # exclure outliers
        .withColumn("pickup_date",  F.to_date(F.col("pickup_time")))
        .withColumn("pickup_hour",  F.hour(F.col("pickup_time")))
        .withColumn("price_per_mile",
            F.round(F.col("total_amount") / F.col("distance_miles"), 2))
        .withColumn("tip_rate",
            F.round(F.col("tip_amount") / F.col("base_fare"), 4))
    )

    # ── KPI 1 : Stats par zone de prise en charge ─────────
    kpi_zones = (df
        .groupBy("pickup_zone")
        .agg(
            F.count("*").alias("nb_trips"),
            F.avg("total_amount").alias("avg_fare"),
            F.avg("distance_miles").alias("avg_distance"),
            F.avg("tip_rate").alias("avg_tip_rate"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("duration_minutes").alias("avg_duration_min"),
        )
        .withColumn("rank",
            F.rank().over(Window.orderBy(F.desc("nb_trips"))))
        .withColumn("computed_at", F.current_timestamp())
        .orderBy(F.desc("nb_trips"))
    )
    write_pg(kpi_zones, "kpi_taxi_zones")

    # ── KPI 2 : Demande par heure ─────────────────────────
    kpi_hourly = (df
        .groupBy("pickup_date", "pickup_hour")
        .agg(
            F.count("*").alias("nb_trips"),
            F.avg("total_amount").alias("avg_fare"),
            F.sum("total_amount").alias("revenue"),
        )
        .withColumn("computed_at", F.current_timestamp())
        .orderBy("pickup_date", "pickup_hour")
    )
    write_pg(kpi_hourly, "kpi_taxi_hourly")

    # ── KPI 3 : Top paires origine → destination ──────────
    kpi_od = (df
        .groupBy("pickup_zone", "dropoff_zone")
        .agg(
            F.count("*").alias("nb_trips"),
            F.avg("total_amount").alias("avg_fare"),
            F.avg("distance_miles").alias("avg_distance"),
        )
        .filter(F.col("nb_trips") >= 5)
        .withColumn("computed_at", F.current_timestamp())
        .orderBy(F.desc("nb_trips"))
        .limit(50)
    )
    write_pg(kpi_od, "kpi_taxi_od_pairs")

    print("  ✅  ETL NYC Taxi terminé — 3 tables KPI chargées")


# ═══════════════════════════════════════════════════════════
#  ETL MÉTÉO
# ═══════════════════════════════════════════════════════════
def etl_weather(spark: SparkSession):
    print("\n🌤️  ETL Météo …")

    df = spark.read.parquet(f"{HDFS_BASE}/weather/")
    print(f"  📂  {df.count():,} observations lues depuis HDFS")

    df = (df
        .dropna(subset=["city", "temperature_c"])
        .withColumn("obs_date", F.to_date(F.col("fetched_at")))
        .withColumn("obs_hour", F.hour(F.col("fetched_at")))
    )

    # ── KPI 1 : Stats journalières par ville ──────────────
    kpi_daily = (df
        .groupBy("city", "country", "obs_date")
        .agg(
            F.avg("temperature_c").alias("avg_temp"),
            F.min("temperature_c").alias("min_temp"),
            F.max("temperature_c").alias("max_temp"),
            F.avg("humidity_pct").alias("avg_humidity"),
            F.avg("wind_speed_kmh").alias("avg_wind"),
            F.max("wind_gusts_kmh").alias("max_gusts"),
            F.sum("precip_mm").alias("total_precip"),
            F.max("alert_count").alias("max_alerts"),
            F.count("*").alias("nb_observations"),
        )
        .withColumn("computed_at", F.current_timestamp())
        .orderBy("obs_date", "country", "city")
    )
    write_pg(kpi_daily, "kpi_weather_daily")

    # ── KPI 2 : Comparaison inter-villes (dernière obs) ───
    w_latest = Window.partitionBy("city").orderBy(F.desc("fetched_at"))
    kpi_latest = (df
        .withColumn("rn", F.row_number().over(w_latest))
        .filter(F.col("rn") == 1)
        .select("city", "country", "latitude", "longitude",
                "temperature_c", "feels_like_c", "humidity_pct",
                "wind_speed_kmh", "wind_direction", "weather_description",
                "precip_mm", "alert_count", "fetched_at")
        .withColumn("computed_at", F.current_timestamp())
    )
    write_pg(kpi_latest, "kpi_weather_latest", mode="overwrite")

    print("  ✅  ETL Météo terminé — 2 tables KPI chargées")


# ─── MAIN ────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser(description="Spark Batch ETL")
    parser.add_argument("--source",
        choices=["ecommerce", "taxi", "weather", "all"],
        default="all")
    args = parser.parse_args()

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    print(f"\n{'═'*60}")
    print(f"  🚀  Spark Batch ETL — {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"  Source : {args.source}")
    print(f"{'═'*60}")

    if args.source in ("ecommerce", "all"): etl_ecommerce(spark)
    if args.source in ("taxi",      "all"): etl_taxi(spark)
    if args.source in ("weather",   "all"): etl_weather(spark)

    print(f"\n✅  Batch ETL terminé — {datetime.now().strftime('%H:%M:%S')}")
    spark.stop()

if __name__ == "__main__":
    main()
