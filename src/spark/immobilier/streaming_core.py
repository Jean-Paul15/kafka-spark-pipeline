# =============================================================================
# SPARK STREAMING - Traitement Temps Réel Données Immobilières
# Agrégation et analyse des flux Kafka multi-sources
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, window, avg, count, sum as spark_sum,
    current_timestamp, expr, lit, when, max as spark_max, min as spark_min
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    FloatType, TimestampType
)


def create_spark_session():
    """Initialise la session Spark avec configuration Kafka."""
    return (
        SparkSession.builder
        .appName("ImmobilierStreamingAnalytics")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.streaming.backpressure.enabled", "true")
        .getOrCreate()
    )


def get_annonces_schema():
    """Schéma JSON pour les annonces immobilières togolaises."""
    return StructType([
        StructField("id_annonce", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("source", StringType(), True),
        StructField("type_bien", StringType(), True),
        StructField("ville", StringType(), True),
        StructField("region", StringType(), True),
        StructField("quartier", StringType(), True),
        StructField("surface_m2", IntegerType(), True),
        StructField("nb_pieces", IntegerType(), True),
        StructField("prix_xof", IntegerType(), True),
        StructField("prix_m2_xof", IntegerType(), True),
        StructField("statut", StringType(), True),
        StructField("commodites", StringType(), True),
    ])


def get_prix_schema():
    """Schéma JSON pour les mises à jour de prix togolais (XOF)."""
    return StructType([
        StructField("id_update", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("ville", StringType(), True),
        StructField("region", StringType(), True),
        StructField("ancien_prix_m2_xof", IntegerType(), True),
        StructField("nouveau_prix_m2_xof", IntegerType(), True),
        StructField("variation_pct", FloatType(), True),
        StructField("tendance", StringType(), True),
        StructField("source_indice", StringType(), True),
    ])


def get_transactions_schema():
    """Schéma JSON pour les transactions togolaises (XOF)."""
    return StructType([
        StructField("id_transaction", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("type_bien", StringType(), True),
        StructField("ville", StringType(), True),
        StructField("region", StringType(), True),
        StructField("surface_m2", IntegerType(), True),
        StructField("prix_vente_xof", IntegerType(), True),
        StructField("prix_m2_final_xof", IntegerType(), True),
        StructField("duree_vente_jours", IntegerType(), True),
        StructField("type_acheteur", StringType(), True),
        StructField("financement", StringType(), True),
    ])


def read_kafka_stream(spark, topic, schema):
    """Lit un stream Kafka et parse le JSON."""
    kafka_broker = "kafka:29092"
    
    raw_stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_broker)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    
    return (
        raw_stream
        .selectExpr("CAST(value AS STRING) as json_value", "timestamp as kafka_ts")
        .select(from_json(col("json_value"), schema).alias("data"), col("kafka_ts"))
        .select("data.*", "kafka_ts")
    )


def process_annonces_stream(annonces_df):
    """Traitement des annonces togolaises: agrégation par ville et type."""
    return (
        annonces_df
        .withWatermark("kafka_ts", "30 seconds")
        .groupBy(
            window(col("kafka_ts"), "30 seconds", "10 seconds"),
            col("ville"),
            col("type_bien")
        )
        .agg(
            count("*").alias("nb_annonces"),
            avg("prix_xof").alias("prix_moyen_xof"),
            avg("prix_m2_xof").alias("prix_m2_moyen_xof"),
            avg("surface_m2").alias("surface_moyenne"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("ville"),
            col("type_bien"),
            col("nb_annonces"),
            col("prix_moyen_xof").cast("int"),
            col("prix_m2_moyen_xof").cast("int"),
            col("surface_moyenne").cast("int"),
        )
    )


def process_prix_stream(prix_df):
    """Traitement des prix togolais: tendances par ville (XOF)."""
    return (
        prix_df
        .withWatermark("kafka_ts", "30 seconds")
        .groupBy(
            window(col("kafka_ts"), "30 seconds", "10 seconds"),
            col("ville")
        )
        .agg(
            count("*").alias("nb_updates"),
            avg("variation_pct").alias("variation_moyenne"),
            spark_max("nouveau_prix_m2_xof").alias("prix_max_xof"),
            spark_min("nouveau_prix_m2_xof").alias("prix_min_xof"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("ville"),
            col("nb_updates"),
            col("variation_moyenne"),
            col("prix_max_xof"),
            col("prix_min_xof"),
            when(col("variation_moyenne") > 0, "HAUSSE")
            .when(col("variation_moyenne") < 0, "BAISSE")
            .otherwise("STABLE").alias("tendance_globale"),
        )
    )


def process_transactions_stream(transactions_df):
    """Traitement des transactions togolaises: volume et valeur (XOF)."""
    return (
        transactions_df
        .withWatermark("kafka_ts", "30 seconds")
        .groupBy(
            window(col("kafka_ts"), "30 seconds", "10 seconds"),
            col("ville"),
            col("type_acheteur")
        )
        .agg(
            count("*").alias("nb_transactions"),
            spark_sum("prix_vente_xof").alias("volume_total_xof"),
            avg("prix_vente_xof").alias("prix_moyen_vente_xof"),
            avg("duree_vente_jours").alias("duree_moyenne"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("ville"),
            col("type_acheteur"),
            col("nb_transactions"),
            col("volume_total_xof"),
            col("prix_moyen_vente_xof").cast("int"),
            col("duree_moyenne").cast("int"),
        )
    )
