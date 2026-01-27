"""Exporte les tables PostgreSQL (événements) vers HDFS au format Parquet."""
import os

from pyspark.sql import SparkSession

PG_URL  = "jdbc:postgresql://{}:5432/{}".format(
    os.getenv("POSTGRES_HOST", "postgres"), os.getenv("POSTGRES_DB", "bigdata"))
PG_PROPS = {
    "user": os.getenv("POSTGRES_USER", "bigdata"),
    "password": os.environ["POSTGRES_PASSWORD"],
    "driver": "org.postgresql.Driver",
}
HDFS = "hdfs://namenode:9000/data/raw"

spark = SparkSession.builder.appName("ExportToHDFS").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

for table, path in [("ecommerce_events","ecommerce"),("taxi_events","taxi"),("weather_events","weather")]:
    df = spark.read.jdbc(PG_URL, table, properties=PG_PROPS)
    print(f"  {table}: {df.count()} lignes -> HDFS {path}")
    df.write.mode("overwrite").parquet(f"{HDFS}/{path}/")
    print(f"  OK")

print("Export HDFS termine")
spark.stop()
