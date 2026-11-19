# Databricks notebook source
from pyspark.sql import functions as F

source_path = "/Volumes/lab_talan/bronze/raw_files/gares-de-voyageurs.csv"
target_table = "lab_talan.bronze.ref_gares"
column_mapping = {
    "Nom_Gare": "nom_gare",
    "Trigramme": "trigramme",
    "Segment(s) DRG": "segments_drg",
    "Position géographique": "position_geographique",
    "Code commune": "code_commune",
    "Code_UIC": "code_uic",
    "Id_Gare": "id_gare",
}

raw_stations = (
    spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", ";")
    .option("encoding", "UTF-8")
    .option("inferSchema", "false")
    .load(source_path)
)

if set(raw_stations.columns) != set(column_mapping):
    raise ValueError(f"Unexpected station CSV columns: {raw_stations.columns}")

stations = raw_stations.select(
    *[
        F.col(f"`{source_column}`").alias(target_column)
        for source_column, target_column in column_mapping.items()
    ],
    F.lit(source_path).alias("_source_file"),
    F.current_timestamp().alias("_ingested_at"),
)

row_count = stations.count()
if row_count == 0:
    raise ValueError(f"No station rows were read from {source_path}")

(
    stations.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"Wrote {row_count} rows to {target_table}")
