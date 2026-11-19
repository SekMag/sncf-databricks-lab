# Databricks notebook source
from pyspark.sql import functions as F

source_path = "/Volumes/lab_talan/bronze/raw_files/frequentation-gares.csv"
target_table = "lab_talan.bronze.frequentation_gares"
column_mapping = {
    "Nom de la gare": "nom_de_la_gare",
    "Code UIC": "code_uic",
    "Code postal": "code_postal",
    "Direction Régionale Gares": "direction_regionale_gares",
    "Segmentation DRG": "segmentation_drg",
    "Segmentation Marketing": "segmentation_marketing",
    "Total Voyageurs 2025": "total_voyageurs_2025",
    "Total Voyageurs + Non voyageurs 2025": "total_voyageurs_plus_non_voyageurs_2025",
}

raw_data = (
    spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", ";")
    .option("encoding", "UTF-8")
    .option("inferSchema", "false")
    .load(source_path)
)

if set(raw_data.columns) != set(column_mapping):
    raise ValueError(f"Unexpected frequentation CSV columns: {raw_data.columns}")

frequentation = raw_data.select(
    *[
        F.col(f"`{source_column}`").alias(target_column)
        for source_column, target_column in column_mapping.items()
    ],
    F.lit(source_path).alias("_source_file"),
    F.current_timestamp().alias("_ingested_at"),
)

row_count = frequentation.count()
if row_count == 0:
    raise ValueError(f"No frequentation rows were read from {source_path}")

(
    frequentation.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"Wrote {row_count} rows to {target_table}")


