# Databricks notebook source
from pyspark.sql import functions as F

source_path = "/Volumes/lab_talan/bronze/raw_files/regularite-mensuelle-tgv-axes.csv"
target_table = "lab_talan.bronze.regularite_tgv"
column_mapping = {
    "Date": "date",
    "Axe": "axe",
    "Régularité composite": "regularite_composite",
    "Ponctualité origine": "ponctualite_origine",
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
    raise ValueError(f"Unexpected TGV regularity CSV columns: {raw_data.columns}")

regularite_tgv = raw_data.select(
    *[
        F.col(f"`{source_column}`").alias(target_column)
        for source_column, target_column in column_mapping.items()
    ],
    F.lit(source_path).alias("_source_file"),
    F.current_timestamp().alias("_ingested_at"),
)

row_count = regularite_tgv.count()
if row_count == 0:
    raise ValueError(f"No TGV regularity rows were read from {source_path}")

(
    regularite_tgv.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"Wrote {row_count} rows to {target_table}")
