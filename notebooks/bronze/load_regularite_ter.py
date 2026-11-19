# Databricks notebook source
from pyspark.sql import functions as F

source_path = "/Volumes/lab_talan/bronze/raw_files/regularite-mensuelle-ter.csv"
target_table = "lab_talan.bronze.regularite_ter"
column_mapping = {
    "Date": "date",
    "Région": "region",
    "Nombre de trains programmés": "nombre_trains_programmes",
    "Nombre de trains ayant circulé": "nombre_trains_ayant_circule",
    "Nombre de trains annulés": "nombre_trains_annules",
    "Nombre de trains en retard à l'arrivée": "nombre_trains_en_retard_a_l_arrivee",
    "Taux de régularité": "taux_regularite",
    "Nombre de trains à l'heure pour un train en retard à l'arrivée": (
        "nombre_trains_a_l_heure_par_train_en_retard_a_l_arrivee"
    ),
    "Commentaires": "commentaires",
}

raw_data = (
    spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", ";")
    .option("encoding", "UTF-8")
    .option("inferSchema", "false")
    .option("multiLine", "true")
    # Le CSV échappe les guillemets internes en les doublant.
    .option("escape", '"')
    .load(source_path)
)

if set(raw_data.columns) != set(column_mapping):
    raise ValueError(f"Unexpected TER regularity CSV columns: {raw_data.columns}")

regularite_ter = raw_data.select(
    *[
        F.col(f"`{source_column}`").alias(target_column)
        for source_column, target_column in column_mapping.items()
    ],
    F.lit(source_path).alias("_source_file"),
    F.current_timestamp().alias("_ingested_at"),
)

row_count = regularite_ter.count()
if row_count == 0:
    raise ValueError(f"No TER regularity rows were read from {source_path}")

(
    regularite_ter.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(target_table)
)

print(f"Wrote {row_count} rows to {target_table}")
