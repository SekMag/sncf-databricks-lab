# Databricks notebook source
from pyspark.sql import functions as F

source_table = "lab_talan.silver.regularite_ter"
target_table = "lab_talan.gold.kpi_retards"

source = spark.table(source_table)

# Additionne les volumes chaque mois et pour chaque région.
retards = source.groupBy("date", "region").agg(
    F.sum("nombre_trains_programmes").alias("nombre_trains_programmes"),
    F.sum("nombre_trains_ayant_circule").alias("nombre_trains_ayant_circule"),
    F.sum("nombre_trains_annules").alias("nombre_trains_annules"),
    F.sum("nombre_trains_en_retard_a_l_arrivee").alias(
        "nombre_trains_en_retard_a_l_arrivee"
    ),
)

# Calcule les taux à partir des totaux, et évite une division par zéro.
retards = (
    retards
    .withColumn(
        "taux_trains_annules_pct",
        F.when(
            F.col("nombre_trains_programmes") > 0,
            100 * F.col("nombre_trains_annules")
            / F.col("nombre_trains_programmes"),
        ),
    )
    .withColumn(
        "taux_trains_en_retard_pct",
        F.when(
            F.col("nombre_trains_ayant_circule") > 0,
            100 * F.col("nombre_trains_en_retard_a_l_arrivee")
            / F.col("nombre_trains_ayant_circule"),
        ),
    )
)

# Vérifie qu'il y a des données avant d'écrire la table Gold.
row_count = retards.count()
if row_count == 0:
    raise ValueError("No delay KPI rows are available")

# Enregistre la table Gold.
retards.withColumn("_gold_generated_at", F.current_timestamp()).write.format(
    "delta"
).mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)

print(f"Wrote {row_count} rows to {target_table}")
