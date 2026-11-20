# Databricks notebook source
from pyspark.sql import functions as F

ter_source = "lab_talan.silver.regularite_ter"
tgv_source = "lab_talan.silver.regularite_tgv"
target_table = "lab_talan.gold.kpi_ponctualite"

# Les données TER sont disponibles par mois et par région.
ter = spark.table(ter_source).select(
    "date",
    F.lit("TER").alias("transport"),
    F.lit("region").alias("type_dimension"),
    F.col("region").alias("dimension"),
    F.col("taux_regularite").cast("double").alias("taux_regularite_pct"),
    F.lit(None).cast("double").alias("ponctualite_origine_pct"),
    F.when(
        F.col("nombre_trains_ayant_circule") > 0,
        100 * F.col("nombre_trains_en_retard_a_l_arrivee")
        / F.col("nombre_trains_ayant_circule"),
    )
    .otherwise(F.lit(None).cast("double"))
    .alias("taux_trains_en_retard_pct"),
)

# Les données TGV sont disponibles par mois et par axe.
tgv = spark.table(tgv_source).select(
    "date",
    F.lit("TGV").alias("transport"),
    F.lit("axe").alias("type_dimension"),
    F.col("axe").alias("dimension"),
    F.col("regularite_composite").alias("taux_regularite_pct"),
    F.col("ponctualite_origine").alias("ponctualite_origine_pct"),
    F.lit(None).cast("double").alias("taux_trains_en_retard_pct"),
)

# Réunit TER et TGV dans une seule table pour faciliter la comparaison mensuelle.
kpi = ter.unionByName(tgv)
row_count = kpi.count()
if row_count == 0:
    raise ValueError("No punctuality KPI rows are available")

# Enregistre la table Gold.
kpi.withColumn("_gold_generated_at", F.current_timestamp()).write.format(
    "delta"
).mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)

print(f"Wrote {row_count} rows to {target_table}")
