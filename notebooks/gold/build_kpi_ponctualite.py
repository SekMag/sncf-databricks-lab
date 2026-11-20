# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import types as T

ter_source = "lab_talan.silver.regularite_ter"
tgv_source = "lab_talan.silver.regularite_tgv"
target_table = "lab_talan.gold.kpi_ponctualite"

# Harmonise les indicateurs mensuels tout en conservant les dimensions métier.
ter = spark.table(ter_source).select(
    "date",
    F.lit("TER").alias("transport"),
    F.lit("region").alias("type_dimension"),
    F.col("region").alias("dimension"),
    F.col("taux_regularite").cast(T.DoubleType()).alias("taux_regularite_pct"),
    F.lit(None).cast(T.DoubleType()).alias("ponctualite_origine_pct"),
    F.when(
        F.col("nombre_trains_ayant_circule") > 0,
        F.col("nombre_trains_en_retard_a_l_arrivee")
        / F.col("nombre_trains_ayant_circule")
        * F.lit(100.0),
    )
    .otherwise(F.lit(None).cast(T.DoubleType()))
    .alias("taux_trains_en_retard_pct"),
)

tgv = spark.table(tgv_source).select(
    "date",
    F.lit("TGV").alias("transport"),
    F.lit("axe").alias("type_dimension"),
    F.col("axe").alias("dimension"),
    F.col("regularite_composite").alias("taux_regularite_pct"),
    F.col("ponctualite_origine").alias("ponctualite_origine_pct"),
    F.lit(None).cast(T.DoubleType()).alias("taux_trains_en_retard_pct"),
)

# Les métriques propres à un transport restent nulles pour l'autre.
kpi = (
    ter.unionByName(tgv)
    .withColumn("_gold_generated_at", F.current_timestamp())
)

invalid_rows = kpi.where(
    F.col("date").isNull()
    | F.col("dimension").isNull()
    | (
        F.col("taux_regularite_pct").isNull()
        & F.col("ponctualite_origine_pct").isNull()
        & F.col("taux_trains_en_retard_pct").isNull()
    )
)
invalid_count = invalid_rows.count()
if invalid_count > 0:
    raise ValueError(f"{invalid_count} invalid punctuality KPI rows found")

row_count = kpi.count()
if row_count == 0:
    raise ValueError("No punctuality KPI rows are available")

kpi.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(target_table)

print(f"Wrote {row_count} rows to {target_table}")
