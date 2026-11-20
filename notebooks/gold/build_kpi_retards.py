# Databricks notebook source
from pyspark.sql import functions as F

source_table = "lab_talan.silver.regularite_ter"
target_table = "lab_talan.gold.kpi_retards"

# Agrège les volumes par mois et région pour calculer des taux pondérés.
retards = (
    spark.table(source_table)
    .groupBy("date", "region")
    .agg(
        F.sum("nombre_trains_programmes").alias("nombre_trains_programmes"),
        F.sum("nombre_trains_ayant_circule").alias(
            "nombre_trains_ayant_circule"
        ),
        F.sum("nombre_trains_annules").alias("nombre_trains_annules"),
        F.sum("nombre_trains_en_retard_a_l_arrivee").alias(
            "nombre_trains_en_retard_a_l_arrivee"
        ),
    )
    .withColumn(
        "taux_trains_annules_pct",
        F.when(
            F.col("nombre_trains_programmes") > 0,
            F.col("nombre_trains_annules")
            / F.col("nombre_trains_programmes")
            * F.lit(100.0),
        ),
    )
    .withColumn(
        "taux_trains_en_retard_pct",
        F.when(
            F.col("nombre_trains_ayant_circule") > 0,
            F.col("nombre_trains_en_retard_a_l_arrivee")
            / F.col("nombre_trains_ayant_circule")
            * F.lit(100.0),
        ),
    )
    .withColumn("_gold_generated_at", F.current_timestamp())
)

invalid_rows = retards.where(
    F.col("date").isNull()
    | F.col("region").isNull()
    | F.col("nombre_trains_programmes").isNull()
    | F.col("nombre_trains_ayant_circule").isNull()
    | F.col("nombre_trains_annules").isNull()
    | F.col("nombre_trains_en_retard_a_l_arrivee").isNull()
)
invalid_count = invalid_rows.count()
if invalid_count > 0:
    raise ValueError(f"{invalid_count} invalid delay KPI rows found")

row_count = retards.count()
if row_count == 0:
    raise ValueError("No delay KPI rows are available")

retards.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(target_table)

print(f"Wrote {row_count} rows to {target_table}")
