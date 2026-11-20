# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

source_table = "lab_talan.silver.regularite_tgv"
target_table = "lab_talan.gold.top_axes"

# Classe les axes sur la moyenne des indicateurs de leurs mois disponibles.
axes = (
    spark.table(source_table)
    .groupBy("axe")
    .agg(
        F.avg("regularite_composite").alias(
            "regularite_composite_moyenne_pct"
        ),
        F.avg("ponctualite_origine").alias(
            "ponctualite_origine_moyenne_pct"
        ),
        F.countDistinct("date").alias("nombre_mois_observes"),
        F.min("date").alias("premier_mois"),
        F.max("date").alias("dernier_mois"),
    )
    .withColumn(
        "rang",
        F.dense_rank().over(
            Window.orderBy(
                F.desc("regularite_composite_moyenne_pct"),
                F.desc("ponctualite_origine_moyenne_pct"),
                F.asc("axe"),
            )
        ),
    )
    .withColumn("_gold_generated_at", F.current_timestamp())
)

invalid_rows = axes.where(
    F.col("axe").isNull()
    | F.col("regularite_composite_moyenne_pct").isNull()
    | F.col("ponctualite_origine_moyenne_pct").isNull()
    | (F.col("nombre_mois_observes") <= 0)
)
invalid_count = invalid_rows.count()
if invalid_count > 0:
    raise ValueError(f"{invalid_count} invalid TGV axis ranking rows found")

row_count = axes.count()
if row_count == 0:
    raise ValueError("No TGV axes are available for ranking")

axes.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(target_table)

print(f"Wrote {row_count} rows to {target_table}")
