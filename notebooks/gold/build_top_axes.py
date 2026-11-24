# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

source_table = "lab_talan.silver.regularite_tgv"
target_table = "lab_talan.gold.top_axes"

source = spark.table(source_table)

# Calcule les moyennes et la période disponible pour chaque axe.
axes = source.groupBy("axe").agg(
    F.avg("regularite_composite").alias("regularite_composite_moyenne_pct"),
    F.avg("ponctualite_origine").alias("ponctualite_origine_moyenne_pct"),
    F.countDistinct("date").alias("nombre_mois_observes"),
    F.min("date").alias("premier_mois"),
    F.max("date").alias("dernier_mois"),
)

# Classe les axes du meilleur au moins bon, puis départage les égalités.
axes = axes.withColumn(
    "rang",
    F.dense_rank().over(
        Window.orderBy(
            F.desc("regularite_composite_moyenne_pct"),
            F.desc("ponctualite_origine_moyenne_pct"),
            F.asc("axe"),
        )
    ),
)

# Vérifie qu'il y a des données avant d'écrire la table Gold.
row_count = axes.count()
if row_count == 0:
    raise ValueError("No TGV axes are available for ranking")

# Enregistre le classement Gold.
axes.withColumn("_gold_generated_at", F.current_timestamp()).write.format(
    "delta"
).mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)

print(f"Wrote {row_count} rows to {target_table}")
