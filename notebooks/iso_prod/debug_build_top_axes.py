# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

source_table = "lab_talan.silver.regularite_tgv"


def afficher_etape(nom, dataframe):
    print(f"\n--- {nom} : {dataframe.count()} lignes ---")
    dataframe.printSchema()
    display(dataframe)


# COMMAND ----------

# Étape 1 : charge les données Silver TGV.
source = spark.table(source_table)
afficher_etape("Source Silver TGV", source)

# COMMAND ----------

# Étape 2 : calcule les moyennes et les périodes disponibles par axe.
axes = source.groupBy("axe").agg(
    F.avg("regularite_composite").alias("regularite_composite_moyenne_pct"),
    F.avg("ponctualite_origine").alias("ponctualite_origine_moyenne_pct"),
    F.countDistinct("date").alias("nombre_mois_observes"),
    F.min("date").alias("premier_mois"),
    F.max("date").alias("dernier_mois"),
)
afficher_etape("Moyennes par axe", axes)

# COMMAND ----------

# Étape 3 : classe les axes selon leur régularité composite moyenne.
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
afficher_etape("Axes classés", axes)

# COMMAND ----------

# Étape 4 : affiche les lignes qui bloqueraient l'écriture Gold.
invalid_rows = axes.where(
    F.col("axe").isNull()
    | F.col("regularite_composite_moyenne_pct").isNull()
    | F.col("ponctualite_origine_moyenne_pct").isNull()
    | (F.col("nombre_mois_observes") <= 0)
)
afficher_etape("Lignes invalides", invalid_rows)

# COMMAND ----------

invalid_count = invalid_rows.count()
if invalid_count > 0:
    raise ValueError(
        f"{invalid_count} invalid TGV axis ranking rows found; write would fail"
    )

# COMMAND ----------

# Étape 5 : affiche le résultat final, sans écrire la table Gold.
result = axes.withColumn("_gold_generated_at", F.current_timestamp())
afficher_etape("Résultat destiné à lab_talan.gold.top_axes", result)
