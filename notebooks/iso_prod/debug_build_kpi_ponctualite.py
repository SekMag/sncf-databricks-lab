# Databricks notebook source
from pyspark.sql import functions as F

ter_source = "lab_talan.silver.regularite_ter"
tgv_source = "lab_talan.silver.regularite_tgv"


def afficher_etape(nom, dataframe):
    print(f"\n--- {nom} : {dataframe.count()} lignes ---")
    dataframe.printSchema()
    display(dataframe)


# COMMAND ----------

# Étape 1 : prépare les indicateurs TER par mois et par région.
ter = spark.table(ter_source).select(
    "date",
    F.lit("TER").alias("transport"),
    F.lit("region").alias("type_dimension"),
    F.col("region").alias("dimension"),
    F.col("taux_regularite").cast("double").alias("taux_regularite_pct"),
    F.lit(None).cast("double").alias("ponctualite_origine_pct"),
    F.when(
        F.col("nombre_trains_ayant_circule") > 0,
        F.col("nombre_trains_en_retard_a_l_arrivee")
        / F.col("nombre_trains_ayant_circule")
        * F.lit(100.0),
    )
    .otherwise(F.lit(None).cast("double"))
    .alias("taux_trains_en_retard_pct"),
)
afficher_etape("Indicateurs TER", ter)

# COMMAND ----------

# Étape 2 : prépare les indicateurs TGV par mois et par axe.
tgv = spark.table(tgv_source).select(
    "date",
    F.lit("TGV").alias("transport"),
    F.lit("axe").alias("type_dimension"),
    F.col("axe").alias("dimension"),
    F.col("regularite_composite").alias("taux_regularite_pct"),
    F.col("ponctualite_origine").alias("ponctualite_origine_pct"),
    F.lit(None).cast("double").alias("taux_trains_en_retard_pct"),
)
afficher_etape("Indicateurs TGV", tgv)

# COMMAND ----------

# Étape 3 : réunit les deux transports dans le même schéma.
kpi = ter.unionByName(tgv)
afficher_etape("KPI réunis", kpi)

# COMMAND ----------

# Étape 4 : affiche les lignes qui bloqueraient l'écriture Gold.
invalid_rows = kpi.where(
    F.col("date").isNull()
    | F.col("dimension").isNull()
    | (
        F.col("taux_regularite_pct").isNull()
        & F.col("ponctualite_origine_pct").isNull()
        & F.col("taux_trains_en_retard_pct").isNull()
    )
)
afficher_etape("Lignes invalides", invalid_rows)

# COMMAND ----------

invalid_count = invalid_rows.count()
if invalid_count > 0:
    raise ValueError(
        f"{invalid_count} invalid punctuality KPI rows found; write would fail"
    )

# COMMAND ----------

# Étape 5 : affiche le résultat final, sans écrire la table Gold.
result = kpi.withColumn("_gold_generated_at", F.current_timestamp())
afficher_etape("Résultat destiné à lab_talan.gold.kpi_ponctualite", result)
