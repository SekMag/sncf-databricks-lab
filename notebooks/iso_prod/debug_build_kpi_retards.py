# Databricks notebook source
from pyspark.sql import functions as F

source_table = "lab_talan.silver.regularite_ter"


def afficher_etape(nom, dataframe):
    print(f"\n--- {nom} : {dataframe.count()} lignes ---")
    dataframe.printSchema()
    display(dataframe)


# COMMAND ----------

# Étape 1 : charge les données Silver TER.
source = spark.table(source_table)
afficher_etape("Source Silver TER", source)

# COMMAND ----------

# Étape 2 : additionne les volumes par mois et par région.
retards = source.groupBy("date", "region").agg(
    F.sum("nombre_trains_programmes").alias("nombre_trains_programmes"),
    F.sum("nombre_trains_ayant_circule").alias("nombre_trains_ayant_circule"),
    F.sum("nombre_trains_annules").alias("nombre_trains_annules"),
    F.sum("nombre_trains_en_retard_a_l_arrivee").alias(
        "nombre_trains_en_retard_a_l_arrivee"
    ),
)
afficher_etape("Volumes agrégés", retards)

# COMMAND ----------

# Étape 3 : calcule les taux, en évitant les divisions par zéro.
retards = (
    retards
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
)
afficher_etape("Taux calculés", retards)

# COMMAND ----------

# Étape 4 : affiche les lignes qui bloqueraient l'écriture Gold.
invalid_rows = retards.where(
    F.col("date").isNull()
    | F.col("region").isNull()
    | F.col("nombre_trains_programmes").isNull()
    | F.col("nombre_trains_ayant_circule").isNull()
    | F.col("nombre_trains_annules").isNull()
    | F.col("nombre_trains_en_retard_a_l_arrivee").isNull()
)
afficher_etape("Lignes invalides", invalid_rows)

# COMMAND ----------

invalid_count = invalid_rows.count()
if invalid_count > 0:
    raise ValueError(f"{invalid_count} invalid delay KPI rows found; write would fail")

# COMMAND ----------

# Étape 5 : affiche le résultat final, sans écrire la table Gold.
result = retards.withColumn("_gold_generated_at", F.current_timestamp())
afficher_etape("Résultat destiné à lab_talan.gold.kpi_retards", result)
