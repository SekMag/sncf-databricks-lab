# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import types as T

source_table = "lab_talan.bronze.regularite_tgv"


def afficher_etape(nom, dataframe):
    print(f"\n--- {nom} : {dataframe.count()} lignes ---")
    dataframe.printSchema()
    display(dataframe)


# COMMAND ----------

# Étape 1 : charge la table Bronze.
source = spark.table(source_table)
afficher_etape("Source Bronze", source)

# COMMAND ----------

# Étape 2 : convertit la période et les indicateurs.
regularite = (
    source
    .withColumn(
        "date",
        F.to_date(F.concat_ws("-", F.col("date"), F.lit("01")), "yyyy-MM-dd"),
    )
    .withColumn("axe", F.initcap(F.trim(F.col("axe"))))
    .withColumn(
        "_regularite_composite_source",
        F.trim(F.col("regularite_composite")),
    )
    .withColumn(
        "_ponctualite_origine_source",
        F.trim(F.col("ponctualite_origine")),
    )
    .withColumn(
        "regularite_composite",
        F.col("regularite_composite").cast(T.DoubleType()),
    )
    .withColumn(
        "ponctualite_origine",
        F.col("ponctualite_origine").cast(T.DoubleType()),
    )
)
afficher_etape("Données converties", regularite)

# COMMAND ----------

# Étape 3 : affiche les mesures manquantes, puis applique la règle Silver existante.
missing_metrics = (
    F.col("_regularite_composite_source").isNull()
    | (F.col("_regularite_composite_source") == "")
    | F.col("_ponctualite_origine_source").isNull()
    | (F.col("_ponctualite_origine_source") == "")
)
missing_rows = regularite.where(missing_metrics)
afficher_etape("Lignes écartées : indicateurs absents", missing_rows)

# COMMAND ----------

regularite = regularite.where(~missing_metrics).drop(
    "_regularite_composite_source",
    "_ponctualite_origine_source",
)
afficher_etape("Lignes avec indicateurs", regularite)

# COMMAND ----------

# Étape 4 : déduplique et affiche les anomalies bloquantes.
regularite = regularite.dropDuplicates(["date", "axe"])
afficher_etape("Données dédupliquées", regularite)

invalid_rows = regularite.where(
    F.col("date").isNull()
    | F.col("axe").isNull()
    | F.col("regularite_composite").isNull()
    | F.col("ponctualite_origine").isNull()
    | (F.col("regularite_composite") < 0)
    | (F.col("regularite_composite") > 100)
    | (F.col("ponctualite_origine") < 0)
    | (F.col("ponctualite_origine") > 100)
)
afficher_etape("Lignes invalides", invalid_rows)

# COMMAND ----------

invalid_count = invalid_rows.count()
if invalid_count > 0:
    raise ValueError(f"{invalid_count} invalid TGV rows found; write would fail")

# COMMAND ----------

# Étape 5 : affiche le résultat qui serait écrit dans la table Silver.
result = regularite.withColumn("_silver_ingested_at", F.current_timestamp())
afficher_etape("Résultat destiné à lab_talan.silver.regularite_tgv", result)
