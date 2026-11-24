# Databricks notebook source
from pyspark.sql import functions as F

source_path = "/Volumes/lab_talan/bronze/raw_files/frequentation-gares.csv"
target_table = "lab_talan.bronze.frequentation_gares"
column_mapping = {
    "Nom de la gare": "nom_de_la_gare",
    "Code UIC": "code_uic",
    "Code postal": "code_postal",
    "Direction Régionale Gares": "direction_regionale_gares",
    "Segmentation DRG": "segmentation_drg",
    "Segmentation Marketing": "segmentation_marketing",
    "Total Voyageurs 2025": "total_voyageurs_2025",
    "Total Voyageurs + Non voyageurs 2025": (
        "total_voyageurs_plus_non_voyageurs_2025"
    ),
}


def afficher_etape(nom, dataframe):
    print(f"\n--- {nom} : {dataframe.count()} lignes ---")
    dataframe.printSchema()
    display(dataframe)


# COMMAND ----------

# Étape 1 : lit et affiche le CSV brut, sans écrire de table.
raw_data = (
    spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", ";")
    .option("encoding", "UTF-8")
    .option("inferSchema", "false")
    .load(source_path)
)
afficher_etape("CSV brut", raw_data)

# COMMAND ----------

# Étape 2 : vérifie le schéma source avant les renommages.
if set(raw_data.columns) != set(column_mapping):
    raise ValueError(f"Unexpected frequentation CSV columns: {raw_data.columns}")
print(f"Colonnes détectées : {raw_data.columns}")

# COMMAND ----------

# Étape 3 : renomme les colonnes et ajoute les métadonnées Bronze.
frequentation = raw_data.select(
    *[
        F.col(f"`{source_column}`").alias(target_column)
        for source_column, target_column in column_mapping.items()
    ],
    F.lit(source_path).alias("_source_file"),
    F.current_timestamp().alias("_ingested_at"),
)
afficher_etape(f"Résultat destiné à {target_table}", frequentation)
