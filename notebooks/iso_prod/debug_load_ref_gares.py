# Databricks notebook source
from pyspark.sql import functions as F

source_path = "/Volumes/lab_talan/bronze/raw_files/gares-de-voyageurs.csv"
target_table = "lab_talan.bronze.ref_gares"
column_mapping = {
    "Nom_Gare": "nom_gare",
    "Trigramme": "trigramme",
    "Segment(s) DRG": "segments_drg",
    "Position géographique": "position_geographique",
    "Code commune": "code_commune",
    "Code_UIC": "code_uic",
    "Id_Gare": "id_gare",
}


def afficher_etape(nom, dataframe):
    print(f"\n--- {nom} : {dataframe.count()} lignes ---")
    dataframe.printSchema()
    display(dataframe)


# COMMAND ----------

# Étape 1 : lit et affiche le CSV brut, sans écrire de table.
raw_stations = (
    spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", ";")
    .option("encoding", "UTF-8")
    .option("inferSchema", "false")
    .load(source_path)
)
afficher_etape("CSV brut", raw_stations)

# COMMAND ----------

# Étape 2 : vérifie que les colonnes du fichier correspondent au mapping.
if set(raw_stations.columns) != set(column_mapping):
    raise ValueError(f"Unexpected station CSV columns: {raw_stations.columns}")
print(f"Colonnes détectées : {raw_stations.columns}")

# COMMAND ----------

# Étape 3 : renomme les colonnes et ajoute les métadonnées Bronze.
stations = raw_stations.select(
    *[
        F.col(f"`{source_column}`").alias(target_column)
        for source_column, target_column in column_mapping.items()
    ],
    F.lit(source_path).alias("_source_file"),
    F.current_timestamp().alias("_ingested_at"),
)
afficher_etape(f"Résultat destiné à {target_table}", stations)
