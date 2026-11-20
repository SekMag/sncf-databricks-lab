# Databricks notebook source
from pyspark.sql import functions as F

source_path = (
    "/Volumes/lab_talan/bronze/raw_files/regularite-mensuelle-tgv-axes.csv"
)
target_table = "lab_talan.bronze.regularite_tgv"
column_mapping = {
    "Date": "date",
    "Axe": "axe",
    "Régularité composite": "regularite_composite",
    "Ponctualité origine": "ponctualite_origine",
}


def afficher_etape(nom, dataframe):
    print(f"\n--- {nom} : {dataframe.count()} lignes ---")
    dataframe.printSchema()
    display(dataframe)


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

# Étape 2 : vérifie le schéma source avant les renommages.
if set(raw_data.columns) != set(column_mapping):
    raise ValueError(f"Unexpected TGV regularity CSV columns: {raw_data.columns}")
print(f"Colonnes détectées : {raw_data.columns}")

# Étape 3 : renomme les colonnes et ajoute les métadonnées Bronze.
regularite_tgv = raw_data.select(
    *[
        F.col(f"`{source_column}`").alias(target_column)
        for source_column, target_column in column_mapping.items()
    ],
    F.lit(source_path).alias("_source_file"),
    F.current_timestamp().alias("_ingested_at"),
)
afficher_etape(f"Résultat destiné à {target_table}", regularite_tgv)
