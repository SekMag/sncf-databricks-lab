# Databricks notebook source
from pyspark.sql import functions as F

source_path = (
    "/Volumes/lab_talan/bronze/raw_files/regularite-mensuelle-ter.csv"
)
target_table = "lab_talan.bronze.regularite_ter"
column_mapping = {
    "Date": "date",
    "Région": "region",
    "Nombre de trains programmés": "nombre_trains_programmes",
    "Nombre de trains ayant circulé": "nombre_trains_ayant_circule",
    "Nombre de trains annulés": "nombre_trains_annules",
    "Nombre de trains en retard à l'arrivée": (
        "nombre_trains_en_retard_a_l_arrivee"
    ),
    "Taux de régularité": "taux_regularite",
    "Nombre de trains à l'heure pour un train en retard à l'arrivée": (
        "nombre_trains_a_l_heure_par_train_en_retard_a_l_arrivee"
    ),
    "Commentaires": "commentaires",
}


def afficher_etape(nom, dataframe):
    print(f"\n--- {nom} : {dataframe.count()} lignes ---")
    dataframe.printSchema()
    display(dataframe)


# Étape 1 : lit les commentaires multilignes du CSV TER.
raw_data = (
    spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", ";")
    .option("encoding", "UTF-8")
    .option("inferSchema", "false")
    .option("multiLine", "true")
    .option("escape", '"')
    .load(source_path)
)
afficher_etape("CSV brut", raw_data)

# Étape 2 : vérifie les colonnes puis isole les lignes au format mensuel attendu.
if set(raw_data.columns) != set(column_mapping):
    raise ValueError(f"Unexpected TER regularity CSV columns: {raw_data.columns}")

valid_period = F.coalesce(
    F.col("Date").rlike(r"^[0-9]{4}-(0[1-9]|1[0-2])$"),
    F.lit(False),
)
invalid_periods = raw_data.where(~valid_period)
afficher_etape("Fragments ignorés : date mensuelle invalide", invalid_periods)

valid_data = raw_data.where(valid_period)
afficher_etape("Lignes avec période valide", valid_data)

# Étape 3 : renomme les colonnes et ajoute les métadonnées Bronze.
regularite_ter = valid_data.select(
    *[
        F.col(f"`{source_column}`").alias(target_column)
        for source_column, target_column in column_mapping.items()
    ],
    F.lit(source_path).alias("_source_file"),
    F.current_timestamp().alias("_ingested_at"),
)
afficher_etape(f"Résultat destiné à {target_table}", regularite_ter)
