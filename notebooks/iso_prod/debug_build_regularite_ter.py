# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import types as T

source_table = "lab_talan.bronze.regularite_ter"


def afficher_etape(nom, dataframe):
    print(f"\n--- {nom} : {dataframe.count()} lignes ---")
    dataframe.printSchema()
    display(dataframe)


# Étape 1 : charge la table Bronze.
source = spark.table(source_table)
afficher_etape("Source Bronze", source)

# Étape 2 : convertit les dates, les libellés et les mesures.
regularite = (
    source
    .withColumn(
        "date",
        F.to_date(F.concat_ws("-", F.col("date"), F.lit("01")), "yyyy-MM-dd"),
    )
    .withColumn("region", F.initcap(F.trim(F.col("region"))))
    .withColumn(
        "nombre_trains_programmes",
        F.col("nombre_trains_programmes").cast(T.IntegerType()),
    )
    .withColumn(
        "nombre_trains_ayant_circule",
        F.col("nombre_trains_ayant_circule").cast(T.IntegerType()),
    )
    .withColumn(
        "nombre_trains_annules",
        F.col("nombre_trains_annules").cast(T.IntegerType()),
    )
    .withColumn(
        "nombre_trains_en_retard_a_l_arrivee",
        F.col("nombre_trains_en_retard_a_l_arrivee").cast(T.IntegerType()),
    )
    .withColumn(
        "_taux_regularite_source",
        F.trim(F.col("taux_regularite")),
    )
    .withColumn(
        "taux_regularite",
        F.col("taux_regularite").cast(T.DecimalType(7, 2)),
    )
    .withColumn(
        "nombre_trains_a_l_heure_par_train_en_retard_a_l_arrivee",
        F.col(
            "nombre_trains_a_l_heure_par_train_en_retard_a_l_arrivee"
        ).cast(T.DoubleType()),
    )
    .withColumn(
        "commentaires",
        F.nullif(F.trim(F.col("commentaires")), F.lit("")),
    )
)
afficher_etape("Données converties", regularite)

# Étape 3 : affiche les taux absents, puis applique la règle Silver existante.
missing_rate = F.col("_taux_regularite_source").isNull() | (
    F.col("_taux_regularite_source") == ""
)
missing_rows = regularite.where(missing_rate)
afficher_etape("Lignes écartées : taux absent", missing_rows)
regularite = regularite.where(~missing_rate).drop("_taux_regularite_source")
afficher_etape("Lignes avec taux", regularite)

# Étape 4 : déduplique et affiche les anomalies bloquantes.
regularite = regularite.dropDuplicates(["date", "region"])
afficher_etape("Données dédupliquées", regularite)

invalid_rows = regularite.where(
    F.col("date").isNull()
    | F.col("region").isNull()
    | (F.col("nombre_trains_programmes") <= 0)
    | (F.col("nombre_trains_ayant_circule") < 0)
    | (F.col("nombre_trains_annules") < 0)
    | (F.col("nombre_trains_en_retard_a_l_arrivee") < 0)
    | F.col("taux_regularite").isNull()
    | (F.col("taux_regularite") < 0)
    | (F.col("taux_regularite") > 100)
)
afficher_etape("Lignes invalides", invalid_rows)
invalid_count = invalid_rows.count()
if invalid_count > 0:
    raise ValueError(f"{invalid_count} invalid TER rows found; write would fail")

# Étape 5 : affiche le résultat qui serait écrit dans la table Silver.
result = regularite.withColumn("_silver_ingested_at", F.current_timestamp())
afficher_etape("Résultat destiné à lab_talan.silver.regularite_ter", result)
