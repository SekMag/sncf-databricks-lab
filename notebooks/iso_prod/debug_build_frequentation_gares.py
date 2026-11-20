# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import types as T

frequentation_source = "lab_talan.bronze.frequentation_gares"
gares_source = "lab_talan.bronze.ref_gares"


def afficher_etape(nom, dataframe):
    print(f"\n--- {nom} : {dataframe.count()} lignes ---")
    dataframe.printSchema()
    display(dataframe)


# Étape 1 : charge les tables Bronze.
source = spark.table(frequentation_source)
afficher_etape("Fréquentation Bronze", source)

# Étape 2 : nettoie les champs et convertit les volumes.
frequentation = (
    source
    .withColumn("nom_de_la_gare", F.initcap(F.trim(F.col("nom_de_la_gare"))))
    .withColumn("code_uic", F.trim(F.col("code_uic")))
    .withColumn("code_postal", F.trim(F.col("code_postal")))
    .withColumn(
        "total_voyageurs_2025",
        F.col("total_voyageurs_2025").cast(T.DoubleType()),
    )
    .withColumn(
        "total_voyageurs_plus_non_voyageurs_2025",
        F.col("total_voyageurs_plus_non_voyageurs_2025").cast(T.DoubleType()),
    )
)
afficher_etape("Fréquentation normalisée", frequentation)

# Étape 3 : prépare une ligne du référentiel par code UIC.
gares = (
    spark.table(gares_source)
    .select(
        F.trim(F.col("code_uic")).alias("code_uic"),
        F.col("trigramme").alias("trigramme_gare"),
        F.col("segments_drg").alias("segment_drg_gare"),
        F.col("position_geographique").alias("position_geographique_gare"),
    )
    .dropDuplicates(["code_uic"])
)
afficher_etape("Référentiel dédupliqué", gares)

# Étape 4 : enrichit les données sans exclure les gares non référencées.
frequentation = (
    frequentation
    .join(gares, "code_uic", "left")
    .withColumn(
        "_gare_reference_trouvee",
        F.col("trigramme_gare").isNotNull(),
    )
    .dropDuplicates(["code_uic"])
)
afficher_etape("Fréquentation enrichie", frequentation)

# Étape 5 : affiche les anomalies qui bloqueraient l'écriture Silver.
invalid_rows = frequentation.where(
    F.col("code_uic").isNull()
    | F.col("nom_de_la_gare").isNull()
    | F.col("total_voyageurs_2025").isNull()
    | F.col("total_voyageurs_plus_non_voyageurs_2025").isNull()
    | (F.col("total_voyageurs_2025") < 0)
    | (F.col("total_voyageurs_plus_non_voyageurs_2025") < 0)
)
afficher_etape("Lignes invalides", invalid_rows)
invalid_count = invalid_rows.count()
if invalid_count > 0:
    raise ValueError(
        f"{invalid_count} invalid frequentation rows found; write would fail"
    )

# Étape 6 : affiche le résultat qui serait écrit dans la table Silver.
result = frequentation.withColumn("_silver_ingested_at", F.current_timestamp())
afficher_etape("Résultat destiné à lab_talan.silver.frequentation_gares", result)
