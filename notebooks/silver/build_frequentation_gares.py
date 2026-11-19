# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import types as T

frequentation_source = "lab_talan.bronze.frequentation_gares"
gares_source = "lab_talan.bronze.ref_gares"
target_table = "lab_talan.silver.frequentation_gares"

# Normalise les champs clés et convertit les volumes en valeurs numériques.
frequentation = (
    spark.table(frequentation_source)
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

# Prépare une ligne de référence par code UIC pour éviter de multiplier les lignes à la jointure.
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

# Enrichit la fréquentation sans exclure les gares absentes du référentiel.
frequentation = (
    frequentation
    .join(gares, "code_uic", "left")
    .withColumn(
        "_gare_reference_trouvee",
        F.col("trigramme_gare").isNotNull(),
    )
    .dropDuplicates(["code_uic"])
    .withColumn("_silver_ingested_at", F.current_timestamp())
)

# Bloque l'écriture si les identifiants ou les volumes obligatoires sont invalides.
invalid_rows = frequentation.where(
    F.col("code_uic").isNull()
    | F.col("nom_de_la_gare").isNull()
    | F.col("total_voyageurs_2025").isNull()
    | F.col("total_voyageurs_plus_non_voyageurs_2025").isNull()
    | (F.col("total_voyageurs_2025") < 0)
    | (F.col("total_voyageurs_plus_non_voyageurs_2025") < 0)
)

invalid_count = invalid_rows.count()
if invalid_count > 0:
    raise ValueError(
        f"{invalid_count} invalid frequentation rows found; write aborted"
    )

row_count = frequentation.count()
if row_count == 0:
    raise ValueError("No station frequentation rows are available")

# Remplace la table Silver avec les données contrôlées et enrichies.
frequentation.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(target_table)

print(f"Wrote {row_count} rows to {target_table}")
