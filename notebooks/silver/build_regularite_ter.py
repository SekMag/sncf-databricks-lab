# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import types as T

source_table = "lab_talan.bronze.regularite_ter"
target_table = "lab_talan.silver.regularite_ter"

source = spark.table(source_table)

# Convertit la période mensuelle en date du premier jour du mois, nettoie les libellés
# et transforme les mesures conservées en texte dans Bronze vers leurs types métier.
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
        "taux_regularite",
        F.col("taux_regularite").cast(T.DecimalType(7, 2)),
    )
    .withColumn(
        "nombre_trains_a_l_heure_par_train_en_retard_a_l_arrivee",
        F.col(
            "nombre_trains_a_l_heure_par_train_en_retard_a_l_arrivee"
        ).cast(T.DoubleType()),
    )
     .withColumn("commentaires", F.nullif(F.trim(F.col("commentaires")), F.lit("")))
    .dropDuplicates(["date", "region"])
)

# Les données incohérentes font échouer le traitement avant toute écriture.
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

invalid_count = invalid_rows.count()
if invalid_count > 0:
    raise ValueError(
        f"{invalid_count} invalid TER rows found; write aborted"
    )

regularite = regularite.withColumn("_silver_ingested_at", F.current_timestamp())

row_count = regularite.count()
if row_count == 0:
    raise ValueError("No TER regularity rows are available")

# Publie une table Silver dédupliquée et typée.
regularite.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(target_table)

print(f"Wrote {row_count} rows to {target_table}")
