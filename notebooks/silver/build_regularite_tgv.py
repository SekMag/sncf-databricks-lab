# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql import types as T

source_table = "lab_talan.bronze.regularite_tgv"
target_table = "lab_talan.silver.regularite_tgv"

source = spark.table(source_table)

# Convertit la période mensuelle et les indicateurs en types analytiques,
# puis conserve une seule observation par mois et par axe.
regularite = (
    source
    .withColumn(
        "date",
        F.to_date(F.concat_ws("-", F.col("date"), F.lit("01")), "yyyy-MM-dd"),
    )
    .withColumn("axe", F.initcap(F.trim(F.col("axe"))))
    .withColumn(
        "regularite_composite",
        F.col("regularite_composite").cast(T.DoubleType()),
    )
    .withColumn(
        "ponctualite_origine",
        F.col("ponctualite_origine").cast(T.DoubleType()),
    )
    .dropDuplicates(["date", "axe"])
)

# Les dates, axes et taux doivent être renseignés ; les taux sont des pourcentages.
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

invalid_count = invalid_rows.count()
if invalid_count > 0:
    raise ValueError(
        f"{invalid_count} invalid TGV rows found; write aborted"
    )

regularite = regularite.withColumn("_silver_ingested_at", F.current_timestamp())

row_count = regularite.count()
if row_count == 0:
    raise ValueError("No TGV regularity rows are available")

# Publie la table Silver après validation et déduplication.
regularite.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(target_table)

print(f"Wrote {row_count} rows to {target_table}")
