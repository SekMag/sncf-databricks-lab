# Databricks notebook source
from pyspark.sql import Window
from pyspark.sql import functions as F

ter_source = "lab_talan.silver.regularite_ter"
tgv_source = "lab_talan.silver.regularite_tgv"
frequentation_source = "lab_talan.silver.frequentation_gares"

spark.sql("CREATE SCHEMA IF NOT EXISTS lab_talan.gold")

ter = spark.table(ter_source).select(
    F.col("date").cast("date").alias("periode"),
    F.trim(F.col("region")).alias("region"),
    F.col("nombre_trains_ayant_circule").cast("long").alias("trains_circules"),
    F.col("nombre_trains_en_retard_a_l_arrivee")
    .cast("long")
    .alias("trains_en_retard"),
)
tgv = spark.table(tgv_source).select(
    F.col("date").cast("date").alias("periode"),
    F.trim(F.col("axe")).alias("axe"),
    F.col("regularite_composite").cast("double").alias("regularite_composite"),
    F.col("ponctualite_origine").cast("double").alias("ponctualite_origine"),
)
frequentation = spark.table(frequentation_source).select(
    F.trim(F.col("code_uic")).alias("code_uic"),
    F.col("total_voyageurs_2025")
    .cast("double")
    .alias("total_voyageurs_2025"),
    F.col("total_voyageurs_plus_non_voyageurs_2025")
    .cast("double")
    .alias("total_voyageurs_plus_non_voyageurs_2025"),
)

if ter.limit(1).count() == 0:
    raise ValueError(f"No TER regularity rows found in {ter_source}")
if tgv.limit(1).count() == 0:
    raise ValueError(f"No TGV regularity rows found in {tgv_source}")
if frequentation.limit(1).count() == 0:
    raise ValueError(f"No station fréquentation rows found in {frequentation_source}")

invalid_ter = ter.where(
    F.col("periode").isNull()
    | F.col("region").isNull()
    | F.col("trains_circules").isNull()
    | F.col("trains_en_retard").isNull()
    | (F.col("trains_circules") < 0)
    | (F.col("trains_en_retard") < 0)
    | (F.col("trains_en_retard") > F.col("trains_circules"))
)
invalid_ter_count = invalid_ter.count()
if invalid_ter_count > 0:
    raise ValueError(f"{invalid_ter_count} invalid TER rows found in {ter_source}")

invalid_tgv = tgv.where(
    F.col("periode").isNull()
    | F.col("axe").isNull()
    | F.col("regularite_composite").isNull()
    | F.col("ponctualite_origine").isNull()
    | (F.col("regularite_composite") < 0)
    | (F.col("regularite_composite") > 100)
    | (F.col("ponctualite_origine") < 0)
    | (F.col("ponctualite_origine") > 100)
)
invalid_tgv_count = invalid_tgv.count()
if invalid_tgv_count > 0:
    raise ValueError(f"{invalid_tgv_count} invalid TGV rows found in {tgv_source}")

invalid_frequentation = frequentation.where(
    F.col("code_uic").isNull()
    | F.col("total_voyageurs_2025").isNull()
    | F.col("total_voyageurs_plus_non_voyageurs_2025").isNull()
    | (F.col("total_voyageurs_2025") < 0)
    | (F.col("total_voyageurs_plus_non_voyageurs_2025") < 0)
)
invalid_frequentation_count = invalid_frequentation.count()
if invalid_frequentation_count > 0:
    raise ValueError(
        f"{invalid_frequentation_count} invalid station fréquentation rows "
        f"found in {frequentation_source}"
    )

ter_2025 = ter.where(F.year("periode") == 2025)
tgv_2025 = tgv.where(F.year("periode") == 2025)
if ter_2025.limit(1).count() == 0:
    raise ValueError("No TER regularity rows found for 2025")
if tgv_2025.limit(1).count() == 0:
    raise ValueError("No TGV regularity rows found for 2025")

frequentation_summary = frequentation.agg(
    F.countDistinct("code_uic").alias("nb_gares"),
    F.sum("total_voyageurs_2025").alias("total_voyageurs_2025"),
    F.sum("total_voyageurs_plus_non_voyageurs_2025").alias(
        "total_voyageurs_plus_non_voyageurs_2025"
    ),
)
ter_summary_2025 = ter_2025.agg(
    F.sum("trains_circules").alias("trains_circules_ter_2025"),
    F.sum("trains_en_retard").alias("trains_en_retard_ter_2025"),
    F.countDistinct("periode").alias("mois_ter_observes_2025"),
)
tgv_monthly_2025 = tgv_2025.groupBy("periode").agg(
    F.avg("ponctualite_origine").alias("ponctualite_tgv_mensuelle_pct"),
    F.avg("regularite_composite").alias("regularite_tgv_mensuelle_pct"),
)
tgv_summary_2025 = tgv_monthly_2025.agg(
    F.avg("ponctualite_tgv_mensuelle_pct").alias(
        "ponctualite_tgv_moyenne_pct"
    ),
    F.avg("regularite_tgv_mensuelle_pct").alias(
        "regularite_tgv_moyenne_pct"
    ),
    F.count("*").alias("mois_tgv_observes_2025"),
)
kpi_national_frequentation_performance = (
    frequentation_summary.crossJoin(ter_summary_2025)
    .crossJoin(tgv_summary_2025)
    .select(
        F.lit(2025).alias("annee"),
        "nb_gares",
        "total_voyageurs_2025",
        "total_voyageurs_plus_non_voyageurs_2025",
        F.when(
            F.col("trains_circules_ter_2025") > 0,
            F.round(
                (
                    F.col("trains_circules_ter_2025")
                    - F.col("trains_en_retard_ter_2025")
                )
                / F.col("trains_circules_ter_2025")
                * 100,
                2,
            ),
        ).alias("ponctualite_ter_pct"),
        F.when(
            F.col("trains_circules_ter_2025") > 0,
            F.round(
                F.col("trains_en_retard_ter_2025")
                / F.col("trains_circules_ter_2025")
                * 100,
                2,
            ),
        ).alias("taux_retard_ter_pct"),
        F.round("ponctualite_tgv_moyenne_pct", 2).alias(
            "ponctualite_tgv_moyenne_pct"
        ),
        F.round("regularite_tgv_moyenne_pct", 2).alias(
            "regularite_tgv_moyenne_pct"
        ),
        "mois_ter_observes_2025",
        "mois_tgv_observes_2025",
    )
)

ter_by_region = ter.groupBy("periode", "region").agg(
    F.sum("trains_circules").alias("trains_circules"),
    F.sum("trains_en_retard").alias("trains_en_retard"),
)
ter_by_network = ter.groupBy("periode").agg(
    F.sum("trains_circules").alias("trains_circules"),
    F.sum("trains_en_retard").alias("trains_en_retard"),
).withColumn("reseau", F.lit("TER"))


def ter_punctuality(dataframe, entity_type, entity_column):
    return dataframe.select(
        "periode",
        F.lit("TER").alias("mode"),
        F.lit(entity_type).alias("entite_type"),
        F.col(entity_column).alias("entite"),
        F.when(
            F.col("trains_circules") > 0,
            F.round(
                (
                    F.col("trains_circules").cast("double")
                    - F.col("trains_en_retard").cast("double")
                )
                / F.col("trains_circules").cast("double")
                * 100,
                2,
            ),
        )
        .otherwise(F.lit(None).cast("double"))
        .alias("taux_ponctualite_pct"),
        F.lit("arrivee").alias("base_mesure"),
    )


ter_punctuality_by_region = ter_punctuality(
    ter_by_region, "region", "region"
)
ter_punctuality_by_network = ter_punctuality(
    ter_by_network, "reseau", "reseau"
)

tgv_by_axis = tgv.groupBy("periode", "axe").agg(
    F.avg("ponctualite_origine").alias("taux_ponctualite_pct")
)
tgv_by_network = tgv.groupBy("periode").agg(
    F.avg("ponctualite_origine").alias("taux_ponctualite_pct")
).withColumn("reseau", F.lit("TGV"))

tgv_punctuality_by_axis = tgv_by_axis.select(
    "periode",
    F.lit("TGV").alias("mode"),
    F.lit("axe").alias("entite_type"),
    F.col("axe").alias("entite"),
    F.round("taux_ponctualite_pct", 2).alias("taux_ponctualite_pct"),
    F.lit("origine").alias("base_mesure"),
)
tgv_punctuality_by_network = tgv_by_network.select(
    "periode",
    F.lit("TGV").alias("mode"),
    F.lit("reseau").alias("entite_type"),
    F.col("reseau").alias("entite"),
    F.round("taux_ponctualite_pct", 2).alias("taux_ponctualite_pct"),
    F.lit("origine").alias("base_mesure"),
)

kpi_ponctualite = (
    ter_punctuality_by_region
    .unionByName(ter_punctuality_by_network)
    .unionByName(tgv_punctuality_by_axis)
    .unionByName(tgv_punctuality_by_network)
)


def ter_delay_rate(dataframe, entity_type, entity_column):
    return dataframe.select(
        "periode",
        F.lit(entity_type).alias("entite_type"),
        F.col(entity_column).alias("entite"),
        "trains_circules",
        "trains_en_retard",
        F.when(
            F.col("trains_circules") > 0,
            F.round(
                F.col("trains_en_retard").cast("double")
                / F.col("trains_circules").cast("double")
                * 100,
                2,
            ),
        )
        .otherwise(F.lit(None).cast("double"))
        .alias("taux_retard_pct"),
    )


kpi_retards = (
    ter_delay_rate(ter_by_region, "region", "region")
    .unionByName(ter_delay_rate(ter_by_network, "reseau", "reseau"))
)

axis_rank = Window.orderBy(
    F.desc("taux_ponctualite_moyen_pct"),
    F.asc("axe"),
)
top_axes = (
    tgv.groupBy("axe")
    .agg(
        F.round(F.avg("ponctualite_origine"), 2).alias(
            "taux_ponctualite_moyen_pct"
        ),
        F.round(F.avg("regularite_composite"), 2).alias(
            "regularite_composite_moyenne_pct"
        ),
        F.countDistinct("periode").alias("mois_observes"),
    )
    .withColumn("rang", F.row_number().over(axis_rank))
    .select(
        "rang",
        "axe",
        "taux_ponctualite_moyen_pct",
        "regularite_composite_moyenne_pct",
        "mois_observes",
    )
)

region_summary = ter.groupBy("region").agg(
    F.sum("trains_circules").alias("trains_circules"),
    F.sum("trains_en_retard").alias("trains_en_retard"),
    F.countDistinct("periode").alias("mois_observes"),
)
region_summary = region_summary.withColumn(
    "taux_ponctualite_pct",
    F.when(
        F.col("trains_circules") > 0,
        F.round(
            (
                F.col("trains_circules").cast("double")
                - F.col("trains_en_retard").cast("double")
            )
            / F.col("trains_circules").cast("double")
            * 100,
            2,
        ),
    ),
)
region_rank = Window.orderBy(
    F.desc("taux_ponctualite_pct"),
    F.asc("region"),
)
top_regions = (
    region_summary.where(F.col("taux_ponctualite_pct").isNotNull())
    .withColumn("rang", F.row_number().over(region_rank))
    .select(
        "rang",
        "region",
        "taux_ponctualite_pct",
        "trains_circules",
        "mois_observes",
    )
)


def write_gold_table(dataframe, table_name):
    row_count = dataframe.count()
    if row_count == 0:
        raise ValueError(f"No KPI rows available for {table_name}")

    (
        dataframe.withColumn("_gold_computed_at", F.current_timestamp())
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(table_name)
    )
    print(f"Wrote {row_count} rows to {table_name}")


write_gold_table(kpi_ponctualite, "lab_talan.gold.kpi_ponctualite")
write_gold_table(kpi_retards, "lab_talan.gold.kpi_retards")
write_gold_table(top_axes, "lab_talan.gold.top_axes")
write_gold_table(top_regions, "lab_talan.gold.top_regions")
write_gold_table(
    kpi_national_frequentation_performance,
    "lab_talan.gold.kpi_national_frequentation_performance",
)
