import ast
import csv
import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
DATASETS = (
    (
        "load_ref_gares.py",
        "gares/gares-de-voyageurs.csv",
    ),
    (
        "load_frequentation_gares.py",
        "frequentation/frequentation-gares.csv",
    ),
    (
        "load_regularite_ter.py",
        "regularite_ter/regularite-mensuelle-ter.csv",
    ),
    (
        "load_regularite_tgv.py",
        "regularite_tgv/regularite-mensuelle-tgv-axes.csv",
    ),
)


class BronzeColumnMappingTests(unittest.TestCase):
    def test_notebook_mappings_match_csv_headers(self):
        for notebook_name, csv_relative_path in DATASETS:
            with self.subTest(notebook=notebook_name):
                notebook_path = ROOT / "notebooks" / "bronze" / notebook_name
                source_path = ROOT / "data" / "raw" / csv_relative_path
                notebook_source = notebook_path.read_text(encoding="utf-8")
                syntax_tree = ast.parse(notebook_source)
                mapping = next(
                    ast.literal_eval(node.value)
                    for node in syntax_tree.body
                    if isinstance(node, ast.Assign)
                    and any(
                        isinstance(target, ast.Name)
                        and target.id == "column_mapping"
                        for target in node.targets
                    )
                )

                with source_path.open(
                    encoding="utf-8-sig", newline=""
                ) as source_file:
                    headers = csv.DictReader(
                        source_file, delimiter=";"
                    ).fieldnames

                self.assertEqual(set(headers), set(mapping))
                self.assertEqual(len(mapping), len(set(mapping.values())))
                self.assertTrue(
                    all(
                        name.replace("_", "").isalnum()
                        and name == name.lower()
                        for name in mapping.values()
                    )
                )

    def test_ter_notebook_reads_multiline_comments(self):
        notebook_path = (
            ROOT / "notebooks" / "bronze" / "load_regularite_ter.py"
        )
        notebook_source = notebook_path.read_text(encoding="utf-8")

        self.assertIn('.option("multiLine", "true")', notebook_source)
        self.assertIn(".option(\"escape\", '\"')", notebook_source)
        self.assertIn(
            'F.col("Date").rlike(r"^[0-9]{4}-(0[1-9]|1[0-2])$")',
            notebook_source,
        )


class SilverNotebookTests(unittest.TestCase):
    SILVER_NOTEBOOKS = (
        "build_regularite_ter.py",
        "build_regularite_tgv.py",
        "build_frequentation_gares.py",
    )

    def test_silver_notebooks_exist_and_are_syntax_valid(self):
        for notebook_name in self.SILVER_NOTEBOOKS:
            notebook_path = ROOT / "notebooks" / "silver" / notebook_name
            with self.subTest(notebook=notebook_name):
                notebook_source = notebook_path.read_text(encoding="utf-8")
                ast.parse(notebook_source)
                self.assertIn(".saveAsTable(target_table)", notebook_source)
                self.assertIn('mode("overwrite")', notebook_source)

    def test_job_defines_silver_dependencies(self):
        jobs_path = ROOT / "resources" / "jobs.yml"
        jobs_source = jobs_path.read_text(encoding="utf-8")

        required_tasks = (
            "load_ref_gares",
            "load_frequentation_gares",
            "load_regularite_ter",
            "load_regularite_tgv",
            "build_silver_frequentation",
            "build_silver_regularite_ter",
            "build_silver_regularite_tgv",
        )
        for task_key in required_tasks:
            with self.subTest(task_key=task_key):
                self.assertIn(f"- task_key: {task_key}", jobs_source)

        self.assertIn(
            "depends_on:\n            - task_key: load_ref_gares",
            jobs_source,
        )

    def test_ter_excludes_missing_rates_and_reports_the_count(self):
        notebook_path = ROOT / "notebooks" / "silver" / "build_regularite_ter.py"
        notebook_source = notebook_path.read_text(encoding="utf-8")

        self.assertIn('F.col("_taux_regularite_source") == ""', notebook_source)
        self.assertIn('drop("_taux_regularite_source")', notebook_source)
        self.assertIn("missing_rate_count", notebook_source)
        self.assertIn("Excluded {missing_rate_count}", notebook_source)

    def test_tgv_excludes_missing_metrics_and_reports_the_count(self):
        notebook_path = ROOT / "notebooks" / "silver" / "build_regularite_tgv.py"
        notebook_source = notebook_path.read_text(encoding="utf-8")

        self.assertIn(
            'F.col("_regularite_composite_source") == ""',
            notebook_source,
        )
        self.assertIn(
            'F.col("_ponctualite_origine_source") == ""',
            notebook_source,
        )
        self.assertIn("missing_metrics_count", notebook_source)
        self.assertIn("Excluded {missing_metrics_count}", notebook_source)


class GoldNotebookTests(unittest.TestCase):
    EXPECTED_OUTPUTS = {
        "build_kpi_ponctualite.py": "lab_talan.gold.kpi_ponctualite",
        "build_kpi_retards.py": "lab_talan.gold.kpi_retards",
        "build_top_axes.py": "lab_talan.gold.top_axes",
    }

    def test_gold_notebooks_are_valid_and_write_expected_tables(self):
        for notebook_name, target_table in self.EXPECTED_OUTPUTS.items():
            notebook_path = ROOT / "notebooks" / "gold" / notebook_name
            with self.subTest(notebook=notebook_name):
                notebook_source = notebook_path.read_text(encoding="utf-8")
                ast.parse(notebook_source)
                self.assertIn(target_table, notebook_source)
                self.assertIn(".saveAsTable(target_table)", notebook_source)
                self.assertIn('mode("overwrite")', notebook_source)

    def test_kpi_tables_preserve_month_and_transport_dimensions(self):
        notebook_path = ROOT / "notebooks" / "gold" / "build_kpi_ponctualite.py"
        notebook_source = notebook_path.read_text(encoding="utf-8")

        self.assertIn('F.lit("TER").alias("transport")', notebook_source)
        self.assertIn('F.lit("TGV").alias("transport")', notebook_source)
        self.assertIn("unionByName", notebook_source)

        delays_path = ROOT / "notebooks" / "gold" / "build_kpi_retards.py"
        delays_source = delays_path.read_text(encoding="utf-8")
        self.assertIn('.groupBy("date", "region")', delays_source)
        self.assertIn("taux_trains_en_retard_pct", delays_source)

    def test_axis_ranking_uses_composite_regularity(self):
        notebook_path = ROOT / "notebooks" / "gold" / "build_top_axes.py"
        notebook_source = notebook_path.read_text(encoding="utf-8")

        self.assertIn("F.dense_rank()", notebook_source)
        self.assertIn('F.desc("regularite_composite_moyenne_pct")', notebook_source)

    def test_national_kpi_combines_silver_frequentation_and_2025_performance(self):
        notebook_path = ROOT / "notebooks" / "gold" / "build_kpis.py"
        notebook_source = notebook_path.read_text(encoding="utf-8")
        ast.parse(notebook_source)

        self.assertIn("lab_talan.silver.frequentation_gares", notebook_source)
        self.assertIn(
            "lab_talan.gold.kpi_national_frequentation_performance",
            notebook_source,
        )
        self.assertIn('F.year("periode") == 2025', notebook_source)

        jobs_source = (ROOT / "resources" / "jobs.yml").read_text(
            encoding="utf-8"
        )
        gold_task = jobs_source.split("- task_key: build_gold_kpis", 1)[1]
        self.assertIn("- task_key: build_silver_frequentation", gold_task)


class IsoProdNotebookTests(unittest.TestCase):
    def test_iso_prod_notebooks_show_steps_without_writing_tables(self):
        iso_prod_notebooks = sorted(
            (ROOT / "notebooks" / "iso_prod").glob("debug_*.py")
        )
        self.assertEqual(len(iso_prod_notebooks), 10)

        for notebook_path in iso_prod_notebooks:
            with self.subTest(notebook=notebook_path.name):
                notebook_source = notebook_path.read_text(encoding="utf-8")
                syntax_tree = ast.parse(notebook_source)
                step_calls = sum(
                    isinstance(node, ast.Call)
                    and isinstance(node.func, ast.Name)
                    and node.func.id == "afficher_etape"
                    for node in ast.walk(syntax_tree)
                )

                self.assertGreaterEqual(step_calls, 2)
                self.assertIn("display(dataframe)", notebook_source)
                self.assertGreaterEqual(
                    notebook_source.count("# COMMAND ----------"),
                    2,
                )
                self.assertNotIn(".saveAsTable(", notebook_source)
                self.assertNotIn(".write.format(", notebook_source)

    def test_frequentation_quality_branch_is_kept_before_silver_write(self):
        jobs_source = (ROOT / "resources" / "jobs.yml").read_text(
            encoding="utf-8"
        )
        notebook_source = (
            ROOT / "notebooks" / "silver" / "build_frequentation_gares.py"
        ).read_text(encoding="utf-8")

        self.assertIn("task_key: frequentation_has_invalid_rows", jobs_source)
        self.assertIn('outcome: "true"', jobs_source)
        self.assertIn('outcome: "false"', jobs_source)
        self.assertIn(
            "dbutils.jobs.taskValues.set(key=\"invalid_count\", value=invalid_count)",
            notebook_source,
        )


if __name__ == "__main__":
    unittest.main()
