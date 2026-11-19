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


if __name__ == "__main__":
    unittest.main()
