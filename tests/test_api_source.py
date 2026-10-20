import ast
import unittest
from pathlib import Path

from api.model_paths import model_source


class TestApiSource(unittest.TestCase):
    def test_api_imports_path_for_model_lookup(self):
        source = Path("api/main.py").read_text(encoding="utf-8")
        tree = ast.parse(source)

        imports_path = any(
            isinstance(node, ast.ImportFrom)
            and node.module == "pathlib"
            and any(alias.name == "Path" for alias in node.names)
            for node in ast.walk(tree)
        )

        self.assertTrue(imports_path)

    def test_models_directory_is_reported_as_bundled(self):
        self.assertEqual(model_source("models"), "bundled")

    def test_other_model_directory_is_reported_as_blob_cache(self):
        self.assertEqual(model_source(".cache/models"), "azure-blob-cache")
