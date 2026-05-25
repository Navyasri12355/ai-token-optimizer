"""
spark/test_pipeline.py
=======================
End-to-end test suite for the Spark pipeline.

Tests are self-contained – they create a tiny synthetic dataset and exercise
all pipeline modules without requiring raw.json or a running ELK stack.

Run:
  python spark/test_pipeline.py            # all tests
  python spark/test_pipeline.py --quick   # skip slow Spark stages
  python spark/test_pipeline.py -v        # verbose output
"""

import sys
import os
import json
import subprocess
import tempfile
import unittest
import logging
from pathlib import Path

ROOT = str(Path(__file__).resolve().parent.parent)
sys.path.insert(0, ROOT)

logging.basicConfig(level=logging.WARNING,
                    format="%(asctime)s %(levelname)s %(message)s")

# ── Synthetic dataset ──────────────────────────────────────────────────────────
SYNTHETIC_RECORDS = [
    {
        "id": f"test_{i}",
        "conversations": [
            {"from": "human", "value": f"Please could you explain concept {i} in detail?"},
            {"from": "gpt",   "value": f"Sure! Concept {i} works by doing X Y Z. " * 5},
            {"from": "human", "value": f"Can you give an example of concept {i}?"},
            {"from": "gpt",   "value": f"Example: ```python\nprint({i})\n```"},
        ],
    }
    for i in range(200)
]


def _write_synthetic(path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(SYNTHETIC_RECORDS, f)


def _run_script(script_lines, timeout=300):
    """Run a list of Python statements in a child process; return exit code."""
    script = "; ".join(script_lines)
    return subprocess.run([sys.executable, "-c", script],
                          timeout=timeout).returncode


def _preprocess_script(root, raw_json, output, sample, stats):
    return [
        f"import sys",
        f"sys.path.insert(0, {repr(root)})",
        f"from spark.preprocess import run_preprocessing",
        f"run_preprocessing("
        f"sample_size=200,"
        f"raw_json={repr(raw_json)},"
        f"output_parquet={repr(output)},"
        f"sample_parquet={repr(sample)},"
        f"stats_path={repr(stats)})",
    ]


# ══════════════════════════════════════════════════════════════════════════════
class TestELKLogger(unittest.TestCase):
    """Test ELK logger – no ELK stack required (offline graceful fallback)."""

    def test_import(self):
        from spark.elk_logger import get_elk_logger
        self.assertTrue(callable(get_elk_logger))

    def test_get_logger_returns_logger(self):
        from spark.elk_logger import get_elk_logger
        lg = get_elk_logger("test_unit")
        self.assertIsNotNone(lg)
        self.assertTrue(hasattr(lg, "info"))

    def test_logger_caching(self):
        from spark.elk_logger import get_elk_logger
        lg1 = get_elk_logger("cached_test")
        lg2 = get_elk_logger("cached_test")
        self.assertIs(lg1, lg2)

    def test_push_event_no_crash(self):
        from spark.elk_logger import push_event
        push_event("unit_test", {"key": "value"}, es_host="localhost", es_port=19999)

    def test_ensure_kibana_index_patterns_no_crash(self):
        from spark.elk_logger import ensure_kibana_index_patterns
        ensure_kibana_index_patterns(kibana_url="http://localhost:19999")


# ══════════════════════════════════════════════════════════════════════════════
class TestCostAnalysisPricing(unittest.TestCase):
    """Unit tests for cost calculations (no Spark needed)."""

    def test_pricing_table(self):
        from spark.cost_analysis import PRICING
        for model, rates in PRICING.items():
            self.assertIn("input",  rates)
            self.assertIn("output", rates)
            self.assertGreater(rates["input"],  0)
            self.assertGreater(rates["output"], 0)

    def test_cost_formula(self):
        from spark.cost_analysis import _cost_usd
        self.assertAlmostEqual(_cost_usd(1000, 0.005), 0.005, places=6)

    def test_zero_tokens(self):
        from spark.cost_analysis import _cost_usd
        self.assertEqual(_cost_usd(0, 0.005), 0.0)

    def test_unknown_model_raises(self):
        from spark.cost_analysis import run_cost_analysis
        with self.assertRaises(ValueError):
            run_cost_analysis(model="unknown-model-xyz")


# ══════════════════════════════════════════════════════════════════════════════
class TestOptimizer(unittest.TestCase):
    """Unit tests for PromptOptimizer."""

    def setUp(self):
        from optimizer import PromptOptimizer
        self.opt = PromptOptimizer()

    def test_removes_fillers(self):
        result = self.opt.optimize("Please could you explain what recursion is?")
        self.assertNotIn("please", result)

    def test_keeps_meaning_words(self):
        result = self.opt.optimize("explain what recursion is")
        self.assertIn("explain", result)
        self.assertIn("what", result)

    def test_empty_input(self):
        self.assertEqual(self.opt.optimize(""), "")

    def test_returns_string(self):
        self.assertIsInstance(self.opt.optimize("hello world"), str)

    def test_non_string_input(self):
        self.assertEqual(self.opt.optimize(None), "")
        self.assertEqual(self.opt.optimize(123),  "")

    def test_tokens_reduced(self):
        original = ("Please could you kindly help me understand how "
                    "neural networks work and give me examples")
        result = self.opt.optimize(original)
        self.assertLessEqual(len(result.split()), len(original.split()))


# ══════════════════════════════════════════════════════════════════════════════
class TestPreprocessUDFs(unittest.TestCase):
    """Unit tests for the pure-Python helper functions in preprocess.py."""

    def setUp(self):
        from spark.preprocess import (
            _optimize_text, _approx_tokens,
            _sentence_count, _avg_word_len, _has_code
        )
        self.opt     = _optimize_text
        self.tokens  = _approx_tokens
        self.sents   = _sentence_count
        self.avgwl   = _avg_word_len
        self.hascode = _has_code

    def test_approx_tokens_basic(self):
        self.assertEqual(self.tokens("hello world"), 2)

    def test_approx_tokens_empty(self):
        self.assertEqual(self.tokens(""), 0)
        self.assertEqual(self.tokens(None), 0)

    def test_sentence_count(self):
        self.assertGreaterEqual(self.sents("Hello. World! How are you?"), 3)

    def test_avg_word_length(self):
        self.assertAlmostEqual(self.avgwl("hi there"), (2 + 5) / 2, places=1)

    def test_has_code_true(self):
        self.assertTrue(self.hascode("```python\nprint('hi')\n```"))

    def test_has_code_false(self):
        self.assertFalse(self.hascode("No code here"))

    def test_optimize_text_reduces_length(self):
        text = "Please could you kindly tell me how machine learning works"
        result = self.opt(text)
        self.assertLessEqual(len(result.split()), len(text.split()))

    def test_optimize_text_non_empty(self):
        self.assertTrue(len(self.opt("explain recursion").strip()) > 0)


# ══════════════════════════════════════════════════════════════════════════════
# Spark Integration Tests – each runs Spark in a SUBPROCESS to avoid
# JVM re-init failures when multiple SparkSessions start/stop in one process.
# ══════════════════════════════════════════════════════════════════════════════

class TestSparkPreprocess(unittest.TestCase):
    """Integration test: preprocessing with synthetic data via PySpark."""

    @classmethod
    def setUpClass(cls):
        cls.tmpdir   = tempfile.mkdtemp()
        cls.raw_json = os.path.join(cls.tmpdir, "raw.json")
        cls.output   = os.path.join(cls.tmpdir, "processed")
        cls.sample   = os.path.join(cls.tmpdir, "sample")
        cls.stats    = os.path.join(cls.tmpdir, "stats.json")
        _write_synthetic(cls.raw_json)
        cls.preprocess_ok = False

        rc = _run_script(_preprocess_script(
            ROOT, cls.raw_json, cls.output, cls.sample, cls.stats))
        cls.preprocess_ok = (rc == 0)
        if rc != 0:
            print(f"\n[setUpClass TestSparkPreprocess] exit {rc}")

    def test_preprocessing_runs(self):
        self.assertTrue(self.preprocess_ok, "Preprocessing subprocess failed")

    def test_stats_file_created(self):
        if not self.preprocess_ok:
            self.skipTest("preprocessing failed")
        self.assertTrue(os.path.exists(self.stats))
        with open(self.stats) as f:
            s = json.load(f)
        self.assertIn("total_turns", s)
        self.assertGreater(s["total_turns"], 0)

    def test_parquet_written(self):
        if not self.preprocess_ok:
            self.skipTest("preprocessing failed")
        self.assertTrue(os.path.isdir(self.output))
        files = [f for f in os.listdir(self.output) if f.endswith(".parquet")]
        self.assertGreater(len(files), 0)

    def test_avg_savings_positive(self):
        if not self.preprocess_ok:
            self.skipTest("preprocessing failed")
        with open(self.stats) as f:
            s = json.load(f)
        self.assertGreaterEqual(s.get("avg_savings_pct", 0), 0)


class TestSparkTraining(unittest.TestCase):
    """Integration test: model training with synthetic processed Parquet."""

    @classmethod
    def setUpClass(cls):
        cls.tmpdir  = tempfile.mkdtemp()
        raw_json    = os.path.join(cls.tmpdir, "raw.json")
        cls.parquet = os.path.join(cls.tmpdir, "processed")
        sample      = os.path.join(cls.tmpdir, "sample")
        stats       = os.path.join(cls.tmpdir, "stats.json")
        cls.models  = os.path.join(cls.tmpdir, "models")
        cls.eval    = os.path.join(cls.tmpdir, "eval.json")
        _write_synthetic(raw_json)
        cls.train_ok = False

        # Step 1: preprocess
        if _run_script(_preprocess_script(ROOT, raw_json, cls.parquet, sample, stats)) != 0:
            print("\n[setUpClass TestSparkTraining] preprocess failed")
            return

        # Step 2: train
        train_cmd = [
            f"import sys",
            f"sys.path.insert(0, {repr(ROOT)})",
            f"from spark.train_model import train",
            f"train(parquet_path={repr(cls.parquet)}, "
            f"models_dir={repr(cls.models)}, "
            f"eval_path={repr(cls.eval)}, "
            f"cv_folds=2)",
        ]
        rc = _run_script(train_cmd)
        cls.train_ok = (rc == 0)
        if rc != 0:
            print(f"\n[setUpClass TestSparkTraining] train exit {rc}")

    def test_training_runs(self):
        self.assertTrue(self.train_ok, "Training subprocess failed")

    def test_eval_file_created(self):
        if not self.train_ok:
            self.skipTest("training failed")
        self.assertTrue(os.path.exists(self.eval))
        with open(self.eval) as f:
            ev = json.load(f)
        self.assertIn("best_model", ev)

    def test_r2_is_numeric(self):
        if not self.train_ok:
            self.skipTest("training failed")
        with open(self.eval) as f:
            ev = json.load(f)
        best = ev.get("best_model")
        r2   = ev.get("models", {}).get(best, {}).get("r2", None)
        self.assertIsNotNone(r2)
        self.assertIsInstance(r2, float)

    def test_ridge_model_saved(self):
        if not self.train_ok:
            self.skipTest("training failed")
        self.assertTrue(os.path.isdir(os.path.join(self.models, "ridge_token_count")))


class TestSparkCostAnalysis(unittest.TestCase):
    """Integration test: cost analysis on synthetic Parquet."""

    @classmethod
    def setUpClass(cls):
        cls.tmpdir  = tempfile.mkdtemp()
        raw_json    = os.path.join(cls.tmpdir, "raw.json")
        cls.parquet = os.path.join(cls.tmpdir, "processed")
        sample      = os.path.join(cls.tmpdir, "sample")
        stats       = os.path.join(cls.tmpdir, "stats.json")
        cls.report  = os.path.join(cls.tmpdir, "cost_report.json")
        cls.plots   = os.path.join(cls.tmpdir, "plots")
        _write_synthetic(raw_json)
        cls.cost_ok = False

        if _run_script(_preprocess_script(ROOT, raw_json, cls.parquet, sample, stats)) != 0:
            print("\n[setUpClass TestSparkCostAnalysis] preprocess failed")
            return

        cost_cmd = [
            f"import sys",
            f"sys.path.insert(0, {repr(ROOT)})",
            f"from spark.cost_analysis import run_cost_analysis",
            f"run_cost_analysis("
            f"parquet_path={repr(cls.parquet)}, "
            f"model='gpt-4o', "
            f"cost_report_path={repr(cls.report)}, "
            f"plot_dir={repr(cls.plots)}, "
            f"push_to_elk=False)",
        ]
        rc = _run_script(cost_cmd)
        cls.cost_ok = (rc == 0)
        if rc != 0:
            print(f"\n[setUpClass TestSparkCostAnalysis] cost exit {rc}")

    def test_cost_analysis_runs(self):
        self.assertTrue(self.cost_ok, "Cost analysis subprocess failed")

    def test_report_file_created(self):
        if not self.cost_ok:
            self.skipTest("cost analysis failed")
        self.assertTrue(os.path.exists(self.report))
        with open(self.report) as f:
            r = json.load(f)
        self.assertIn("totals", r)
        self.assertIn("all_model_comparison", r)

    def test_savings_non_negative(self):
        if not self.cost_ok:
            self.skipTest("cost analysis failed")
        with open(self.report) as f:
            r = json.load(f)
        self.assertGreaterEqual(r["totals"].get("total_cost_saved_usd", 0), 0)

    def test_all_models_compared(self):
        if not self.cost_ok:
            self.skipTest("cost analysis failed")
        with open(self.report) as f:
            r = json.load(f)
        self.assertGreaterEqual(len(r["all_model_comparison"]), 4)

    def test_plots_generated(self):
        if not self.cost_ok:
            self.skipTest("cost analysis failed")
        plots = [f for f in os.listdir(self.plots) if f.endswith(".png")]
        self.assertGreater(len(plots), 0)


# ══════════════════════════════════════════════════════════════════════════════
class TestKibanaDashboards(unittest.TestCase):
    """Unit tests for Kibana dashboard helpers (no live Kibana needed)."""

    def test_wait_for_kibana_false(self):
        from spark.kibana_dashboards import wait_for_kibana
        self.assertFalse(wait_for_kibana("http://localhost:19999", retries=1))

    def test_create_data_view_no_crash(self):
        from spark.kibana_dashboards import create_data_view
        self.assertIsNone(create_data_view("test-*", kibana_url="http://localhost:19999"))

    def test_create_dashboard_no_crash(self):
        from spark.kibana_dashboards import create_dashboard
        self.assertIsNone(create_dashboard("Test", [], kibana_url="http://localhost:19999"))


# ══════════════════════════════════════════════════════════════════════════════
# Runner
# ══════════════════════════════════════════════════════════════════════════════
def _run_quick():
    """Run only non-Spark tests."""
    loader = unittest.TestLoader()
    suite  = unittest.TestSuite()
    for cls in [TestELKLogger, TestCostAnalysisPricing,
                TestOptimizer, TestPreprocessUDFs,
                TestKibanaDashboards]:
        suite.addTests(loader.loadTestsFromTestCase(cls))
    return suite


def _run_all():
    return unittest.TestLoader().loadTestsFromModule(sys.modules[__name__])


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--quick", action="store_true",
                    help="Skip slow Spark integration tests")
    ap.add_argument("-v", "--verbose", action="store_true")
    args, _ = ap.parse_known_args()
    verbosity = 2 if args.verbose else 1
    suite = _run_quick() if args.quick else _run_all()
    runner = unittest.TextTestRunner(verbosity=verbosity)
    result = runner.run(suite)
    sys.exit(0 if result.wasSuccessful() else 1)
