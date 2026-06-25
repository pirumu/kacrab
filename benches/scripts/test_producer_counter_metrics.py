import tempfile
import unittest
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import producer_counter_metrics as metrics


RUN_ONE = """
producer-metrics:record-send-total:{client-id=java-default}: 1000.0
producer-metrics:records-per-request-avg:{client-id=java-default}: 2.5
producer-metrics:request-size-avg:{client-id=java-default}: 30000.0
producer-metrics:record-retry-total:{client-id=java-default}: 2.0
producer-metrics:record-error-total:{client-id=java-default}: 0.0
producer-metrics:batch-split-total:{client-id=java-default}: 1.0
"""

RUN_TWO = """
producer-metrics:record-send-total:{client-id=java-default}: 2000.0
producer-metrics:records-per-request-avg:{client-id=java-default}: 4.0
producer-metrics:request-size-avg:{client-id=java-default}: 32000.0
producer-metrics:record-retry-total:{client-id=java-default}: 0.0
producer-metrics:record-error-total:{client-id=java-default}: 2.0
producer-metrics:batch-split-total:{client-id=java-default}: 3.0
"""


class ProducerCounterMetricsTest(unittest.TestCase):
    def test_make_test_runs_benchmark_script_tests(self):
        makefile = Path(__file__).resolve().parents[2] / "Makefile"
        source = makefile.read_text(encoding="utf-8")

        self.assertIn("test-bench-scripts", source)
        self.assertRegex(source, r"(?m)^test:\s+test-bench-scripts\b")

    def test_makefile_benchmark_script_tests_do_not_write_pycache(self):
        makefile = Path(__file__).resolve().parents[2] / "Makefile"
        source = makefile.read_text(encoding="utf-8")

        self.assertRegex(
            source,
            r"(?m)^\tPYTHONDONTWRITEBYTECODE=1 python3 -m unittest "
            r"benches/scripts/test_producer_counter_metrics\.py$",
        )

    def test_format_java_counter_line_matches_parity_schema(self):
        line = metrics.format_counter_line("java producer counters", RUN_ONE)

        self.assertIn("produce_requests=400", line)
        self.assertIn("record_batches=not_exposed_by_producer_perf", line)
        self.assertIn("records_per_batch_avg=not_exposed_by_producer_perf", line)
        self.assertIn("records_per_request_avg=2.500", line)
        self.assertIn("request_size_avg=30000", line)
        self.assertIn("retries=2", line)
        self.assertIn("errors=0", line)
        self.assertIn("in_flight_stalls=not_exposed_by_producer_perf", line)
        self.assertIn("batch_splits=1", line)
        self.assertIn("request_splits=not_exposed_by_producer_perf", line)

    def test_average_counter_line_weights_request_size_by_requests(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            first = Path(tmpdir) / "run-one.log"
            second = Path(tmpdir) / "run-two.log"
            first.write_text(RUN_ONE, encoding="utf-8")
            second.write_text(RUN_TWO, encoding="utf-8")

            line = metrics.format_average_counter_line([first, second])

        self.assertTrue(line.startswith("java average counters: "))
        self.assertIn("produce_requests=450.000", line)
        self.assertIn("record_batches=not_exposed_by_producer_perf", line)
        self.assertIn("records_per_batch_avg=not_exposed_by_producer_perf", line)
        self.assertIn("records_per_request_avg=3.333", line)
        self.assertIn("request_size_avg=31111.111", line)
        self.assertIn("retries=1.000", line)
        self.assertIn("errors=1.000", line)
        self.assertIn("in_flight_stalls=not_exposed_by_producer_perf", line)
        self.assertIn("batch_splits=2.000", line)
        self.assertIn("request_splits=not_exposed_by_producer_perf", line)


if __name__ == "__main__":
    unittest.main()
