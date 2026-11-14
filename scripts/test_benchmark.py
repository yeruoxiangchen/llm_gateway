"""Prevent accidental credit for unparsed responses or transport errors."""
import unittest

from benchmark import extract_answer, numeric_answer, scores


class BenchmarkScoringTests(unittest.TestCase):
    def test_numeric_final_answer(self):
        self.assertEqual(numeric_answer("There are 25 items.\n#### 1,250.00"), "1250")
        self.assertEqual(numeric_answer("#### -0.50"), "-0.5")
        self.assertIsNone(numeric_answer("I considered 25 and 50 but have no final answer."))

    def test_boolean_labels(self):
        self.assertEqual(extract_answer("boolq", " YES\n"), "yes")
        self.assertEqual(extract_answer("boolq", "no"), "no")
        self.assertIsNone(extract_answer("boolq", "not yes"))
        self.assertIsNone(extract_answer("boolq", "yes or no"))

    def test_errors_stay_in_denominator(self):
        items = [{"dataset": "gsm8k"}, {"dataset": "gsm8k"}, {"dataset": "boolq"}]
        rows = [{"dataset": "gsm8k", "arm": "direct", "status": "completed", "correct": True, "format_valid": True},
                {"dataset": "gsm8k", "arm": "direct", "status": "error", "correct": False}]
        result = scores(rows, items)["gsm8k"]["direct"]
        self.assertEqual(result["accuracy_pct"], 50)
        self.assertEqual(result["errors"], 1)
        self.assertEqual(result["total"], 2)


if __name__ == "__main__":
    unittest.main()
