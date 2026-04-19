from __future__ import annotations

import unittest
from pathlib import Path

from atomic_ability_evaluate.evaluate import (
    METRIC_ORDER,
    evaluate_record,
    evaluate_records,
    load_records_from_path,
)


class EvaluateTests(unittest.TestCase):
    def test_evaluate_record_returns_all_metrics(self) -> None:
        text = (
            "This is a clean paragraph with punctuation. It should look fairly normal.\n\n"
            "第二段内容也比较自然，而且有完整的句子。这里继续补充一些文本。"
        )
        result = evaluate_record(
            {"text": text, "meta": {"source_type": "html"}},
            output_profile="full",
        )
        self.assertEqual(set(result["document_metrics"].keys()), set(METRIC_ORDER))
        self.assertIsInstance(result["final_score"], float)
        self.assertIn(result["final_decision"], {"pass", "reject", "manual_review"})
        self.assertGreaterEqual(len(result["chunk_metrics"]), 1)

    def test_duplicate_noise_is_detected(self) -> None:
        text = "\n".join(["SPAM HEADER"] * 20) + "\n\n" + "\n".join(["内容重复内容重复"] * 20)
        result = evaluate_record(
            {"text": text, "meta": {"source_type": "pdf_text"}},
            output_profile="compact",
        )
        self.assertGreater(result["document_metrics"]["duplication_ratio"], 0.5)
        self.assertIn(result["final_decision"], {"reject", "manual_review"})

    def test_short_text_triggers_manual_review(self) -> None:
        result = evaluate_record({"text": "too short", "meta": {"source_type": "html"}}, output_profile="compact")
        self.assertIsNone(result["final_score"])
        self.assertEqual(result["final_decision"], "manual_review")

    def test_batch_evaluation_round_trip(self) -> None:
        records = [
            {"text": "Sentence one. Sentence two.", "meta": {"source_type": "html"}},
            {"text": "另一段文本。另一句文本。", "meta": {"source_type": "pdf_text"}},
        ]
        result = evaluate_records(records, output_profile="compact")
        self.assertEqual(len(result), 2)
        self.assertNotIn("chunk_metrics", result[0])

    def test_pdf_loading_and_evaluation(self) -> None:
        pdf_path = Path(__file__).resolve().parent / "test_data.pdf"
        records = load_records_from_path(str(pdf_path))
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["meta"]["source_type"], "pdf_text")
        result = evaluate_record(records[0], output_profile="full")
        self.assertIn("dependency_report", result)
        self.assertGreaterEqual(result["normalized_text_length"], 0)


if __name__ == "__main__":
    unittest.main()
