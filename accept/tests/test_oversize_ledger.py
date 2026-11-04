"""裁决 B：not_delivered_oversize_message 是合法未交付，不是移除步骤。

历史 bug：classify_step 把带 "message" 的字段归到 filter_min_messages，
超限丢弃被当成过滤移除，导致「过滤段不闭合」+「对账不闭合」两个 error。
"""
import json
import tempfile
import unittest
from pathlib import Path

from accept import ledger as L


def _manifest():
    return {
        "generated_at": "2026-11-04T00:00:00Z",
        "STOP_REASON": "source_exhausted",
        "shards": [{"file": "gpt-x.jsonl", "source_key": "Gpt/x.jsonl", "part": 1,
                    "samples": 8, "bytes": 1000, "tokens": 900}],
        "delivered_files": [{"source_key": "Gpt/x.jsonl", "status": "delivered",
                             "mode": "single", "parts": 1, "records": 8, "tokens": 900}],
        "delivered_tokens": 900, "delivered_samples": 8, "delivered_records": 8,
        "kept_records": 10, "not_delivered": 2,
        "files": [{
            "source_key": "Gpt/x.jsonl", "scanned": 12, "passed": 10,
            "no_session_id": 0, "no_rs_id": 0, "no_encrypted_content": 0,
            "too_short": 0, "has_image": 0, "parse_error": 2,
            "session_removed": 0, "prefix_removed": 0,
            "kept_records": 10, "delivered_records": 8, "delivered_windows": 8,
            "delivered_tokens": 900, "not_delivered": 2,
            "not_delivered_oversize_message": 1,
        }],
    }


class TestOversizeNotDelivered(unittest.TestCase):
    def test_no_error(self):
        with tempfile.TemporaryDirectory() as td:
            p = Path(td) / "manifest.json"
            p.write_text(json.dumps(_manifest()))
            man = L.load_manifest(p)
            problems = L.reconcile(man, strict_totals=False)
        errors = [(x.where, x.message) for x in problems if x.level == "error"]
        self.assertEqual(errors, [])


if __name__ == "__main__":
    unittest.main()
