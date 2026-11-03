"""1B 到顶即停的验收单测：整组原子、停止点可追溯、超出目标的上界、未交付闭合。"""

from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from accept import ledger, shard, stopcheck


def _manifest(**overrides) -> ledger.Manifest:
    raw = {
        "STOP_REASON": "token_target_reached",
        "target_tokens": 1000,
        "delivered_tokens": 1500,
        "delivered_samples": 3,
        "kept_records": 5,
        "delivered_records": 4,
        "not_delivered": 1,
        "shards": [{"file": "deliver-0001.jsonl", "samples": 3, "bytes": 10, "tokens": 1500}],
        "ledger": {"scanned": 10, "passed": 5, "session_removed": 0, "prefix_removed": 0},
        "stop_after_record": {
            "id": "a.jsonl#100",
            "key": "a.jsonl",
            "off": 100,
            "n_msgs": 260,
            "windows": 2,
            "tokens": 1200,
            "tokens_total_after": 1500,
        },
    }
    raw.update(overrides)
    return ledger.normalize_manifest(raw)


def _delivery(max_unit: int = 1200, windows: list[int] | None = None) -> shard.DeliveryReport:
    report = shard.DeliveryReport(directory="out")
    report.unit_tokens = {"window:a.jsonl#100": max_unit}
    report.window_index_map = {"a.jsonl#100": windows if windows is not None else [0, 1]}
    return report


def _journal(tmp: Path, last_id: str = "a.jsonl#100", tokens: int = 1200) -> Path:
    path = tmp / "emit.journal"
    path.write_text(
        "\n".join(
            json.dumps(entry)
            for entry in (
                {"id": "a.jsonl#1", "key": "a.jsonl", "shard": 1, "bytes": 10, "tokens": 300, "lines": 1},
                {"id": last_id, "key": "a.jsonl", "shard": 1, "bytes": 10, "tokens": tokens, "lines": 2},
            )
        )
        + "\n",
        encoding="utf-8",
    )
    return tmp


class StopCheckTest(unittest.TestCase):
    def _run(self, manifest, delivery, tmp: Path) -> list[str]:
        return [
            f"{p.where}: {p.message}"
            for p in stopcheck.check_stop(manifest, delivery, tmp)
            if p.level == "error"
        ]

    def test_clean_stop_passes(self):
        with TemporaryDirectory() as tmp:
            _journal(Path(tmp))
            self.assertEqual(self._run(_manifest(), _delivery(), Path(tmp)), [])

    def test_not_a_target_stop_is_ignored(self):
        with TemporaryDirectory() as tmp:
            manifest = _manifest(STOP_REASON="corpus_exhausted")
            self.assertEqual(self._run(manifest, _delivery(), Path(tmp)), [])

    def test_missing_stop_after_record_is_error(self):
        with TemporaryDirectory() as tmp:
            _journal(Path(tmp))
            manifest = _manifest(stop_after_record=None)
            messages = self._run(manifest, _delivery(), Path(tmp))
            self.assertTrue(any("没有 stop_after_record" in m for m in messages))

    def test_stop_point_must_match_journal(self):
        with TemporaryDirectory() as tmp:
            _journal(Path(tmp), last_id="a.jsonl#999")
            messages = self._run(_manifest(), _delivery(), Path(tmp))
            self.assertTrue(any("journal 最后一笔" in m for m in messages))

    def test_tokens_total_after_must_match(self):
        with TemporaryDirectory() as tmp:
            _journal(Path(tmp))
            manifest = _manifest(
                stop_after_record={
                    "id": "a.jsonl#100", "key": "a.jsonl", "off": 100,
                    "windows": 2, "tokens": 1200, "tokens_total_after": 999,
                }
            )
            messages = self._run(manifest, _delivery(), Path(tmp))
            self.assertTrue(any("停止后累计 token" in m for m in messages))

    def test_partial_window_group_at_stop_is_error(self):
        with TemporaryDirectory() as tmp:
            _journal(Path(tmp))
            messages = self._run(_manifest(), _delivery(windows=[0]), Path(tmp))
            self.assertTrue(any("窗口组不完整" in m for m in messages))

    def test_missing_window_group_is_error(self):
        with TemporaryDirectory() as tmp:
            _journal(Path(tmp))
            messages = self._run(_manifest(), _delivery(windows=[]), Path(tmp))
            self.assertTrue(any("一个窗口都没有" in m for m in messages))

    def test_overshoot_must_not_exceed_single_record(self):
        with TemporaryDirectory() as tmp:
            _journal(Path(tmp))
            # 超出目标 500，但交付集里单条最大只有 100 → 不可能由"整组原子"解释
            messages = self._run(_manifest(), _delivery(max_unit=100), Path(tmp))
            self.assertTrue(any("大于交付集里单条记录的最大 token" in m for m in messages))

    def test_overshoot_within_single_record_is_allowed(self):
        with TemporaryDirectory() as tmp:
            _journal(Path(tmp))
            manifest = _manifest(delivered_tokens=1500, stop_after_record={
                "id": "a.jsonl#100", "key": "a.jsonl", "off": 100,
                "windows": 2, "tokens": 1200, "tokens_total_after": 1500,
            })
            self.assertEqual(self._run(manifest, _delivery(max_unit=186022), Path(tmp)), [])

    def test_not_delivered_must_close(self):
        with TemporaryDirectory() as tmp:
            _journal(Path(tmp))
            manifest = _manifest(not_delivered=3)
            messages = self._run(manifest, _delivery(), Path(tmp))
            self.assertTrue(any("not_delivered" in m for m in messages))

    def test_missing_journal_is_warning_only(self):
        with TemporaryDirectory() as tmp:
            problems = stopcheck.check_stop(_manifest(), _delivery(), Path(tmp))
            self.assertEqual([p.message for p in problems if p.level == "error"], [])
            self.assertTrue(any("emit.journal" in p.message for p in problems if p.level == "warn"))


if __name__ == "__main__":
    unittest.main()
