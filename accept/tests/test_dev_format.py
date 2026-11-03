"""开发侧 `corpus_pipeline` 实际输出格式的兼容测试（联调后加）。

这里的数据形态**照抄开发侧真实产物**，不是我方猜测：

* ``manifest.json``：``STOP_REASON`` / ``delivered_samples`` / ``shard.file+samples`` /
  聚合 ``ledger`` / ``config.tokenizer``；
* 逐文件明细在同目录 ``ledger.json`` 的 ``files{}``，字段扁平
  （``parse_error`` / ``no_rs_id`` / ``session_removed`` / ``passed`` …）；
* 切窗标记 ``_window: {index, of, source:{key,off}, messages_total}``，
  窗口内容实际写在顶层 ``messages``。
"""

from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from accept import ledger, shard, synth


def _record(index: int, turns: int = 100, *, prefix: str = "D") -> dict:
    return synth.record(
        session_uuid=f"dev-{index}",
        messages=synth.dialog(turns, prefix=f"{prefix}{index}"),
        user_hash=f"u{index}",
        rs_id=f"rs_dev_{index:04d}",
    )


def _write_shard(directory: Path, name: str, lines: list[dict]) -> None:
    with open(directory / name, "wb") as handle:
        for item in lines:
            handle.write(json.dumps(item, ensure_ascii=False).encode() + b"\n")


def _dev_ledger(files: dict[str, dict]) -> dict:
    keys = (
        "scanned", "parse_error", "no_session_id", "no_rs_id", "no_encrypted_content",
        "too_short", "passed", "session_removed", "prefix_removed", "exact_dup",
        "delivered_records", "delivered_windows",
    )
    totals = {key: sum(row.get(key, 0) for row in files.values()) for key in keys}
    totals["filter_closure_ok"] = True
    return {"files": files, "totals": totals}


def _dev_manifest(**overrides) -> dict:
    base = {
        "generated_at": "2026-11-03T13:52:41Z",
        "STOP_REASON": "corpus_exhausted",
        "target_tokens": 1_000_000_000,
        "delivered_tokens": 1000,
        "delivered_samples": 2,
        "kept_records": 2,
        "delivered_records": 2,
        "not_delivered": 0,
        "shards": [{"file": "deliver-0001.jsonl", "samples": 2, "bytes": 100, "tokens": 1000}],
        "config": {"tokenizer": "o200k_base", "min_messages": 100},
        "ledger": {
            "scanned": 5, "parse_error": 0, "no_session_id": 0, "no_rs_id": 1,
            "no_encrypted_content": 1, "too_short": 1, "passed": 2,
            "session_removed": 0, "prefix_removed": 0, "exact_dup": 0,
            "delivered_records": 2, "delivered_windows": 2, "filter_closure_ok": True,
        },
        "dedup_closure_ok": True,
    }
    base.update(overrides)
    return base


def _dev_files() -> dict[str, dict]:
    return {
        "synthetic-0000.jsonl": {
            "scanned": 5, "parse_error": 0, "no_session_id": 0, "no_rs_id": 1,
            "no_encrypted_content": 1, "too_short": 1, "passed": 2,
            "session_removed": 0, "prefix_removed": 0, "exact_dup": 0,
            "delivered_records": 2, "delivered_windows": 2,
        }
    }


class DevFormatManifestTest(unittest.TestCase):
    def _build(self, tmp: Path, manifest: dict, files: dict) -> Path:
        (tmp / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False), encoding="utf-8")
        (tmp / "ledger.json").write_text(
            json.dumps(_dev_ledger(files), ensure_ascii=False), encoding="utf-8"
        )
        return tmp / "manifest.json"

    def test_reads_per_file_from_ledger_sidecar(self):
        with TemporaryDirectory() as tmp:
            path = self._build(Path(tmp), _dev_manifest(), _dev_files())
            manifest = ledger.load_manifest(path)
            self.assertEqual(len(manifest.files), 1)
            self.assertIn("ledger.json", manifest.files_source)
            self.assertEqual(manifest.files[0].removed["dedupe_session"], 0)
            self.assertEqual(manifest.files[0].removed["filter_rs_id"], 1)
            self.assertEqual(manifest.files[0].removed["filter_encrypted_content"], 1)
            self.assertEqual(manifest.files[0].removed["filter_min_messages"], 1)

    def test_stop_reason_alias(self):
        with TemporaryDirectory() as tmp:
            path = self._build(Path(tmp), _dev_manifest(), _dev_files())
            manifest = ledger.load_manifest(path)
            self.assertEqual(manifest.stop_reason, "source_exhausted")

    def test_tokenizer_from_config(self):
        with TemporaryDirectory() as tmp:
            path = self._build(Path(tmp), _dev_manifest(), _dev_files())
            manifest = ledger.load_manifest(path)
            self.assertEqual(manifest.tokenizer.get("encoding"), "o200k_base")

    def test_closed_dev_ledger_passes(self):
        # 5 = 1(rs) + 1(enc) + 1(short) + 0 + 0 + 2(delivered)
        with TemporaryDirectory() as tmp:
            path = self._build(Path(tmp), _dev_manifest(), _dev_files())
            problems = ledger.reconcile(ledger.load_manifest(path))
            self.assertEqual([p.message for p in problems if p.level == "error"], [])

    def test_zero_delivered_counts_are_flagged(self):
        with TemporaryDirectory() as tmp:
            files = _dev_files()
            files["synthetic-0000.jsonl"]["delivered_records"] = 0
            files["synthetic-0000.jsonl"]["delivered_windows"] = 0
            manifest = _dev_manifest()
            manifest["ledger"]["delivered_records"] = 0
            manifest["ledger"]["delivered_windows"] = 0
            path = self._build(Path(tmp), manifest, files)
            problems = ledger.reconcile(ledger.load_manifest(path))
            messages = [p.message for p in problems if p.level == "error"]
            self.assertTrue(any("交付计数没有回写账本" in m for m in messages))
            self.assertTrue(any("总交付条数" in m for m in messages))

    def test_per_file_gaps_matching_not_delivered_is_warning(self):
        with TemporaryDirectory() as tmp:
            files = _dev_files()
            files["synthetic-0000.jsonl"]["delivered_records"] = 1
            files["synthetic-0000.jsonl"]["delivered_windows"] = 1
            manifest = _dev_manifest(
                delivered_records=1, delivered_samples=1, kept_records=2, not_delivered=1,
                delivered_tokens=500
            )
            manifest["ledger"]["delivered_records"] = 1
            manifest["ledger"]["delivered_windows"] = 1
            manifest["shards"] = [{"file": "deliver-0001.jsonl", "samples": 1, "bytes": 50, "tokens": 500}]
            path = self._build(Path(tmp), manifest, files)
            problems = ledger.reconcile(ledger.load_manifest(path))
            self.assertEqual([p.message for p in problems if p.level == "error"], [])
            self.assertTrue(any("停止后未交付" in p.message for p in problems if p.level == "warn"))


class DevWindowFormatTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = TemporaryDirectory()
        self.dir = Path(self._tmp.name)
        self.counter = shard_tokens_counter()
        self.cfg = shard.ShardConfig()

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _windowed(self, index: int, of: int, messages: list, total: int) -> dict:
        record = _record(1, 100)
        record["messages"] = messages
        record["_window"] = {
            "index": index,
            "of": of,
            "source": {"key": "synthetic-0000.jsonl", "off": 123},
            "messages_total": total,
            "oversize_message": 0,
        }
        return record

    def test_window_content_location_is_error_by_default(self):
        source = _record(1, 100)["request"]["messages"]
        _write_shard(self.dir, "deliver-0001.jsonl", [self._windowed(0, 1, source[:50], 100)])
        report = shard.check_delivery(self.dir, self.cfg, counter=self.counter, check_prefix_invariant=False)
        messages = [p.message for p in report.all_problems() if p.level == "error"]
        self.assertTrue(any("顶层 messages" in m for m in messages))

    def test_window_content_location_can_be_allowed(self):
        source = _record(1, 100)["request"]["messages"]
        cfg = shard.ShardConfig(allow_top_level_window_messages=True)
        _write_shard(self.dir, "deliver-0001.jsonl", [self._windowed(0, 1, source[:50], 50)])
        report = shard.check_delivery(self.dir, cfg, counter=self.counter, check_prefix_invariant=False)
        self.assertTrue(any("顶层 messages" in p.message for p in report.all_problems() if p.level == "warn"))
        self.assertEqual([p.message for p in report.all_problems() if p.level == "error"], [])

    def test_window_coverage_must_cover_messages_total(self):
        source = _record(1, 100)["request"]["messages"]
        cfg = shard.ShardConfig(allow_top_level_window_messages=True)
        lines = [
            self._windowed(0, 2, source[:50], 100),
            self._windowed(1, 2, source[50:90], 100),  # 少了 10 条
        ]
        _write_shard(self.dir, "deliver-0001.jsonl", lines)
        report = shard.check_delivery(self.dir, cfg, counter=self.counter, check_prefix_invariant=False)
        messages = [p.message for p in report.all_problems() if p.level == "error"]
        self.assertTrue(any("messages 合计" in m for m in messages))

    def test_incomplete_window_group_is_error(self):
        """停止时把一条记录的窗口切一半交付：必须报错（下游拿到的是截断样本）。"""
        source = _record(1, 100)["request"]["messages"]
        cfg = shard.ShardConfig(allow_top_level_window_messages=True)
        _write_shard(self.dir, "deliver-0001.jsonl", [self._windowed(0, 2, source[:50], 100)])
        report = shard.check_delivery(self.dir, cfg, counter=self.counter, check_prefix_invariant=False)
        messages = [p.message for p in report.all_problems() if p.level == "error"]
        self.assertTrue(any("切窗覆盖不完整" in m for m in messages))

    def test_window_tokens_counted_from_window_content(self):
        source = _record(1, 100)["request"]["messages"]
        cfg = shard.ShardConfig(allow_top_level_window_messages=True, max_record_tokens=10**9)
        _write_shard(self.dir, "deliver-0001.jsonl", [self._windowed(0, 1, source[:50], 50)])
        report = shard.check_delivery(self.dir, cfg, counter=self.counter, check_prefix_invariant=False)
        self.assertGreater(report.tokens, 0)
        self.assertEqual(
            report.tokens, self.counter.count_messages(source[:50])
        )


def shard_tokens_counter():
    """真 tiktoken 可用就用真的，否则用显式占位的计数器。"""
    from accept import tokens

    info = tokens.tokenizer_info()
    if info["available"]:
        class _Real:
            fake = False

            def count_messages(self, messages):
                return tokens.count_messages(messages)

            def count_text(self, text):
                return tokens.count_text(text)

        return _Real()
    return tokens.CharCounter(allow=True)


if __name__ == "__main__":
    unittest.main()
