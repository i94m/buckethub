"""对账单测：`原始条数 = 各步移除量之和 + 交付条数`，逐文件逐阶段都要对上。"""

from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from accept import ledger


def _file(
    key: str = "a.jsonl",
    raw: int = 100,
    filters: tuple[int, int, int] = (10, 5, 15),
    dedupe: tuple[int, int] = (20, 10),
    delivered: int = 40,
    tokens: int = 1000,
    status: str = "done",
) -> dict:
    return {
        "source_key": key,
        "raw_lines": raw,
        "removed": {
            "parse_errors": 0,
            "filter_rs_id": filters[0],
            "filter_encrypted_content": filters[1],
            "filter_min_messages": filters[2],
            "dedupe_session": dedupe[0],
            "dedupe_prefix": dedupe[1],
        },
        "delivered_records": delivered,
        "delivered_tokens": tokens,
        "status": status,
    }


def _manifest(files: list[dict] | None = None, **overrides) -> dict:
    files = files if files is not None else [_file()]
    raw = sum(f["raw_lines"] for f in files)
    delivered = sum(f["delivered_records"] for f in files)
    tokens = sum(f["delivered_tokens"] for f in files)
    removed: dict[str, int] = {}
    for item in files:
        for step, value in item["removed"].items():
            removed[step] = removed.get(step, 0) + value
    base = {
        "schema": ledger.MANIFEST_SCHEMA,
        "stop_reason": "source_exhausted",
        "token_target": 0,
        "tokenizer": {"encoding": "o200k_base", "method": "encode_ordinary"},
        "totals": {
            "raw_lines": raw,
            "removed": removed,
            "delivered_records": delivered,
            "delivered_tokens": tokens,
            "window_pieces": sum(f.get("window_pieces", 0) for f in files),
        },
        "files": files,
        "shards": [
            {
                "name": "deliver-0001.jsonl",
                "records": delivered + sum(f.get("window_pieces", 0) for f in files),
                "bytes": 123,
                "tokens": tokens,
            }
        ],
    }
    base.update(overrides)
    return base


def _errors(problems) -> list[str]:
    return [f"{p.where}: {p.message}" for p in problems if p.level == "error"]


class NormalizeTest(unittest.TestCase):
    def test_alias_names_are_recognized(self):
        raw = {
            "files": [
                {
                    "key": "x.jsonl",
                    "input_lines": 10,
                    "dropped": {"parse_failures": 1, "no_rs_id": 2, "session_dup": 3, "prefix_dup": 4},
                    "kept": 0,
                }
            ],
            "totals": {"raw_lines": 10, "delivered_records": 0},
        }
        manifest = ledger.normalize_manifest(raw)
        self.assertEqual(manifest.files[0].source_key, "x.jsonl")
        self.assertEqual(manifest.files[0].raw_lines, 10)
        self.assertEqual(manifest.files[0].removed["parse_errors"], 1)
        self.assertEqual(manifest.files[0].removed["filter_rs_id"], 2)
        self.assertEqual(manifest.files[0].removed["dedupe_session"], 3)
        self.assertEqual(manifest.files[0].removed["dedupe_prefix"], 4)

    def test_nested_count_containers_are_flattened(self):
        raw = _manifest()
        raw["files"][0]["stats"] = {"parse_errors": 0}
        manifest = ledger.normalize_manifest(raw)
        self.assertEqual(manifest.files[0].removed["parse_errors"], 0)

    def test_classify_step(self):
        self.assertEqual(ledger.classify_step("removed_no_encrypted_content"), "filter_encrypted_content")
        self.assertEqual(ledger.classify_step("prefix_dedup"), "dedupe_prefix")
        self.assertEqual(ledger.classify_step("session_dedup"), "dedupe_session")
        self.assertEqual(ledger.classify_step("too_short_messages"), "filter_min_messages")
        self.assertEqual(ledger.classify_step("stop_cut"), "stop_cut")
        self.assertIsNone(ledger.classify_step("mystery"))

    def test_load_manifest_from_disk(self):
        with TemporaryDirectory() as tmp:
            path = Path(tmp) / "manifest.json"
            path.write_text(json.dumps(_manifest()), encoding="utf-8")
            manifest = ledger.load_manifest(path)
            self.assertEqual(manifest.schema, ledger.MANIFEST_SCHEMA)
            self.assertEqual(manifest.files[0].delivered_records, 40)


class ReconcileTest(unittest.TestCase):
    def test_closed_ledger_passes(self):
        # 100 = 10+5+15+20+10+40
        problems = ledger.reconcile(ledger.normalize_manifest(_manifest()))
        self.assertEqual(_errors(problems), [])

    def test_per_file_imbalance_is_error(self):
        files = [_file(raw=100, delivered=41)]
        problems = ledger.reconcile(ledger.normalize_manifest(_manifest(files)))
        self.assertTrue(any("对账不闭合" in m for m in _errors(problems)))

    def test_imbalance_in_one_of_several_files_is_error(self):
        files = [
            _file(key="a.jsonl"),
            _file(key="b.jsonl", raw=50, filters=(5, 5, 5), dedupe=(5, 5), delivered=25),
            _file(key="c.jsonl", raw=200, delivered=141),
        ]
        problems = ledger.reconcile(ledger.normalize_manifest(_manifest(files)))
        messages = _errors(problems)
        self.assertTrue(any("c.jsonl" in m and "对账不闭合" in m for m in messages))
        self.assertFalse(any("b.jsonl" in m and "对账不闭合" in m for m in messages))

    def test_unknown_step_is_error(self):
        files = [_file()]
        files[0]["removed"]["mystery_stage"] = 3
        problems = ledger.reconcile(ledger.normalize_manifest(_manifest(files)))
        self.assertTrue(any("认不出的移除步骤" in m for m in _errors(problems)))

    def test_missing_step_is_error_unless_allowed(self):
        files = [_file()]
        files[0]["removed"].pop("dedupe_prefix")
        raw = _manifest(files)
        raw["files"][0]["delivered_records"] = 50
        raw["totals"]["delivered_records"] = 50
        raw["shards"][0]["records"] = 50
        manifest = ledger.normalize_manifest(raw)
        problems = ledger.reconcile(manifest)
        self.assertTrue(any("缺少移除步骤" in m for m in _errors(problems)))
        problems = ledger.reconcile(manifest, allow_missing_steps=True)
        self.assertEqual(_errors(problems), [])

    def test_totals_must_match_per_file(self):
        raw = _manifest()
        raw["totals"]["delivered_records"] = 999
        problems = ledger.reconcile(ledger.normalize_manifest(raw))
        self.assertTrue(any("总交付条数" in m for m in _errors(problems)))

    def test_shard_records_must_match_delivered(self):
        raw = _manifest()
        raw["shards"][0]["records"] = 41
        problems = ledger.reconcile(ledger.normalize_manifest(raw))
        self.assertTrue(any("分片行数之和" in m for m in _errors(problems)))

    def test_shard_records_account_for_window_pieces(self):
        files = [_file()]
        files[0]["window_pieces"] = 3
        raw = _manifest(files)
        # 40 条源记录切成 43 条分片记录，对账仍应闭合
        raw["shards"][0]["records"] = 43
        problems = ledger.reconcile(ledger.normalize_manifest(raw))
        self.assertEqual(_errors(problems), [])

    def test_shard_token_mismatch_is_error(self):
        raw = _manifest()
        raw["shards"][0]["tokens"] = 999
        problems = ledger.reconcile(ledger.normalize_manifest(raw))
        self.assertTrue(any("分片 token" in m for m in _errors(problems)))

    def test_stop_reason_must_be_known(self):
        raw = _manifest(stop_reason="whatever")
        problems = ledger.reconcile(ledger.normalize_manifest(raw))
        self.assertTrue(any("未知停止原因" in m for m in _errors(problems)))

    def test_token_target_reached_requires_actual_tokens(self):
        raw = _manifest(stop_reason="token_target_reached", token_target=10**9)
        problems = ledger.reconcile(ledger.normalize_manifest(raw))
        self.assertTrue(any("声称到达目标" in m for m in _errors(problems)))

    def test_token_target_reached_ok(self):
        files = [_file(tokens=10**9)]
        raw = _manifest(files, stop_reason="token_target_reached", token_target=10**9)
        problems = ledger.reconcile(ledger.normalize_manifest(raw))
        self.assertEqual(_errors(problems), [])

    def test_stop_must_be_at_last_file_only(self):
        files = [
            _file(key="a.jsonl", status="done_partial"),
            _file(key="b.jsonl", status="done_partial"),
        ]
        raw = _manifest(files, stop_reason="token_target_reached", token_target=1)
        raw["totals"]["delivered_tokens"] = 2000
        problems = ledger.reconcile(ledger.normalize_manifest(raw))
        self.assertTrue(any("中途停止" in m for m in _errors(problems)))


if __name__ == "__main__":
    unittest.main()
