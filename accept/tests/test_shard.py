"""分片完整性单测：逐行可解析、末尾完整、体积、单条 token、切窗、命名、重复。"""

from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from accept import cli, reference, shard, synth


def _record(index: int, turns: int = 100, *, rs_id: str | None = None) -> dict:
    return synth.record(
        session_uuid=f"shard-{index}",
        messages=synth.dialog(turns, prefix=f"R{index}"),
        user_hash=f"u{index}",
        rs_id=rs_id if rs_id is not None else f"rs_{index:04d}",
    )


def _write(directory: Path, name: str, records: list[dict], *, newline: bool = True) -> Path:
    path = directory / name
    payload = "".join(
        json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n" for record in records
    )
    if not newline and payload.endswith("\n"):
        payload = payload[:-1]
    path.write_bytes(payload.encode("utf-8"))
    return path


class _Base(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = TemporaryDirectory()
        self.dir = Path(self._tmp.name)
        self.counter = cli._Counter(fake=True, allow_fake=True)
        # 占位分词器算的是字符数，阈值放宽，避免默认 128k 误伤
        self.cfg = shard.ShardConfig(max_record_tokens=10_000_000)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _errors(self, report) -> list[str]:
        return [f"{p.where}: {p.message}" for p in report.all_problems() if p.level == "error"]


class CleanDeliveryTest(_Base):
    def test_reference_delivery_is_clean(self):
        paths = synth.write_corpus(self.dir / "raw")
        reference.run_reference(
            paths,
            self.dir / "delivery",
            counter=self.counter,
            cfg=reference.ReferenceConfig(max_record_tokens=10_000_000),
            tokenizer_meta=self.counter.meta(),
        )
        report = shard.check_delivery(self.dir / "delivery", self.cfg, counter=self.counter)
        self.assertEqual(self._errors(report), [])
        self.assertTrue(report.ok)
        self.assertGreater(report.records, 0)
        self.assertGreater(report.tokens, 0)

    def test_empty_directory_is_error(self):
        (self.dir / "empty").mkdir()
        report = shard.check_delivery(self.dir / "empty", self.cfg)
        self.assertTrue(any("没有匹配" in m for m in self._errors(report)))


class LineIntegrityTest(_Base):
    def test_missing_trailing_newline(self):
        _write(self.dir, "deliver-0001.jsonl", [_record(1)], newline=False)
        report = shard.check_delivery(self.dir, self.cfg, check_prefix_invariant=False)
        self.assertTrue(any("末尾行" in m for m in self._errors(report)))

    def test_broken_json_line(self):
        path = _write(self.dir, "deliver-0001.jsonl", [_record(1)])
        with open(path, "ab") as handle:
            handle.write(b'{"request": {"messages": []}\n')
        report = shard.check_delivery(self.dir, self.cfg, check_prefix_invariant=False)
        self.assertTrue(any("json.loads" in m for m in self._errors(report)))

    def test_blank_line_counted_not_fatal(self):
        path = _write(self.dir, "deliver-0001.jsonl", [_record(1)])
        with open(path, "ab") as handle:
            handle.write(b"\n")
        report = shard.check_delivery(self.dir, self.cfg, check_prefix_invariant=False)
        self.assertEqual(report.shards[0].empty_lines, 1)
        self.assertEqual(self._errors(report), [])


class ShardSizeTest(_Base):
    def test_oversize_shard(self):
        _write(self.dir, "deliver-0001.jsonl", [_record(1, 120)])
        cfg = shard.ShardConfig(shard_max_bytes=1024, shard_slack=0.0)
        report = shard.check_delivery(self.dir, cfg, check_prefix_invariant=False)
        self.assertTrue(any("超过阈值" in m for m in self._errors(report)))

    def test_within_slack_is_tolerated(self):
        path = _write(self.dir, "deliver-0001.jsonl", [_record(1, 120)])
        cfg = shard.ShardConfig(shard_max_bytes=path.stat().st_size - 1, shard_slack=0.5)
        report = shard.check_delivery(self.dir, cfg, check_prefix_invariant=False)
        self.assertEqual(self._errors(report), [])


class RecordContentTest(_Base):
    def test_short_messages_rejected(self):
        _write(self.dir, "deliver-0001.jsonl", [_record(1, 42)])
        report = shard.check_delivery(self.dir, self.cfg, check_prefix_invariant=False)
        self.assertTrue(any("少于锁定口径" in m for m in self._errors(report)))

    def test_windowed_record_may_have_fewer_messages(self):
        record = _record(1, 42)
        record["window"] = {"index": 0, "count": 1, "source_fingerprint": "abc"}
        _write(self.dir, "deliver-0001.jsonl", [record])
        report = shard.check_delivery(self.dir, self.cfg, check_prefix_invariant=False)
        self.assertEqual(self._errors(report), [])

    def test_missing_rs_id_rejected(self):
        record = _record(1)
        record["response"]["response_data"]["output"] = []
        _write(self.dir, "deliver-0001.jsonl", [record])
        report = shard.check_delivery(self.dir, self.cfg, check_prefix_invariant=False)
        self.assertTrue(any("rs_ 前缀" in m for m in self._errors(report)))

    def test_missing_encrypted_part_rejected(self):
        record = _record(1)
        for message in record["request"]["messages"]:
            message["content"] = [
                part for part in message["content"] if part.get("type") != "encrypted_content"
            ]
        _write(self.dir, "deliver-0001.jsonl", [record])
        report = shard.check_delivery(self.dir, self.cfg, check_prefix_invariant=False)
        self.assertTrue(any("encrypted_content" in m for m in self._errors(report)))

    def test_empty_encrypted_value_rejected(self):
        record = _record(1)
        for message in record["request"]["messages"]:
            for part in message["content"]:
                if part.get("type") == "encrypted_content":
                    part["encrypted_content"] = "   "
        _write(self.dir, "deliver-0001.jsonl", [record])
        report = shard.check_delivery(self.dir, self.cfg, check_prefix_invariant=False)
        self.assertTrue(any("encrypted_content" in m for m in self._errors(report)))


class TokenLimitTest(_Base):
    def test_record_over_limit_rejected(self):
        # 多条 message 累计超限 = 窗口切得太大（真 bug），文案必须是这一种
        _write(self.dir, "deliver-0001.jsonl", [_record(1, 120)])
        cfg = shard.ShardConfig(max_record_tokens=200)
        report = shard.check_delivery(self.dir, cfg, counter=self.counter, check_prefix_invariant=False)
        errors = self._errors(report)
        self.assertTrue(any("窗口整体超限" in m for m in errors), errors)
        self.assertFalse(any("单条 message 自身超限" in m for m in errors), "不该误判成固有超限")

    def test_windowed_record_over_limit_still_rejected(self):
        record = _record(1, 120)
        record["window"] = {"index": 0, "count": 2, "source_fingerprint": "abc"}
        _write(self.dir, "deliver-0001.jsonl", [record])
        cfg = shard.ShardConfig(max_record_tokens=200)
        report = shard.check_delivery(self.dir, cfg, counter=self.counter, check_prefix_invariant=False)
        self.assertTrue(any("窗口整体超限" in m for m in self._errors(report)))


class IntrinsicOversizeTest(_Base):
    """单条 message 自身超 128k（图片 base64）：切窗无解，只能走显式口径例外。"""

    def _record_with_huge_message(self) -> dict:
        record = _record(1, 100)
        record["request"]["messages"][3]["content"].append(
            {"type": "input_image", "image_url": "data:image/png;base64," + "A" * 5000}
        )
        return record

    def _cfg(self, **kw):
        return shard.ShardConfig(max_record_tokens=2000, **kw)

    def test_default_is_strict(self):
        _write(self.dir, "deliver-0001.jsonl", [self._record_with_huge_message()])
        report = shard.check_delivery(self.dir, self._cfg(), counter=self.counter,
                                      check_prefix_invariant=False)
        errors = self._errors(report)
        self.assertTrue(any("单条 message 自身超限" in m for m in errors), errors)
        self.assertTrue(any("需口径例外" in m for m in errors), errors)
        self.assertFalse(any("窗口整体超限" in m for m in errors), "不该误判成窗口太大")
        self.assertFalse(report.ok)

    def test_whitelist_turns_it_into_warn(self):
        _write(self.dir, "deliver-0001.jsonl", [self._record_with_huge_message()])
        cfg = self._cfg(allow_intrinsic_oversize=True)
        report = shard.check_delivery(self.dir, cfg, counter=self.counter,
                                      check_prefix_invariant=False)
        self.assertTrue(report.ok, [p.message for p in report.all_problems()])
        self.assertEqual(report.shards[0].intrinsic_oversize_records, 1)
        self.assertTrue(any(p.level == "warn" and "已按 --allow-intrinsic-oversize 放行" in p.message
                            for p in report.all_problems()))

    def test_whitelist_does_not_cover_oversized_window(self):
        # 多条 message 累计超限仍然必须报错，白名单不该把真正的窗口过大放过去
        _write(self.dir, "deliver-0001.jsonl", [_record(1, 120)])
        cfg = shard.ShardConfig(max_record_tokens=200, allow_intrinsic_oversize=True)
        report = shard.check_delivery(self.dir, cfg, counter=self.counter,
                                      check_prefix_invariant=False)
        self.assertTrue(any("窗口整体超限" in m for m in self._errors(report)))
        self.assertFalse(report.ok)


class ShardLayoutTest(_Base):
    def test_numbering_gap(self):
        _write(self.dir, "deliver-0001.jsonl", [_record(1)])
        _write(self.dir, "deliver-0003.jsonl", [_record(2)])
        report = shard.check_delivery(self.dir, self.cfg, check_prefix_invariant=False)
        self.assertTrue(any("编号不连续" in m for m in self._errors(report)))

    def test_duplicate_record_across_shards(self):
        record = _record(1)
        _write(self.dir, "deliver-0001.jsonl", [record])
        _write(self.dir, "deliver-0002.jsonl", [record])
        report = shard.check_delivery(self.dir, self.cfg, check_prefix_invariant=False)
        self.assertTrue(any("完全重复" in m for m in self._errors(report)))

    def test_duplicate_rs_id_across_shards(self):
        _write(self.dir, "deliver-0001.jsonl", [_record(1, rs_id="rs_same")])
        _write(self.dir, "deliver-0002.jsonl", [_record(2, rs_id="rs_same")])
        report = shard.check_delivery(self.dir, self.cfg, check_prefix_invariant=False)
        self.assertTrue(any("rs_id rs_same" in m for m in self._errors(report)))

    def test_window_pieces_share_rs_id_and_cover_all_indexes(self):
        pieces = []
        for index in range(2):
            piece = _record(1, 42)
            piece["window"] = {"index": index, "count": 2, "source_fingerprint": "fp1"}
            pieces.append(piece)
        _write(self.dir, "deliver-0001.jsonl", pieces)
        report = shard.check_delivery(self.dir, self.cfg, check_prefix_invariant=False)
        self.assertEqual(self._errors(report), [])

    def test_incomplete_window_coverage(self):
        pieces = []
        for index in (0, 2):
            piece = _record(1, 42)
            piece["window"] = {"index": index, "count": 3, "source_fingerprint": "fp2"}
            pieces.append(piece)
        _write(self.dir, "deliver-0001.jsonl", pieces)
        report = shard.check_delivery(self.dir, self.cfg, check_prefix_invariant=False)
        self.assertTrue(any("切窗覆盖不完整" in m for m in self._errors(report)))


class PrefixInvariantTest(_Base):
    def test_prefix_relation_in_delivery_is_error(self):
        longer = _record(1, 120)
        shorter = json.loads(json.dumps(longer))
        shorter["request"]["messages"] = shorter["request"]["messages"][:60]
        shorter["response"]["response_data"]["output"][0]["id"] = "rs_short"
        _write(self.dir, "deliver-0001.jsonl", [longer, shorter])
        report = shard.check_delivery(self.dir, self.cfg, counter=self.counter)
        self.assertTrue(any("前缀关系" in m for m in self._errors(report)))

    def test_shared_prefix_k_is_not_error(self):
        base = synth.dialog(3, prefix="K")
        left = _record(1, 100)
        right = _record(2, 100)
        left["request"]["messages"][:3] = base
        right["request"]["messages"][:3] = base
        _write(self.dir, "deliver-0001.jsonl", [left, right])
        report = shard.check_delivery(self.dir, self.cfg)
        self.assertEqual(self._errors(report), [])


if __name__ == "__main__":
    unittest.main()
