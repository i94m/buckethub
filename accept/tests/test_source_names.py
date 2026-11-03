"""验收侧对「按源文件命名」的交付形态：globs / 文件名排序 / .part-NNNN / stop_file。

2026-11-03 v2 改造的验收侧回归用例。改造前这些用例必然失败：
分片模式写死 ``deliver-*.jsonl``、编号必须从 1 连号、停止点只认 ``stop_after_record``。
"""

from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from accept import cli, ledger, shard, stopcheck, synth


def _record(index: int, turns: int = 100) -> dict:
    return synth.record(
        session_uuid=f"named-{index}",
        messages=synth.dialog(turns, prefix=f"N{index}"),
        user_hash=f"n{index}",
        rs_id=f"rs_{index:04d}",
    )


def _write(directory: Path, name: str, records: list[dict]) -> Path:
    path = directory / name
    path.write_bytes(
        "".join(json.dumps(r, ensure_ascii=False, separators=(",", ":")) + "\n" for r in records).encode()
    )
    return path


class _Base(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = TemporaryDirectory()
        self.dir = Path(self._tmp.name)
        self.counter = cli._Counter(fake=True, allow_fake=True)
        self.cfg = shard.ShardConfig(glob="*.jsonl", max_record_tokens=10_000_000)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _errors(self, report) -> list[str]:
        return [p.message for p in report.all_problems() if p.level == "error"]


class SourceNamedShardTest(_Base):
    def test_source_named_shard_passes(self):
        _write(self.dir, "gpt-6-astra-0009-20261001.jsonl", [_record(1)])
        _write(self.dir, "gpt-6-astra-0010-20261001.jsonl", [_record(2)])
        report = shard.check_delivery(self.dir, self.cfg, counter=self.counter)
        self.assertEqual(self._errors(report), [])
        self.assertEqual(report.records, 2)

    def test_parts_are_discovered_in_order(self):
        _write(self.dir, "big-0001.part-0001.jsonl", [_record(1)])
        _write(self.dir, "big-0001.part-0002.jsonl", [_record(2)])
        report = shard.check_delivery(self.dir, self.cfg, counter=self.counter)
        self.assertEqual(self._errors(report), [])
        self.assertEqual([s.name for s in report.shards],
                         ["big-0001.part-0001.jsonl", "big-0001.part-0002.jsonl"])

    def test_part_numbering_gap_is_error(self):
        _write(self.dir, "big-0001.part-0001.jsonl", [_record(1)])
        _write(self.dir, "big-0001.part-0003.jsonl", [_record(2)])
        report = shard.check_delivery(self.dir, self.cfg, counter=self.counter)
        self.assertTrue(any("分片编号不连续" in m for m in self._errors(report)))

    def test_single_and_parts_conflict_is_error(self):
        _write(self.dir, "big-0001.jsonl", [_record(1)])
        _write(self.dir, "big-0001.part-0001.jsonl", [_record(2)])
        report = shard.check_delivery(self.dir, self.cfg, counter=self.counter)
        self.assertTrue(any("命名冲突" in m for m in self._errors(report)))

    def test_pipeline_metadata_files_are_not_shards(self):
        """同一 out-dir 直接验收时，keep/index/ledger 不能被当成分片。"""
        _write(self.dir, "gpt-6-astra-0009.jsonl", [_record(1)])
        (self.dir / "keep.jsonl").write_text('{"id": "not-a-record"}\n', encoding="utf-8")
        (self.dir / "index.jsonl").write_text('{"id": "x"}\n', encoding="utf-8")
        (self.dir / "ledger.json").write_text("{}", encoding="utf-8")
        report = shard.check_delivery(self.dir, self.cfg, counter=self.counter)
        self.assertEqual(self._errors(report), [])
        self.assertEqual([s.name for s in report.shards], ["gpt-6-astra-0009.jsonl"])

    def test_cli_default_glob_takes_source_names(self):
        parser = cli.build_parser()
        args = parser.parse_args(["verify", "--delivery", str(self.dir)])
        self.assertEqual(args.shard_glob, "*.jsonl")


class StopFileTest(_Base):
    """文件边界停：stop_file 必须是最后一个被交付的源文件，且超出量 ≤ 该文件产出。"""

    def _manifest(self, *, stop_file: str = "", shards=None, target=100, delivered=150):
        raw = {
            "STOP_REASON": "token_target_reached",
            "target_tokens": target,
            "delivered_tokens": delivered,
            "stop_after_record": {"id": "k#0", "windows": 1, "tokens": 50,
                                  "tokens_total_after": delivered},
            "shards": shards if shards is not None else [
                {"file": "a.jsonl", "source_key": "a.jsonl", "records": 1, "tokens": 100},
                {"file": "b.jsonl", "source_key": "b.jsonl", "records": 1, "tokens": 50},
            ],
        }
        if stop_file:
            raw["stop_file"] = stop_file
        return ledger.normalize_manifest(raw)

    def _delivery(self):
        report = shard.DeliveryReport(directory=str(self.dir))
        report.unit_tokens = {"line:a": 50}
        return report

    def test_missing_both_stop_markers_is_error(self):
        man = self._manifest()
        man.stop_after_record = None
        problems = stopcheck.check_stop(man, self._delivery(), self.dir)
        self.assertTrue(any("既没有 stop_file 也没有 stop_after_record" in p.message
                            for p in problems))

    def test_records_after_stop_file_is_error(self):
        man = self._manifest(stop_file="a.jsonl")
        problems = stopcheck.check_stop(man, self._delivery(), self.dir)
        self.assertTrue(any("更后面的源文件" in p.message for p in problems),
                        [p.message for p in problems])

    def test_clean_file_boundary_stop_passes(self):
        man = self._manifest(stop_file="b.jsonl")
        problems = stopcheck.check_stop(man, self._delivery(), self.dir)
        self.assertEqual([p.message for p in problems if p.level == "error"], [])

    def test_overshoot_beyond_stop_file_is_error(self):
        # 目标 100，交付 150（超出 50 = b.jsonl 自身产出，合法）；
        # 超出 200 就超过停止文件的产出，必须报错。
        man = self._manifest(stop_file="b.jsonl", target=1, delivered=400)
        problems = stopcheck.check_stop(man, self._delivery(), self.dir)
        self.assertTrue(any("超出目标" in p.message for p in problems),
                        [p.message for p in problems])
