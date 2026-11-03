"""联调回归：直接跑开发侧 `pipeline/corpus_pipeline.py`，验收侧独立断言。

开发侧分支合并进当前分支后自动生效；没合并时整组 skip（不会假装通过）。
也可以用环境变量指定路径：

    CORPUS_PIPELINE=/path/to/corpus_pipeline.py python -m unittest ...
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from accept import synth, tokens

REPO_ROOT = Path(__file__).resolve().parents[2]
PIPELINE = Path(
    os.environ.get("CORPUS_PIPELINE", REPO_ROOT / "pipeline" / "corpus_pipeline.py")
)
TOKENIZER_READY = tokens.tokenizer_info()["available"]
SHARD_BYTES = 2_000_000


def _make_corpus(root: Path) -> None:
    """小语料：几条正常记录 + 一条必然切窗的超长记录 + 各种过滤样本。"""
    records = synth.build_source_corpus(group_size=2)
    half = max(1, len(records) // 2)
    for index, chunk in enumerate((records[:half], records[half:])):
        with open(root / f"synthetic-{index:04d}.jsonl", "w", encoding="utf-8") as handle:
            for record in chunk:
                handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")


@unittest.skipUnless(PIPELINE.exists(), f"开发侧流水线不在本分支：{PIPELINE}")
@unittest.skipUnless(TOKENIZER_READY, "需要 tiktoken/o200k_base")
class DevPipelineClosureTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.raw = self.root / "raw"
        self.raw.mkdir()
        _make_corpus(self.raw)
        self.out = self.root / "out"

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _run(self, target: int = 10**9, out: Path | None = None, extra: list[str] | None = None):
        out = out or self.out
        cmd = [
            sys.executable,
            str(PIPELINE),
            "--execute",
            "--local-dir",
            str(self.raw),
            "--out-dir",
            str(out),
            "--target-tokens",
            str(target),
            "--shard-bytes",
            str(SHARD_BYTES),
            *(extra or []),
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, msg=proc.stdout + proc.stderr)
        return json.loads((out / "manifest.json").read_text(encoding="utf-8"))

    def _lines(self, out: Path, manifest: dict) -> list[dict]:
        lines: list[dict] = []
        for shard in manifest["shards"]:
            lines += [
                json.loads(line)
                for line in (out / shard["file"]).read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
        return lines

    # ---------------------------------------------------------------- ① 切窗位置

    def test_window_written_into_request_messages(self):
        manifest = self._run()
        lines = self._lines(self.out, manifest)
        windowed = [item for item in lines if "_window" in item]
        self.assertTrue(windowed, "fixture 里应当有切窗记录")

        for item in windowed:
            self.assertNotIn("messages", item, "窗口内容不允许写在顶层 messages")
            self.assertIn("request", item)
            messages = item["request"]["messages"]
            self.assertIsInstance(messages, list)
            self.assertLessEqual(len(messages), item["_window"]["messages_total"])
            self.assertLessEqual(
                tokens.count_messages(messages),
                128_000,
                msg="按 request.messages 复算 token 必须 ≤ 128k",
            )

        # 同一源记录的窗口按 index 拼接，应还原原始 messages
        groups: dict[str, dict[int, list]] = {}
        for item in windowed:
            source = item["_window"]["source"]
            key = f"{source['key']}#{source['off']}"
            groups.setdefault(key, {})[item["_window"]["index"]] = item["request"]["messages"]
        self.assertTrue(groups)
        for key, windows in groups.items():
            self.assertEqual(sorted(windows), list(range(len(windows))), msg=f"{key} 窗口下标不连续")
            joined: list = []
            for index in sorted(windows):
                joined.extend(windows[index])
            # 与原始 jsonl 里同一条记录逐成员比对
            original = self._source_messages(key)
            self.assertEqual(
                [tokens.compact_json(m) for m in joined],
                [tokens.compact_json(m) for m in original],
                msg=f"{key} 的窗口拼接结果与原始 messages 不一致",
            )

    def _source_messages(self, key: str) -> list:
        name, offset = key.rsplit("#", 1)
        offset = int(offset)
        with open(self.raw / name, "rb") as handle:
            handle.seek(offset)
            record = json.loads(handle.readline())
        return record["request"]["messages"]

    # ---------------------------------------------------------------- ② 逐文件对账

    def test_per_file_delivered_counts(self):
        manifest = self._run()
        ledger = json.loads((self.out / "ledger.json").read_text(encoding="utf-8"))
        per_file_sum = sum(row["delivered_records"] for row in ledger["files"].values())
        self.assertGreater(per_file_sum, 0, "首次运行逐文件交付条数不能是 0")
        self.assertEqual(per_file_sum, manifest["delivered_records"])
        self.assertEqual(
            sum(row["delivered_windows"] for row in ledger["files"].values()),
            manifest["delivered_samples"],
        )

    # ---------------------------------------------------------------- ③ 幂等

    def test_three_runs_are_idempotent(self):
        snapshots = []
        for _ in range(3):
            self._run()
            ledger = json.loads((self.out / "ledger.json").read_text(encoding="utf-8"))
            snapshots.append(
                {
                    key: (row["session_removed"], row["prefix_removed"], row["exact_dup"])
                    for key, row in ledger["files"].items()
                }
            )
        self.assertEqual(snapshots[0], snapshots[1])
        self.assertEqual(snapshots[1], snapshots[2])

    # ---------------------------------------------------------------- ④ 整组原子

    def test_stop_keeps_window_group_atomic(self):
        manifest = self._run()
        entries = [
            json.loads(line)
            for line in (self.out / "emit.journal").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        cumulative = 0
        before_group = None
        group = None
        max_record_tokens = 0
        for entry in entries:
            max_record_tokens = max(max_record_tokens, entry["tokens"])
            if entry.get("lines", 1) > 1 and group is None:
                group = entry
                before_group = cumulative
            cumulative += entry["tokens"]
        self.assertIsNotNone(group, "fixture 里应当有切窗记录")

        # (a) 组前边界：整组都不在
        boundary_out = self.root / "out_boundary"
        manifest_boundary = self._run(target=before_group, out=boundary_out)
        lines = self._lines(boundary_out, manifest_boundary)
        self.assertFalse(
            [item for item in lines if "_window" in item], "目标恰在组前，整组不应交付"
        )
        self.assertEqual(
            manifest_boundary["not_delivered"],
            manifest_boundary["kept_records"] - manifest_boundary["delivered_records"],
        )

        # (b) 组内过线：整组必须在，且停在组上
        inside_out = self.root / "out_inside"
        manifest_inside = self._run(target=before_group + 1, out=inside_out)
        lines = self._lines(inside_out, manifest_inside)
        windowed = [item for item in lines if "_window" in item]
        self.assertEqual(
            len(windowed),
            group["lines"],
            "在窗口组中途过线时，整组必须一起落盘（要么全在，要么全不在）",
        )
        self.assertEqual(sorted(item["_window"]["index"] for item in windowed), list(range(group["lines"])))
        self.assertEqual(manifest_inside["stop_after_record"]["id"], group["id"])
        self.assertEqual(
            manifest_inside["not_delivered"],
            manifest_inside["kept_records"] - manifest_inside["delivered_records"],
        )
        overshoot = manifest_inside["delivered_tokens"] - (before_group + 1)
        self.assertGreater(overshoot, 0)
        self.assertLessEqual(
            overshoot,
            max_record_tokens,
            "整组原子导致的最大超出量不应超过单条记录自身的 token",
        )


if __name__ == "__main__":
    unittest.main()
