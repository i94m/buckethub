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

from accept import cli, crosscheck, shard, synth, tokens

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

    def test_stop_at_file_boundary_keeps_window_group_atomic(self):
        """锁定口径：停在**源文件边界**。组内的记录要么整组在、要么整组不在；
        过线后把当前源文件剩余记录处理完即停，后续源文件一条都不碰。"""
        manifest = self._run()
        entries = [
            json.loads(line)
            for line in (self.out / "emit.journal").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        cumulative = 0
        before_group = None
        group = None
        for entry in entries:
            if entry.get("lines", 1) > 1 and group is None:
                group = entry
                before_group = cumulative
            cumulative += entry["tokens"]
        self.assertIsNotNone(group, "fixture 里应当有切窗记录")
        group_key = group["key"]

        def _group_lines(out: Path, man: dict) -> list[dict]:
            return [
                item for item in self._lines(out, man)
                if "_window" in item
                and f'{item["_window"]["source"]["key"]}#{item["_window"]["source"]["off"]}' == group["id"]
            ]

        def _assert_atomic(windowed: list[dict]) -> None:
            groups: dict[str, list[int]] = {}
            for item in windowed:
                src = item["_window"]["source"]
                groups.setdefault(f'{src["key"]}#{src["off"]}', []).append(item["_window"]["index"])
            for key, idxs in groups.items():
                self.assertEqual(sorted(idxs), list(range(len(idxs))),
                                 f"{key} 的窗口不完整（整组原子被破坏）")

        # (a) 目标恰在组前：文件边界停 → 该文件整份交付，组要么全在要么全不在
        boundary_out = self.root / "out_boundary"
        man = self._run(target=before_group, out=boundary_out)
        lines = self._lines(boundary_out, man)
        _assert_atomic([item for item in lines if "_window" in item])
        present = _group_lines(boundary_out, man)
        self.assertIn(len(present), (0, group["lines"]), "窗口组只落了一部分")
        shard_keys = [s["source_key"] for s in man["shards"] if s.get("source_key")]
        if man.get("stop_file"):
            self.assertTrue(all(k <= man["stop_file"] for k in shard_keys),
                            "stop_file 之后的源文件不允许出现在交付里")
            if group_key <= man["stop_file"]:
                self.assertEqual(len(present), group["lines"],
                                 "组所在文件已经交付完，整组必须在")
            else:
                self.assertEqual(present, [], "组所在文件被停在后面，不应交付任何窗口")
        else:
            self.assertEqual(man["STOP_REASON"], "corpus_exhausted")
            self.assertEqual(man["not_delivered"], 0)
        self.assertEqual(
            man["not_delivered"], man["kept_records"] - man["delivered_records"])

        # (b) 组内过线：文件边界停 → 整组必须在，超出量不超过"停止文件自身的产出"
        inside_out = self.root / "out_inside"
        man_inside = self._run(target=before_group + 1, out=inside_out)
        present = _group_lines(inside_out, man_inside)
        self.assertEqual(len(present), group["lines"],
                         "过线落在组内时整组必须一起落盘")
        self.assertEqual(sorted(item["_window"]["index"] for item in present),
                         list(range(group["lines"])))
        self.assertEqual(man_inside["delivered_tokens"] >= before_group + 1, True)
        self.assertEqual(
            man_inside["not_delivered"],
            man_inside["kept_records"] - man_inside["delivered_records"])
        stop_tokens = sum(s["tokens"] for s in man_inside["shards"]
                          if s.get("source_key") == man_inside.get("stop_file"))
        if man_inside.get("stop_file") and stop_tokens:
            overshoot = man_inside["delivered_tokens"] - (before_group + 1)
            self.assertLessEqual(
                overshoot, stop_tokens,
                "文件边界停的超出量不应超过停止文件自身的产出")


if __name__ == "__main__":
    unittest.main()


# ---------------------------------------------------------------- 多源 + 目标停
# 2026-11-03 P1-1 回归：逐文件账本必须闭合。
# 修前形态：停止点之后的文件 delivered=0、kept 未记 → accept 判「该文件去重后应保留 N 条，
# 但交付条数记的是 0——交付计数没有回写账本」。修后：kept_records / not_delivered / status
# 三个字段补齐，verify 直接绿。


class MultiSourceStopLedgerTest(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.raw = self.root / "raw"
        self.raw.mkdir()
        # 3 个源文件，每个 2 条 100 轮记录（互不构成 session/前缀关系）
        for index in range(3):
            path = self.raw / f"gpt-gate-{index:04d}.jsonl"
            with open(path, "w", encoding="utf-8") as handle:
                for j in range(2):
                    record = synth.record(
                        session_uuid=f"gate-{index}-{j}",
                        messages=synth.dialog(100, prefix=f"G{index}-{j}"),
                        user_hash=f"u{index}-{j}",
                        rs_id=f"rs_{index}{j:03d}",
                    )
                    handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        self.out = self.root / "out"

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def _run(self, *extra: str):
        cmd = [
            sys.executable, str(PIPELINE), "--execute",
            "--local-dir", str(self.raw), "--out-dir", str(self.out),
            # 关掉 index 闸门，保证三个文件都被索引（真实运行里闸门会留下未交付文件，
            # 这里要复现的正是"已索引但没交付"的账本形态）
            "--tokens-per-byte", "0",
            *extra,
        ]
        proc = subprocess.run(cmd, capture_output=True, text=True)
        self.assertEqual(proc.returncode, 0, msg=proc.stdout + proc.stderr)
        return json.loads((self.out / "manifest.json").read_text(encoding="utf-8"))

    def test_ledger_fields_are_complete(self):
        manifest = self._run("--target-tokens", "1")
        self.assertEqual(manifest["STOP_REASON"], "token_target_reached")
        self.assertEqual(manifest["stop_file"], "gpt-gate-0000.jsonl")

        payload = json.loads((self.out / "ledger.json").read_text(encoding="utf-8"))
        files = payload["files"]
        self.assertEqual(len(files), 3)
        for key, row in files.items():
            self.assertIn("kept_records", row, f"{key} 缺 kept_records")
            self.assertIn("not_delivered", row, f"{key} 缺 not_delivered")
            self.assertIn("status", row, f"{key} 缺 status")
            self.assertEqual(
                row["kept_records"],
                row["passed"] - row["session_removed"] - row["prefix_removed"],
                f"{key} 的 kept_records 与 passed−移除量 不一致",
            )
            self.assertEqual(row["not_delivered"], row["kept_records"] - row["delivered_records"])
            self.assertEqual(
                row["scanned"],
                row["removed_total"] if "removed_total" in row else sum(
                    row[k] for k in ("parse_error", "no_session_id", "no_rs_id",
                                     "no_encrypted_content", "too_short")
                    if k in row
                ) + row["session_removed"] + row["prefix_removed"]
                + row["delivered_records"] + row["not_delivered"],
                f"{key} 账本不闭合",
            )
            # accept 把 stop_cut 当 not_delivered 的别名，写了就双重计数
            self.assertNotIn("stop_cut", row)
            self.assertNotIn("stop_cut", row.get("removed", {}))

        statuses = sorted(row["status"] for row in files.values())
        self.assertEqual(statuses, ["delivered", "not_reached", "not_reached"])

    def test_verify_is_green_with_multi_source_stop(self):
        self._run("--target-tokens", "1")
        report = cli.run_verify(
            self.out,
            manifest_path=None,
            counter=cli._Counter(),
            cfg=shard.ShardConfig(glob="*.jsonl"),
        )
        errors = [p for p in report["problems"] if p["level"] == "error"]
        self.assertEqual(errors, [], f"verify 不该报错：{errors}")
        self.assertTrue(report["ok"])


class CrosscheckGlobTest(unittest.TestCase):
    """P1-2：crosscheck 可显式指定 glob，默认值保持 deliver-*.jsonl 不放宽。"""

    def setUp(self) -> None:
        self._tmp = TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.raw = self.root / "raw"
        self.raw.mkdir()
        for j in range(2):
            record = synth.record(
                session_uuid=f"cc-{j}",
                messages=synth.dialog(100, prefix=f"CC{j}"),
                user_hash=f"cc{j}",
                rs_id=f"rs_cc{j}",
            )
            with open(self.raw / "gpt-cc-0001.jsonl", "a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
        self.out = self.root / "out"
        proc = subprocess.run(
            [sys.executable, str(PIPELINE), "--execute", "--local-dir", str(self.raw),
             "--out-dir", str(self.out), "--target-tokens", "1000000000"],
            capture_output=True, text=True,
        )
        self.assertEqual(proc.returncode, 0, msg=proc.stdout + proc.stderr)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_explicit_glob_matches_source_named_delivery(self):
        report = crosscheck.compare(self.raw, self.out, shard_glob="gpt-*.jsonl")
        self.assertTrue(report["kept"]["digests_match"], report["kept"])
        self.assertEqual(report["tokens"]["delta"], 0)
        self.assertEqual(report["per_file_mismatches"], [])

    def test_default_glob_is_unchanged(self):
        # 默认 glob 只认 deliver-*.jsonl；按源文件命名时应当"什么也没读到"而不是误吸
        report = crosscheck.compare(self.raw, self.out)
        self.assertEqual(report["delivery_lines"], 0)
        self.assertFalse(report["kept"]["digests_match"])

    def test_cli_flag_is_wired(self):
        code = crosscheck.main([
            "--raw-dir", str(self.raw), "--delivery", str(self.out),
            "--shard-glob", "gpt-*.jsonl",
        ])
        self.assertEqual(code, 0)
