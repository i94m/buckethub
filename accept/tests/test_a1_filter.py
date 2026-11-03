"""A1 口径（整条丢弃含图记录）：验收侧对账 / 复算 / crosscheck 的适配。

用户 2026-11-03 拍板 A1：交付纯文本，含图记录整条不要。
开发侧在 index 期按 ``has_image`` 剔除并落账；验收侧必须
① 认得这一步（否则报"认不出的移除步骤"）、② 独立重算时同样剔除
（否则 crosscheck 逐文件对比必然对不上）。
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from accept import cli, crosscheck, ledger, shard, synth

REPO_ROOT = Path(__file__).resolve().parents[2]
PIPELINE = Path(os.environ.get("CORPUS_PIPELINE", REPO_ROOT / "pipeline" / "corpus_pipeline.py"))


def _record(seed: str, *, image: bool = False, image_in: str = "first") -> dict:
    rec = synth.record(
        session_uuid=f"a1-{seed}",
        messages=synth.dialog(100, prefix=seed),
        user_hash=f"u{seed}",
        rs_id=f"rs_{seed}",
    )
    if image:
        msgs = rec["request"]["messages"]
        target = 0 if image_in == "first" else (len(msgs) // 2 if image_in == "middle" else len(msgs) - 1)
        msgs[target]["content"].append(
            {"type": "input_image", "image_url": "data:image/png;base64," + "A" * 2048}
        )
    return rec


class A1LedgerStepTest(unittest.TestCase):
    """步骤归类别名：has_image 必须被认出来，且不落进 unknown_steps。"""

    def test_step_is_recognized(self):
        self.assertEqual(ledger.classify_step("has_image"), "filter_has_image")
        self.assertEqual(ledger.classify_step("filter_has_image"), "filter_has_image")

    def test_filter_segment_closes_with_has_image(self):
        entry = {
            "source_key": "gpt-a1.jsonl",
            "scanned": 10,
            "no_rs_id": 2,
            "too_short": 1,
            "has_image": 3,
            "passed": 4,
            "session_removed": 0,
            "prefix_removed": 0,
            "delivered_records": 4,
            "not_delivered": 0,
        }
        manifest = ledger.normalize_manifest({"files": [entry], "STOP_REASON": "corpus_exhausted"})
        problems = ledger.reconcile(manifest, allow_missing_steps=True)
        errors = [p.message for p in problems if p.level == "error"]
        self.assertEqual(errors, [], errors)
        self.assertEqual(manifest.files[0].removed.get("filter_has_image"), 3)


class A1PipelineAcceptanceTest(unittest.TestCase):
    """真实产出形态：开发侧跑一遍，验收侧 verify + crosscheck 都必须绿。"""

    def setUp(self) -> None:
        self._tmp = TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.raw = self.root / "raw"
        self.raw.mkdir()
        records = [
            _record("clean-a"),
            _record("img-first", image=True),
            _record("clean-b"),
            _record("img-middle", image=True, image_in="middle"),
        ]
        with open(self.raw / "gpt-a1-0001.jsonl", "w", encoding="utf-8") as handle:
            for rec in records:
                handle.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")
        self.out = self.root / "out"
        proc = subprocess.run(
            [sys.executable, str(PIPELINE), "--execute", "--local-dir", str(self.raw),
             "--out-dir", str(self.out), "--target-tokens", "1000000000"],
            capture_output=True, text=True,
        )
        self.assertEqual(proc.returncode, 0, msg=proc.stdout + proc.stderr)

    def tearDown(self) -> None:
        self._tmp.cleanup()

    def test_image_records_are_dropped_and_counted(self) -> None:
        payload = json.loads((self.out / "ledger.json").read_text(encoding="utf-8"))
        row = payload["files"]["gpt-a1-0001.jsonl"]
        self.assertEqual(row["has_image"], 2, "两条含图记录都要记在 has_image 上")
        self.assertEqual(row["scanned"], 4)
        self.assertEqual(row["passed"], 2)
        self.assertEqual(row["kept_records"], 2)
        self.assertEqual(row["delivered_records"], 2)
        self.assertEqual(payload["totals"]["has_image"], 2)

    def test_verify_is_green(self) -> None:
        report = cli.run_verify(
            self.out, manifest_path=None, counter=cli._Counter(),
            cfg=shard.ShardConfig(glob="*.jsonl"),
        )
        errors = [p for p in report["problems"] if p["level"] == "error"]
        self.assertEqual(errors, [], errors)
        self.assertTrue(report["ok"])

    def test_crosscheck_agrees_on_has_image(self) -> None:
        report = crosscheck.compare(self.raw, self.out, shard_glob="gpt-*.jsonl")
        per_file = report["per_file"][0]
        self.assertEqual(per_file["filters"]["filter_has_image"], [2, 2],
                         "两侧都必须独立数出 2 条含图记录")
        self.assertTrue(report["kept"]["digests_match"], report["kept"])
        self.assertEqual(report["per_file_mismatches"], [])

    def test_no_image_record_reaches_delivery(self) -> None:
        manifest = json.loads((self.out / "manifest.json").read_text(encoding="utf-8"))
        for shard_entry in manifest["shards"]:
            for line in (self.out / shard_entry["file"]).read_text(encoding="utf-8").splitlines():
                if not line.strip():
                    continue
                self.assertFalse(shard.has_image_part(json.loads(line)))
