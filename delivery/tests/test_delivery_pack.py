#!/usr/bin/env python3
"""delivery_pack 的自测：全部本地合成数据，不连网、不需要凭证。

合成交付目录**由 pipeline/corpus_pipeline.py --local-dir 真跑出来**（不是手写 manifest），
这样打包逻辑面对的是和全速跑批完全相同的产物形态。

    python -m unittest discover -s delivery/tests -v
"""
from __future__ import annotations

import json
import shutil
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "delivery"))
sys.path.insert(0, str(ROOT / "pipeline"))

import delivery_pack as dp  # noqa: E402


def _have(mod: str) -> bool:
    try:
        __import__(mod)
        return True
    except ImportError:
        return False


def make_record(i: int, n_msgs: int = 120, image: bool = False, session: str | None = None):
    msgs = []
    for j in range(n_msgs):
        content = [
            {"type": "text", "text": f"turn {j} of record {i} " + "x" * 24},
            {"type": "encrypted_content", "encrypted_content": f"enc-{i}-{j}-" + "y" * 24},
        ]
        if image and j == 60:
            content.append({"type": "input_image", "image_url": "data:image/png;base64,AAAB"})
        msgs.append({"role": "user", "content": content})
    return {
        "session_id": session or f"11111111-2222-3333-4444-{i:012d}~u1",
        "request_id": f"req_{i}",
        "timestamp": "2026-10-01T00:00:00Z",
        "request": {"model": "gpt-6", "messages": msgs, "tools": []},
        "response": {"response_data": {"output": [{"id": f"rs_{i}"}]}},
        "model": "gpt-6",
    }


def build_delivery(tmp: Path, shard_bytes: int = 30000) -> Path:
    """用真 pipeline 造一个多文件、多分片的交付目录。"""
    import corpus_pipeline as cp

    raw = tmp / "raw"
    raw.mkdir(parents=True)
    # 文件 1：3 条可交付 + 1 条含图（A1 应整条丢）
    with open(raw / "gpt-6-astra-0001-20261001.jsonl", "w", encoding="utf-8") as fh:
        for i in range(3):
            fh.write(json.dumps(make_record(100 + i), ensure_ascii=False) + "\n")
        fh.write(json.dumps(make_record(200, image=True), ensure_ascii=False) + "\n")
    # 文件 2：3 条可交付（不同 session）
    with open(raw / "gpt-6-astra-0002-20261001.jsonl", "w", encoding="utf-8") as fh:
        for i in range(3):
            fh.write(json.dumps(make_record(300 + i), ensure_ascii=False) + "\n")

    out = tmp / "out"
    rc = cp.main(["--local-dir", str(raw), "--execute", "--out-dir", str(out),
                  "--target-tokens", "1000000000", "--shard-bytes", str(shard_bytes)])
    assert rc == 0, f"corpus_pipeline 返回 {rc}"
    return out


class DeliveryBase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not _have("tiktoken"):
            raise unittest.SkipTest("tiktoken 未安装，跳过（交付打包依赖真 pipeline 产物）")
        cls._tmp = tempfile.TemporaryDirectory(prefix="delivery_pack_test_")
        cls.tmp = Path(cls._tmp.name)
        cls.out = build_delivery(cls.tmp)
        cls.staging = cls.tmp / "staging"

    @classmethod
    def tearDownClass(cls):
        cls._tmp.cleanup()

    def deliver(self, *flags, sim=None, staging=None, extra=None):
        argv = [str(self.out), "--execute", "--staging-dir", str(staging or self.staging)]
        if sim:
            argv += ["--simulate-local-s3", str(sim)]
        argv += list(flags) + list(extra or [])
        return dp.main(argv)

    def manifest(self, staging=None):
        p = (staging or self.staging) / dp.DELIVERY_MANIFEST
        return json.loads(p.read_text(encoding="utf-8"))


class TestInspect(DeliveryBase):
    def test_delivery_dir_shape(self):
        d = dp.inspect_delivery(self.out)
        self.assertGreaterEqual(len(d.sources), 2)
        self.assertGreaterEqual(len(d.parts), 2, "需要多分片才能覆盖 part 逻辑")
        self.assertTrue(any(len(s.parts) > 1 for s in d.sources),
                        "至少要有一个源文件切成多片")
        windows = {p.shard_name: p.windows for p in d.parts}
        self.assertTrue(all(v > 0 for v in windows.values()))

    def test_a1_image_record_dropped(self):
        ledger = json.loads((self.out / "ledger.json").read_text(encoding="utf-8"))
        self.assertEqual(ledger["totals"]["has_image"], 1, "A1：含图记录应整条丢弃")
        m = json.loads((self.out / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(m["STOP_REASON"], "corpus_exhausted")

    def test_paused_delivery_refused(self):
        tmp = self.tmp / "paused"
        shutil.copytree(self.out, tmp)
        m = json.loads((tmp / "manifest.json").read_text(encoding="utf-8"))
        m["STOP_REASON"] = "segment_paused"
        m["SEGMENT_PAUSED"] = True
        (tmp / "manifest.json").write_text(json.dumps(m), encoding="utf-8")
        with self.assertRaises(dp.DeliveryError):
            dp.inspect_delivery(tmp)

    def test_stray_shard_refused(self):
        tmp = self.tmp / "stray"
        shutil.copytree(self.out, tmp)
        (tmp / "gpt-6-astra-9999-20261001.jsonl").write_text("{}\n", encoding="utf-8")
        with self.assertRaises(dp.DeliveryError):
            dp.inspect_delivery(tmp)

    def test_line_count_mismatch_refused(self):
        tmp = self.tmp / "shrunk"
        shutil.copytree(self.out, tmp)
        shard = sorted(p for p in tmp.glob("*.part-*.jsonl") or list(tmp.glob("*.jsonl"))
                       if p.name not in dp.NON_SHARD_JSONL)[0]
        shard.write_bytes(shard.read_bytes() * 2)   # 行数翻倍 -> 与 manifest 不符
        with self.assertRaises(dp.DeliveryError):
            dp.inspect_delivery(tmp)


class TestPack(DeliveryBase):
    def test_dry_run_writes_nothing(self):
        staging = self.tmp / "dry_staging"
        rc = dp.main([str(self.out), "--staging-dir", str(staging)])
        self.assertEqual(rc, 0)
        self.assertFalse(staging.exists(), "dry-run 不能写盘")

    def test_pack_manifest_and_readme(self):
        staging = self.tmp / "pack_only"
        self.assertEqual(self.deliver("--phase", "pack", staging=staging), 0)
        self.assertTrue((staging / dp.DELIVERY_MANIFEST).exists())
        self.assertTrue((staging / dp.DELIVERY_README).exists())
        m = self.manifest(staging)

        self.assertEqual(m["schema"], dp.SCHEMA)
        self.assertEqual(m["caliber"]["profile"], "A1")
        self.assertTrue(m["caliber"]["a1_drop_image_records"])
        self.assertEqual(m["caliber"]["tokenizer"], "o200k_base")
        self.assertIn("encode_ordinary", m["caliber"]["token_counting"])

        pipeline = json.loads((self.out / "manifest.json").read_text(encoding="utf-8"))
        self.assertEqual(m["totals"]["tokens"], pipeline["delivered_tokens"])
        self.assertEqual(m["totals"]["parts"], len(pipeline["shards"]))
        self.assertEqual(m["object_count"], len(m["objects"]) + 2)

        # 每个对象都真实存在，且自己的 sha256 与实际文件一致
        for o in m["objects"]:
            p = staging / o["name"]
            self.assertTrue(p.exists(), o["name"])
            self.assertEqual(p.stat().st_size, o["size"])
            self.assertEqual(dp.sha256_file(p), o["sha256"])
            self.assertEqual(o["sha256_raw"], dp.decompress_sha256(p),
                             "压缩包必须能解压回原始字节")

        readme = (staging / dp.DELIVERY_README).read_text(encoding="utf-8")
        for needle in ("A1", "o200k_base", "encode_ordinary", "sha256", "zstd", "has_image"):
            self.assertIn(needle, readme)

        # 逐文件 token 与账本一致
        ledger = json.loads((self.out / "ledger.json").read_text(encoding="utf-8"))
        for f in m["files"]:
            self.assertEqual(f["tokens"], ledger["files"][f["source_key"]]["delivered_tokens"])
            self.assertEqual(f["windows"],
                             ledger["files"][f["source_key"]]["delivered_windows"])

    def test_pack_idempotent_no_recompress(self):
        staging = self.tmp / "idem"
        self.assertEqual(self.deliver("--phase", "pack", staging=staging), 0)
        first = self.manifest(staging)
        before = {p.name: p.stat().st_mtime_ns for p in staging.glob("*.zst")}
        self.assertEqual(self.deliver("--phase", "pack", staging=staging), 0)
        second = self.manifest(staging)
        after = {p.name: p.stat().st_mtime_ns for p in staging.glob("*.zst")}
        self.assertEqual(before, after, "重跑不应重压")
        self.assertEqual(first["generated_at"], second["generated_at"],
                         "清单重写会让 sha256 变，续跑会被自己判成冲突")


class TestUpload(DeliveryBase):
    def _sim(self, name):
        return self.tmp / name

    def test_upload_and_resume(self):
        sim = self._sim("sim_ok")
        staging = self.tmp / "st_ok"
        self.assertEqual(self.deliver(staging=staging, sim=sim, extra=["--date", "2026-11-04"]), 0)
        objects = list((sim / "objects" / "2026-11-04").rglob("*"))
        files = [p for p in objects if p.is_file()]
        m = self.manifest(staging)
        self.assertEqual(len(files), m["object_count"])
        self.assertTrue((sim / "objects" / "2026-11-04" / dp.DELIVERY_MANIFEST).exists())
        report = json.loads((staging / dp.UPLOAD_REPORT).read_text(encoding="utf-8"))
        self.assertTrue(report["ok"])
        self.assertEqual(report["object_count"], {"expected": m["object_count"],
                                                  "remote": m["object_count"]})
        self.assertTrue(report["readback"] and all(r["ok"] for r in report["readback"]))

        # 续跑：全部跳过，不新建目录
        self.assertEqual(self.deliver(staging=staging, sim=sim, extra=["--date", "2026-11-04"]), 0)
        report2 = json.loads((staging / dp.UPLOAD_REPORT).read_text(encoding="utf-8"))
        self.assertEqual(report2["uploaded"], [])
        self.assertEqual(report2["target_prefix"], "2026-11-04/")
        self.assertEqual(len(report2["skipped"]), m["object_count"])

    def test_occupied_dir_increments(self):
        sim = self._sim("sim_occupied")
        staging = self.tmp / "st_occupied"
        (sim / "objects" / "2026-11-04").mkdir(parents=True)
        (sim / "objects" / "2026-11-04" / "someone-else.txt").write_text("x")
        self.assertEqual(self.deliver(staging=staging, sim=sim, extra=["--date", "2026-11-04"]), 0)
        report = json.loads((staging / dp.UPLOAD_REPORT).read_text(encoding="utf-8"))
        self.assertEqual(report["target_prefix"], "2026-11-04-delivery-1/")
        self.assertTrue((sim / "objects" / "2026-11-04-delivery-1" / dp.DELIVERY_MANIFEST).exists())
        # 原目录一动不动
        self.assertEqual([p.name for p in (sim / "objects" / "2026-11-04").iterdir()],
                         ["someone-else.txt"])

    def test_half_upload_resumes(self):
        sim = self._sim("sim_half")
        staging = self.tmp / "st_half"
        rc = self.deliver(staging=staging, sim=sim,
                          extra=["--date", "2026-11-04", "--simulate-fail-after", "2"])
        self.assertEqual(rc, 137, "模拟中断应非 0 退出")
        leftovers = [p for p in (sim / "objects").rglob("*") if p.is_file()]
        self.assertGreaterEqual(len(leftovers), 2)

        rc = self.deliver(staging=staging, sim=sim, extra=["--date", "2026-11-04"])
        self.assertEqual(rc, 0)
        report = json.loads((staging / dp.UPLOAD_REPORT).read_text(encoding="utf-8"))
        self.assertTrue(report["ok"])
        self.assertEqual(len(report["repaired"]), 1, "半截对象应重传 1 个")
        m = self.manifest(staging)
        self.assertEqual(report["object_count"]["remote"], m["object_count"])

    def test_conflict_halts_batch(self):
        sim = self._sim("sim_conflict")
        staging = self.tmp / "st_conflict"
        self.assertEqual(self.deliver(staging=staging, sim=sim, extra=["--date", "2026-11-04"]), 0)
        m = self.manifest(staging)
        victim = next((sim / "objects" / "2026-11-04" / o["name"]) for o in m["objects"])
        victim.write_text("foreign content")
        (sim / "meta" / f"2026-11-04/{victim.name}.json").unlink(missing_ok=True)

        before = len([p for p in (sim / "objects").rglob("*") if p.is_file()])
        rc = self.deliver(staging=staging, sim=sim, extra=["--date", "2026-11-04"])
        self.assertEqual(rc, 3, "同名不同内容必须整批停（退出码 3）")
        after = len([p for p in (sim / "objects").rglob("*") if p.is_file()])
        self.assertEqual(before, after, "冲突时一个字节都不许写")
        self.assertEqual(victim.read_text(), "foreign content")

    def test_phase_verify(self):
        sim = self._sim("sim_verify")
        staging = self.tmp / "st_verify"
        self.assertEqual(self.deliver(staging=staging, sim=sim, extra=["--date", "2026-11-04"]), 0)
        self.assertEqual(self.deliver("--phase", "verify", staging=staging, sim=sim,
                                      extra=["--date", "2026-11-04"]), 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
