"""端到端单测：合成语料 → 参考交付 → 验收；另外锁死 dry-run 行为。"""

from __future__ import annotations

import json
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from accept import cli, ledger, reference, shard, synth, tokens


class _E2EBase(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = TemporaryDirectory()
        self.root = Path(self._tmp.name)
        self.counter = cli._Counter(fake=True, allow_fake=True)
        self.cfg = shard.ShardConfig(max_record_tokens=10_000_000)
        self.raw_paths = synth.write_corpus(self.root / "raw")
        self.delivery = self.root / "delivery"
        self.manifest = reference.run_reference(
            self.raw_paths,
            self.delivery,
            counter=self.counter,
            cfg=reference.ReferenceConfig(max_record_tokens=10_000_000),
            tokenizer_meta=self.counter.meta(),
        )

    def tearDown(self) -> None:
        self._tmp.cleanup()


class ReferenceDeliveryTest(_E2EBase):
    def test_full_verify_passes(self):
        report = cli.run_verify(
            self.delivery, manifest_path=None, counter=self.counter, cfg=self.cfg
        )
        self.assertTrue(report["ok"], msg=json.dumps(report["problems"], ensure_ascii=False))
        for check in report["checks"].values():
            self.assertTrue(check["ok"], msg=json.dumps(check, ensure_ascii=False))

    def test_reconcile_closes_per_file(self):
        manifest = ledger.load_manifest(self.delivery / "manifest.json")
        self.assertEqual(ledger.reconcile(manifest), [])
        for item in manifest.files:
            self.assertTrue(item.balanced, msg=item.as_dict())
            # 每一格移除量都要有记账，0 也要写
            self.assertEqual(set(item.removed), set(ledger.CANONICAL_STEPS))
        self.assertEqual(manifest.total.raw_lines, sum(f.raw_lines for f in manifest.files))

    def test_filters_each_removed_something(self):
        manifest = ledger.load_manifest(self.delivery / "manifest.json")
        for step in ("filter_rs_id", "filter_encrypted_content", "filter_min_messages"):
            self.assertGreater(
                manifest.total.removed[step], 0, f"{step} 在合成语料里应当有样本被删掉"
            )
        self.assertGreater(manifest.total.removed["dedupe_session"], 0)
        self.assertGreater(manifest.total.removed["dedupe_prefix"], 0)

    def test_shared_prefix_negative_survives_delivery(self):
        """共享前 k 个成员后分叉的两条记录都要留在交付里。"""
        texts: list[str] = []
        for path in sorted(self.delivery.glob("deliver-*.jsonl")):
            for raw in path.read_bytes().splitlines():
                if raw.strip():
                    texts.append(raw.decode("utf-8"))
        joined = "\n".join(texts)
        self.assertIn("左支", joined)
        self.assertIn("右支", joined)

    def test_delivery_is_reproducible(self):
        second = self.root / "delivery2"
        reference.run_reference(
            self.raw_paths,
            second,
            counter=self.counter,
            cfg=reference.ReferenceConfig(max_record_tokens=10_000_000),
            tokenizer_meta=self.counter.meta(),
        )
        for path in sorted(self.delivery.glob("deliver-*.jsonl")):
            self.assertEqual(path.read_bytes(), (second / path.name).read_bytes())
        self.assertEqual(
            (self.delivery / "manifest.json").read_text(encoding="utf-8"),
            (second / "manifest.json").read_text(encoding="utf-8"),
        )

    def test_shards_respect_size_limit(self):
        cfg = shard.ShardConfig(shard_max_bytes=400 * 1024, shard_slack=0.0)
        delivery = self.root / "small_shards"
        reference.run_reference(
            self.raw_paths,
            delivery,
            counter=self.counter,
            cfg=reference.ReferenceConfig(
                max_record_tokens=10_000_000, shard_max_bytes=cfg.shard_max_bytes
            ),
            tokenizer_meta=self.counter.meta(),
        )
        paths = sorted(delivery.glob("deliver-*.jsonl"))
        self.assertGreater(len(paths), 1, "阈值调小后应当切出多个分片")
        for path in paths:
            self.assertLessEqual(path.stat().st_size, cfg.shard_max_bytes)


class TokenTargetTest(_E2EBase):
    def test_stop_at_target_accounts_for_the_rest(self):
        """到达 1B（这里用很小的阈值模拟）后立刻停，未交付部分必须记 stop_cut。"""
        delivery = self.root / "stopped"
        manifest = reference.run_reference(
            self.raw_paths,
            delivery,
            counter=self.counter,
            cfg=reference.ReferenceConfig(max_record_tokens=10_000_000, token_target=1000),
            tokenizer_meta=self.counter.meta(),
        )
        self.assertEqual(manifest["stop_reason"], "token_target_reached")
        raw = ledger.normalize_manifest(manifest)
        self.assertEqual(ledger.reconcile(raw), [])
        self.assertTrue(any(item.removed.get("stop_cut", 0) > 0 for item in raw.files))
        self.assertGreaterEqual(raw.total.delivered_tokens, 1000)


class DryRunTest(unittest.TestCase):
    def test_verify_without_execute_touches_nothing(self):
        missing = "/nonexistent-buckethub-delivery-dir"
        self.assertEqual(cli.main(["verify", "--delivery", missing]), cli.EXIT_OK)
        self.assertEqual(
            cli.main(
                [
                    "verify",
                    "--delivery",
                    missing,
                    "--execute",
                    "--tokenizer",
                    "chars",
                    "--allow-fake-tokenizer",
                ]
            ),
            cli.EXIT_FAIL,
        )

    def test_recount_and_selftest_default_to_dry_run(self):
        self.assertEqual(cli.main(["recount", "--delivery", "/nonexistent"]), cli.EXIT_OK)
        self.assertEqual(cli.main(["selftest"]), cli.EXIT_OK)
        self.assertEqual(cli.main(["fixture", "--out", "/nonexistent-out"]), cli.EXIT_OK)

    def test_report_only_written_with_execute(self):
        with TemporaryDirectory() as tmp:
            report = Path(tmp) / "report.json"
            code = cli.main(["verify", "--delivery", "/nonexistent", "--report", str(report)])
            self.assertEqual(code, cli.EXIT_OK)
            self.assertFalse(report.exists(), "dry-run 不允许写任何文件")
            code = cli.main(
                [
                    "verify",
                    "--delivery",
                    str(Path(tmp) / "nope"),
                    "--execute",
                    "--tokenizer",
                    "chars",
                    "--allow-fake-tokenizer",
                    "--report",
                    str(report),
                ]
            )
            self.assertEqual(code, cli.EXIT_FAIL)
            self.assertTrue(report.exists())

    def test_fake_tokenizer_requires_explicit_flag(self):
        with TemporaryDirectory() as tmp:
            code = cli.main(["selftest", "--execute", "--tokenizer", "chars", "--report", f"{tmp}/r.json"])
            self.assertEqual(code, cli.EXIT_CONFIG)


class CleanupTest(_E2EBase):
    def test_manifest_and_shards_agree_on_byte_sizes(self):
        manifest = ledger.load_manifest(self.delivery / "manifest.json")
        for declared in manifest.shards:
            actual = self.delivery / declared.name
            self.assertTrue(actual.exists())
            self.assertEqual(declared.bytes, actual.stat().st_size)


@unittest.skipUnless(
    tokens.tokenizer_info()["available"], "本机没有 tiktoken/o200k_base，跳过真分词端到端用例"
)
class RealTokenizerEndToEndTest(_E2EBase):
    def test_delivery_tokens_recomputed_exactly(self):
        counter = cli._Counter(fake=False)
        delivery = self.root / "delivery_real"
        manifest = reference.run_reference(
            self.raw_paths,
            delivery,
            counter=counter,
            cfg=reference.ReferenceConfig(max_record_tokens=128_000),
            tokenizer_meta=counter.meta(),
        )
        report = cli.run_verify(
            delivery, manifest_path=None, counter=counter, cfg=shard.ShardConfig()
        )
        self.assertTrue(report["ok"], msg=json.dumps(report["problems"], ensure_ascii=False))
        self.assertEqual(report["delivery"]["tokens"], manifest["totals"]["delivered_tokens"])

    def test_sample_recompute_has_zero_error(self):
        counter = cli._Counter(fake=False)
        delivery = self.root / "delivery_sample"
        reference.run_reference(
            self.raw_paths,
            delivery,
            counter=counter,
            cfg=reference.ReferenceConfig(max_record_tokens=128_000),
            tokenizer_meta=counter.meta(),
        )
        report = cli.run_recount(
            delivery, counter=counter, cfg=shard.ShardConfig(), sample=10
        )
        self.assertTrue(report["checks"]["sample_recompute"]["ok"])
        self.assertIn("抽样", report["checks"]["sample_recompute"]["detail"])
        self.assertTrue(report["ok"], msg=json.dumps(report["problems"], ensure_ascii=False))

    def test_overlong_record_is_windowed(self):
        counter = cli._Counter(fake=False)
        delivery = self.root / "delivery_windowed"
        reference.run_reference(
            self.raw_paths,
            delivery,
            counter=counter,
            cfg=reference.ReferenceConfig(max_record_tokens=2048),
            tokenizer_meta=counter.meta(),
        )
        cfg = shard.ShardConfig(max_record_tokens=2048)
        report = shard.check_delivery(delivery, cfg, counter=counter, check_prefix_invariant=False)
        self.assertEqual([p.message for p in report.all_problems() if p.level == "error"], [])
        self.assertGreater(sum(item.windowed_records for item in report.shards), 0)


if __name__ == "__main__":
    unittest.main()
