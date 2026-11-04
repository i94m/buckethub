"""裁决 B（emit 侧整条丢弃）回归：

- 某条记录切窗后存在「单条 message 自身 > 128k」，整条不交付；
- 账本记 not_delivered_oversize_message，恒等式仍是 kept = delivered + not_delivered；
- 不新增第 6 道过滤、不改 scanned 漏斗；
- 边界：单条 127,999 以下的记录照常交付，别把边界切错。
"""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline import corpus_pipeline as cp  # noqa: E402

TIKTOKEN_CACHE = Path.home() / "buckethub-1b/tiktoken_cache"


def _msg(text: str, i: int) -> dict:
    return {"role": "user" if i % 2 == 0 else "assistant",
            "content": [{"type": "text", "text": text},
                        {"type": "encrypted_content", "encrypted_content": f"e{i}"}]}


def _record(uid: str, rs: str, big_chars: int, n: int = 100) -> dict:
    msgs = [_msg(f"m{i:03d}", i) for i in range(n)]
    if big_chars:
        msgs[50] = _msg("巨" * big_chars, 50)      # 单条 message 自身超限
    return {"session_id": f"{uid}~p", "request": {"messages": msgs},
            "response": {"response_data": {"output": [{"id": rs}]}}}


class TestEmitOversizeDropped(unittest.TestCase):
    """一条 24 万 token 的单条 message + 一条刚好在上限内的对照记录。"""

    @classmethod
    def setUpClass(cls):
        import os
        os.environ.setdefault("TIKTOKEN_CACHE_DIR", str(TIKTOKEN_CACHE))
        cls.tmp = tempfile.TemporaryDirectory()
        root = Path(cls.tmp.name)
        src = root / "src"
        src.mkdir()
        # 24 万字符 ≈ 24 万 token（CJK 约 1 token/字），必定触发整条丢弃
        rows = [_record("uuid-big", "rs_big", 240_000),
                _record("uuid-edge", "rs_edge", 127_000)]   # 单条 12.7 万，未越界
        with open(src / "gpt-oversize-0001.jsonl", "w", encoding="utf-8") as fh:
            for r in rows:
                fh.write(json.dumps(r, ensure_ascii=False) + "\n")
        cls.out = root / "out"
        args = SimpleNamespace(
            phase="all", execute=True, reset=False, out_dir=str(cls.out),
            endpoint="", region="us-east-005", bucket="b", prefix="p", keys=[],
            keys_file=None, local_dir=str(src), max_records=0, min_messages=100,
            max_sample_tokens=128_000, target_tokens=1_000_000_000,
            shard_bytes=524_288_000, tokenizer="o200k_base",
            tokens_per_byte=0.1015, max_seconds=0, index_connections=1,
            emit_prefetch=1, lock_ttl=300, rank_base=0,
        )
        cls.rc = cp.main(["--execute", "--local-dir", str(src), "--out-dir", str(cls.out),
                          "--target-tokens", "1000000000"])
        cls.ledger = json.loads((cls.out / "ledger.json").read_text())
        cls.man = json.loads((cls.out / "manifest.json").read_text())
        cls.delivered = [json.loads(l) for l in (cls.out / "gpt-oversize-0001.jsonl").open(encoding="utf-8") if l.strip()]

    @classmethod
    def tearDownClass(cls):
        cls.tmp.cleanup()

    def test_exit_and_files(self):
        self.assertEqual(self.rc, 0)
        self.assertTrue((self.out / "gpt-oversize-0001.jsonl").exists())

    def test_big_record_not_delivered(self):
        ids = {r["response"]["response_data"]["output"][0]["id"] for r in self.delivered}
        self.assertNotIn("rs_big", ids, "单条 message 自身超限的记录不该进交付")
        self.assertIn("rs_edge", ids, "边界内的记录必须照常交付")

    def test_ledger_field_and_identity(self):
        row = self.ledger["files"]["gpt-oversize-0001.jsonl"]
        self.assertEqual(row.get("not_delivered_oversize_message"), 1)
        self.assertEqual(row["kept_records"], 2)
        self.assertEqual(row["delivered_records"], 1)
        self.assertEqual(row["not_delivered"], 1)
        self.assertEqual(row["kept_records"], row["delivered_records"] + row["not_delivered"])
        # 不新增第 6 道过滤、不改漏斗
        self.assertNotIn("oversize", [k for k in row if k.startswith("filter_")])
        self.assertEqual(row["scanned"], sum(int(row.get(k) or 0) for k in ("parse_error","no_session_id","no_rs_id","no_encrypted_content","too_short","has_image")) + row["passed"])

    def test_manifest_counts(self):
        self.assertEqual(self.man.get("delivered_records"), 1)
        self.assertEqual(self.man.get("not_delivered"), 1)
