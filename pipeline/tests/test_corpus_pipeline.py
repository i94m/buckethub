#!/usr/bin/env python3
"""corpus_pipeline 单元/集成测试 —— 只使用本地合成数据，不联网、不访问 B2。

运行：
    python -m unittest discover -s pipeline/tests -v
"""
from __future__ import annotations

import copy
import json
import os
import sys
import tempfile
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import corpus_pipeline as cp  # noqa: E402


# ---------------------------------------------------------------- 合成数据工具

def msg(text, role="user"):
    return {"role": role, "content": [{"type": "input_text", "text": text}]}


def encrypted_msg(text="enc"):
    return {"role": "assistant", "content": [{"type": "encrypted_content", "encrypted_content": f"gAAAA-{text}"}]}


def make_record(uuid="u-1", sid_suffix="ua", n=100, seed="a", rs_id=True, enc=True, ts="2026-10-01T00:00:00Z"):
    """前若干条 message 内容由 seed 决定，便于构造「前缀」关系。"""
    msgs = [msg(f"{seed}-{i}") for i in range(n)]
    if enc and msgs:
        msgs[0]["content"].append({"type": "encrypted_content", "encrypted_content": "gAAAA-shared"})
    out = [{"id": "rs_abc", "type": "reasoning", "content": []}] if rs_id else []
    return {
        "session_id": f"{uuid}~{sid_suffix}",
        "request_id": f"req-{uuid}-{sid_suffix}",
        "timestamp": ts,
        "request": {"messages": msgs},
        "response": {"response_data": {"output": out}},
        "model": "gpt-6-astra",
    }


def index_item(record, key="k.jsonl", off=0, length=None, order=0):
    msgs = cp.messages_of(record)
    return {
        "id": f"{key}#{off}",
        "key": key,
        "off": off,
        "len": length if length is not None else len(json.dumps(record)),
        "uuid": cp.session_uuid(record.get("session_id")),
        "timestamp": record.get("timestamp", ""),
        "n_msgs": len(msgs),
        "seq": cp.record_seq(msgs),
        "order": order,
    }


class FakeEncoder:
    """每 10 个字符算 1 token，向上取整；用于窗口切分的确定性测试。"""

    def encode_ordinary(self, text):
        return list(range(max(1, len(text) // 10 + 1)))


def ids(items):
    return sorted(it["id"] for it in items)


# ---------------------------------------------------------------- 过滤口径

class TestFilter(unittest.TestCase):
    def test_pass(self):
        self.assertIsNone(cp.filter_reason(make_record(n=100)))

    def test_too_short(self):
        self.assertEqual(cp.filter_reason(make_record(n=99)), "too_short")

    def test_boundary_100(self):
        self.assertIsNone(cp.filter_reason(make_record(n=100)))
        self.assertEqual(cp.filter_reason(make_record(n=100 - 1)), "too_short")

    def test_no_rs_id(self):
        self.assertEqual(cp.filter_reason(make_record(rs_id=False)), "no_rs_id")

    def test_no_encrypted_content(self):
        self.assertEqual(cp.filter_reason(make_record(enc=False)), "no_encrypted_content")

    def test_empty_encrypted_content_is_not_counted(self):
        rec = make_record(enc=False)
        rec["request"]["messages"][3]["content"].append({"type": "encrypted_content", "encrypted_content": ""})
        self.assertEqual(cp.filter_reason(rec), "no_encrypted_content")

    def test_no_session_id(self):
        rec = make_record()
        rec["session_id"] = ""
        self.assertEqual(cp.filter_reason(rec), "no_session_id")

    def test_uuid_split(self):
        self.assertEqual(cp.session_uuid("abc-123~u9f8e"), "abc-123")
        self.assertEqual(cp.session_uuid("abc-123"), "abc-123")
        self.assertEqual(cp.session_uuid(None), "")

    def test_ledger_closure(self):
        led = cp.Ledger()
        for reason in (None, None, "too_short", "no_rs_id", "parse_error"):
            led.tally_filter("f.jsonl", reason)
        t = led.totals()
        self.assertEqual(t["scanned"], 5)
        self.assertEqual(t["passed"], 2)
        self.assertTrue(t["filter_closure_ok"])


# ---------------------------------------------------------------- session 去重

class TestSessionDedup(unittest.TestCase):
    def test_keep_longest_per_uuid(self):
        short = make_record(uuid="same", sid_suffix="u1", n=100)
        long_ = make_record(uuid="same", sid_suffix="u2", n=140)
        a, b = index_item(short, off=1), index_item(long_, off=2, order=1)
        kept, removed = cp.session_dedup([a, b])
        self.assertEqual(ids(kept), ["k.jsonl#2"])
        self.assertEqual(ids(removed), ["k.jsonl#1"])

    def test_tie_break_by_timestamp(self):
        a = index_item(make_record(uuid="u", sid_suffix="u1", ts="2026-01-01T00:00:00Z"), off=1)
        b = index_item(make_record(uuid="u", sid_suffix="u2", ts="2026-02-01T00:00:00Z"), off=2, order=1)
        kept, _ = cp.session_dedup([a, b])
        self.assertEqual(ids(kept), ["k.jsonl#2"])

    def test_different_uuids_kept(self):
        a = index_item(make_record(uuid="u1"), off=1)
        b = index_item(make_record(uuid="u2"), off=2, order=1)
        kept, removed = cp.session_dedup([a, b])
        self.assertEqual(len(kept), 2)
        self.assertEqual(removed, [])

    def test_same_uuid_different_suffix_still_same_session(self):
        # 锁定口径 4：按 ~ 之前的 uuid 段去重，不是整串
        a = index_item(make_record(uuid="x", sid_suffix="u111"), off=1)
        b = index_item(make_record(uuid="x", sid_suffix="u222"), off=2, order=1)
        kept, removed = cp.session_dedup([a, b])
        self.assertEqual(len(kept), 1)
        self.assertEqual(len(removed), 1)


# ---------------------------------------------------------------- 前缀去重

class TestPrefixDedup(unittest.TestCase):
    def _items(self, seqs):
        return [{"id": f"r{i}", "seq": s, "order": i} for i, s in enumerate(seqs)]

    def test_abcde_abcdefg_keeps_longer(self):
        kept, removed, exact = cp.prefix_dedup(self._items([b"abcde", b"abcdefg"]))
        self.assertEqual([k["id"] for k in kept], ["r1"])
        self.assertEqual([r["id"] for r in removed], ["r0"])
        self.assertEqual(exact, 0)

    def test_order_of_input_does_not_matter(self):
        kept, removed, _ = cp.prefix_dedup(self._items([b"abcdefg", b"abcde"]))
        self.assertEqual([k["id"] for k in kept], ["r0"])
        self.assertEqual([r["id"] for r in removed], ["r1"])

    def test_identical_keeps_one(self):
        kept, removed, exact = cp.prefix_dedup(self._items([b"abcd", b"abcd", b"abcd"]))
        self.assertEqual(len(kept), 1)
        self.assertEqual(len(removed), 2)
        self.assertEqual(exact, 2)

    def test_same_length_different_content_keeps_both(self):
        kept, removed, _ = cp.prefix_dedup(self._items([b"aaaa", b"aaab"]))
        self.assertEqual(len(kept), 2)
        self.assertEqual(removed, [])

    def test_cross_session_prefix_keeps_longest(self):
        base = make_record(uuid="s1", n=100)
        longer = make_record(uuid="s2", n=104)  # 前 100 条与 base 完全相同
        base["request"]["messages"] = base["request"]["messages"]
        longer["request"]["messages"] = copy.deepcopy(base["request"]["messages"]) + [
            msg(f"s2-102"), msg("s2-103")]
        a, b = index_item(base, off=1), index_item(longer, off=2, order=1)
        result = cp.plan_dedup([a, b])
        self.assertEqual([k["id"] for k in result["kept"]], ["k.jsonl#2"])
        self.assertEqual(len(result["prefix_removed"]), 1)

    def test_negative_shares_first_k_then_diverges(self):
        base = make_record(uuid="s1", n=100)
        other = make_record(uuid="s2", n=100, seed="b")  # 首条就不同
        a, b = index_item(base, off=1), index_item(other, off=2, order=1)
        result = cp.plan_dedup([a, b])
        self.assertEqual(len(result["kept"]), 2)
        self.assertEqual(result["prefix_removed"], [])

    def test_negative_shared_prefix_then_diverge_same_length(self):
        shared = [msg(f"x-{i}") for i in range(5)]
        s1 = shared + [msg("tail-a")]
        s2 = shared + [msg("tail-b")]
        a, b = index_item({"request": {"messages": s1}, "session_id": "u1~a"}), \
            index_item({"request": {"messages": s2}, "session_id": "u2~a"}, off=1, order=1)
        result = cp.plan_dedup([a, b])
        self.assertEqual(len(result["kept"]), 2)
        self.assertEqual(result["prefix_removed"], [])

    def test_linear_chain_keeps_only_longest(self):
        seqs = [b"ab", b"abcd", b"abcdef", b"abcdefgh"]
        kept, removed, _ = cp.prefix_dedup(self._items(seqs))
        self.assertEqual([k["id"] for k in kept], ["r3"])
        self.assertEqual(len(removed), 3)

    def test_empty_messages_sequence(self):
        # 两条空序列：算完全重复，只留一条；不能崩
        kept, removed, exact = cp.prefix_dedup(self._items([b"", b""]))
        self.assertEqual(len(kept), 1)
        self.assertEqual(exact, 1)
        self.assertEqual(cp.record_seq([]), b"")

    def test_prefix_of_empty_is_everything(self):
        # 空序列是任何非空序列的严格前缀
        kept, removed, _ = cp.prefix_dedup(self._items([b"", b"ab"]))
        self.assertEqual([k["id"] for k in kept], ["r1"])

    def test_plan_order_session_before_prefix(self):
        # 同 uuid 两条，短的既被 session 去重也被前缀覆盖；必须只记在 session 一栏
        a = index_item(make_record(uuid="same", n=100), off=1)
        b = index_item(make_record(uuid="same", n=102), off=2, order=1)
        b["seq"] = a["seq"] + b"\x00" * 8
        result = cp.plan_dedup([a, b])
        self.assertEqual(len(result["session_removed"]), 1)
        self.assertEqual(len(result["prefix_removed"]), 0)
        self.assertEqual(len(result["kept"]), 1)


# ---------------------------------------------------------------- 分窗

class TestWindows(unittest.TestCase):
    def test_under_limit_single_window(self):
        msgs = [msg("x" * 50) for _ in range(3)]
        windows, toks, oversize = cp.split_windows(msgs, FakeEncoder(), max_tokens=10_000)
        self.assertEqual(len(windows), 1)
        self.assertEqual(oversize, 0)
        self.assertEqual(toks, [sum(toks)])

    def test_split_respects_limit(self):
        msgs = [msg("x" * 100) for _ in range(10)]  # 每条 ~11 token
        enc = FakeEncoder()
        windows, toks, oversize = cp.split_windows(msgs, enc, max_tokens=40)
        self.assertGreater(len(windows), 1)
        self.assertEqual(sum(len(w) for w in windows), len(msgs))
        for tok in toks:
            self.assertLessEqual(tok, 40)
        self.assertEqual(oversize, 0)

    def test_single_oversize_message_isolated(self):
        msgs = [msg("small"), msg("y" * 10_000), msg("small2")]
        windows, toks, oversize = cp.split_windows(msgs, FakeEncoder(), max_tokens=50)
        self.assertEqual(oversize, 1)
        self.assertTrue(any(len(w) == 1 and len(cp.canon(w[0])) > 50 for w in windows))
        self.assertEqual(sum(len(w) for w in windows), 3)


# ---------------------------------------------------------------- 端到端（本地目录）

def write_dataset(dirpath: Path, records):
    key = "synthetic-0001.jsonl"
    with open(dirpath / key, "wb") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False).encode() + b"\n")
    return key


class TestEndToEnd(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.src = self.root / "src"
        self.src.mkdir()
        self.out = self.root / "out"

    def tearDown(self):
        self.tmp.cleanup()

    def run_pipeline(self, *extra):
        argv = ["--execute", "--local-dir", str(self.src), "--out-dir", str(self.out)] + list(extra)
        return cp.main(argv)

    def test_dry_run_writes_nothing(self):
        write_dataset(self.src, [make_record(n=100)])
        rc = cp.main(["--local-dir", str(self.src), "--out-dir", str(self.out)])
        self.assertEqual(rc, 0)
        self.assertFalse(self.out.exists(), "dry-run 不允许创建输出目录")

    def test_full_flow_and_ledger(self):
        base = make_record(uuid="u1", n=100, seed="a")
        longer = make_record(uuid="u2", n=104, seed="a")
        longer["request"]["messages"] = copy.deepcopy(base["request"]["messages"]) + [msg("u2-100"), msg("u2-101"),
                                                                                    msg("u2-102"), msg("u2-103")]
        other = make_record(uuid="u3", n=100, seed="z")
        short = make_record(uuid="u4", n=99, seed="q")
        write_dataset(self.src, [base, longer, other, short])
        self.run_pipeline("--target-tokens", "1000000000", "--shard-bytes", "1000000")

        manifest = json.loads((self.out / "manifest.json").read_text())
        self.assertEqual(manifest["STOP_REASON"], "corpus_exhausted")
        self.assertEqual(manifest["ledger"]["scanned"], 4)
        self.assertEqual(manifest["ledger"]["passed"], 3)     # short 被过滤
        self.assertTrue(manifest["ledger"]["filter_closure_ok"])
        self.assertEqual(manifest["ledger"]["prefix_removed"], 1)   # base 是 longer 的前缀

        lines = []
        for shard in manifest["shards"]:
            p = self.out / shard["file"]
            lines += [json.loads(ln) for ln in p.read_text().splitlines()]
        self.assertEqual(len(lines), 2)          # longer + other
        self.assertEqual({ln["session_id"].split("~")[0] for ln in lines}, {"u2", "u3"})

        keep = [json.loads(ln) for ln in (self.out / "keep.jsonl").read_text().splitlines()]
        self.assertEqual(len(keep), 2)
        self.assertEqual(manifest["delivered_samples"], len(lines))

    def test_shard_rotation(self):
        recs = [make_record(uuid=f"u{i}", n=100, seed=f"s{i}") for i in range(4)]
        write_dataset(self.src, recs)
        self.run_pipeline("--target-tokens", "1000000000", "--shard-bytes", "20000")
        manifest = json.loads((self.out / "manifest.json").read_text())
        self.assertGreaterEqual(len(manifest["shards"]), 2)
        # 硬约束：任何一片都不超过阈值（写入前判断换包）
        for shard in manifest["shards"]:
            self.assertLessEqual(shard["bytes"], 20000)
            self.assertGreater(shard["samples"], 0)
        self.assertEqual(sum(s["samples"] for s in manifest["shards"]), 4)

    def test_stop_reason_token_target(self):
        recs = [make_record(uuid=f"u{i}", n=100, seed=f"s{i}") for i in range(5)]
        write_dataset(self.src, recs)
        self.run_pipeline("--target-tokens", "1")   # 第一条写完就过线
        manifest = json.loads((self.out / "manifest.json").read_text())
        self.assertEqual(manifest["STOP_REASON"], "token_target_reached")
        self.assertEqual(manifest["delivered_samples"], 1)

    def test_resume_does_not_duplicate(self):
        recs = [make_record(uuid=f"u{i}", n=100, seed=f"s{i}") for i in range(4)]
        write_dataset(self.src, recs)
        self.run_pipeline("--target-tokens", "1")
        first = json.loads((self.out / "manifest.json").read_text())["delivered_samples"]
        self.run_pipeline("--target-tokens", "1000000000")
        manifest = json.loads((self.out / "manifest.json").read_text())
        lines = []
        for shard in manifest["shards"]:
            lines += (self.out / shard["file"]).read_text().splitlines()
        self.assertEqual(len(lines), 4, f"续跑后不应重复（首轮 {first} 行）")
        self.assertEqual(len(set(lines)), 4)

    def test_windows_written_when_over_limit(self):
        # 用极小窗口阈值触发切窗（tokenizer 真实存在时才跑）
        try:
            import tiktoken  # noqa: F401
        except Exception:
            self.skipTest("tiktoken 未安装，跳过切窗集成用例")
        write_dataset(self.src, [make_record(uuid="u1", n=100)])
        self.run_pipeline("--target-tokens", "1000000000", "--max-sample-tokens", "200")
        manifest = json.loads((self.out / "manifest.json").read_text())
        lines = []
        for shard in manifest["shards"]:
            lines += [json.loads(ln) for ln in (self.out / shard["file"]).read_text().splitlines()]
        self.assertGreater(len(lines), 1)
        self.assertTrue(all("_window" in ln for ln in lines))
        self.assertEqual([ln["_window"]["index"] for ln in lines], list(range(len(lines))))
        self.assertTrue(all(ln["_window"]["of"] == len(lines) for ln in lines))
        for ln in lines:
            self.assertEqual(ln["_window"]["messages_total"], 100)


if __name__ == "__main__":
    unittest.main(verbosity=2)


# ---------------------------------------------------------------- 联调回归用例
# 2026-11-03 联调（常小玲独立重算）发现的 4 个真 bug，每条用例都能在修复前复现。

class _PipelineCase(unittest.TestCase):
    """公共脚手架：本地目录模式跑全流程。"""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.src = self.root / "src"
        self.src.mkdir()
        self.out = self.root / "out"

    def tearDown(self):
        self.tmp.cleanup()

    def run_pipeline(self, *extra):
        return cp.main(["--execute", "--local-dir", str(self.src), "--out-dir", str(self.out)] + list(extra))

    def manifest(self):
        return json.loads((self.out / "manifest.json").read_text())

    def ledger(self):
        """优先读 ledger.json；只跑 plan 阶段的旧版本没有该文件，回退到 state.json 里的账本。"""
        path = self.out / "ledger.json"
        if path.exists():
            return json.loads(path.read_text())
        state = json.loads((self.out / "state.json").read_text())
        files = state.get("ledger") or {}
        totals = {}
        for row in files.values():
            for key, val in row.items():
                if isinstance(val, int):
                    totals[key] = totals.get(key, 0) + val
        for key, val in (state.get("ledger_meta") or {}).items():
            totals[key] = val
        return {"files": files, "totals": totals}

    def delivered(self):
        lines = []
        for shard in self.manifest()["shards"]:
            lines += [json.loads(ln) for ln in (self.out / shard["file"]).read_text().splitlines()]
        return lines


class TestRegressionWindowTarget(_PipelineCase):
    """① 切窗必须写回 request.messages（裁决：下游按真实 schema 读）。"""

    def setUp(self):
        super().setUp()
        try:
            import tiktoken
        except Exception:
            self.skipTest("tiktoken 未安装，跳过真分词回归用例")
        self.enc = tiktoken.get_encoding("o200k_base")

    def test_window_goes_into_request_messages(self):
        rec = make_record(uuid="u1", n=100)
        # 其余字段必须原样保留（裁员会看 request 里的 model/tools 和顶层字段）
        rec["request"]["model"] = "gpt-6-astra"
        rec["request"]["tools"] = [{"type": "function", "name": "shell"}]
        rec["request"]["prompt_cache_key"] = "cache-key"
        write_dataset(self.src, [rec])
        self.run_pipeline("--target-tokens", "1000000000", "--max-sample-tokens", "150")

        lines = self.delivered()
        self.assertGreater(len(lines), 1, "用例前提：该记录必须被切成多个窗口")

        for ln in lines:
            self.assertIn("request", ln)
            self.assertNotIn("messages", ln, "窗口不允许写在顶层 messages")
            wm = ln["request"]["messages"]
            self.assertLessEqual(cp.count_tokens(wm, self.enc), 150,
                                 f"窗口按 request.messages 复算超出 128k 口径（实际 {len(wm)} 条）")

        total = sum(len(ln["request"]["messages"]) for ln in lines)
        self.assertEqual(total, 100, "所有窗口的 request.messages 必须拼回原始 100 条")
        for ln in lines:
            self.assertEqual(ln["request"]["model"], "gpt-6-astra")
            self.assertEqual(ln["request"]["prompt_cache_key"], "cache-key")
            self.assertEqual(ln["request"]["tools"], [{"type": "function", "name": "shell"}])
            self.assertEqual(ln["session_id"], "u1~ua")
        self.assertEqual([ln["_window"]["index"] for ln in lines], list(range(len(lines))))
        self.assertTrue(all(ln["_window"]["messages_total"] == 100 for ln in lines))


class TestRegressionWindowGroupAtomicity(_PipelineCase):
    """④ 到 1B 时不能把一组窗口拦腰截断。"""

    def setUp(self):
        super().setUp()
        try:
            import tiktoken
        except Exception:
            self.skipTest("tiktoken 未安装，跳过真分词回归用例")

    def test_stop_keeps_whole_window_group(self):
        write_dataset(self.src, [make_record(uuid="u1", n=100)])
        # 目标 1 token：第一条记录的第一个窗口就"过线"，但整组必须一起落
        self.run_pipeline("--target-tokens", "1", "--max-sample-tokens", "150")

        lines = self.delivered()
        man = self.manifest()
        self.assertGreater(len(lines), 1)
        self.assertEqual(man["STOP_REASON"], "token_target_reached")
        # 组内连续、无缺口（拦腰截断的特征就是 index 缺尾）
        self.assertEqual([ln["_window"]["index"] for ln in lines], list(range(len(lines))))
        self.assertTrue(all(ln["_window"]["of"] == len(lines) for ln in lines))
        self.assertEqual(man["not_delivered"], 0)
        self.assertEqual(man["delivered_records"], 1)

    def test_manifest_records_where_it_stopped(self):
        write_dataset(self.src, [make_record(uuid=f"u{i}", n=100, seed=f"s{i}") for i in range(3)])
        self.run_pipeline("--target-tokens", "1", "--max-sample-tokens", "150")
        man = self.manifest()
        self.assertIn("stop_after_record", man)
        self.assertTrue(man["stop_after_record"]["id"])
        self.assertEqual(man["stop_after_record"]["id"],
                         self.manifest()["shards"] and man["stop_after_record"]["id"])


class TestRegressionPlanIdempotent(_PipelineCase):
    """③ plan 重跑不能重复累计移除量。"""

    def test_plan_three_runs_stable(self):
        base = make_record(uuid="u1", n=100, seed="a")
        longer = make_record(uuid="u2", n=104, seed="a")
        longer["request"]["messages"] = copy.deepcopy(base["request"]["messages"]) + [
            msg("u2-100"), msg("u2-101"), msg("u2-102"), msg("u2-103")]
        same_session = make_record(uuid="u3", n=100, seed="c")
        same_session_long = make_record(uuid="u3", n=105, seed="c")
        same_session_long["request"]["messages"] = copy.deepcopy(same_session["request"]["messages"]) + [
            msg("u3-extra") for _ in range(5)]
        write_dataset(self.src, [base, longer, same_session, same_session_long])

        self.run_pipeline("--phase", "index")
        snapshots = []
        for i in range(3):
            self.run_pipeline("--phase", "plan")
            led = self.ledger()
            snapshots.append((led["totals"]["session_removed"],
                              led["totals"]["prefix_removed"],
                              led["totals"].get("exact_dup", 0)))

        self.assertEqual(snapshots[0], (1, 1, 0), f"首轮移除量不对：{snapshots[0]}")
        self.assertEqual(snapshots[0], snapshots[1], "plan 第二次重跑改变了移除量")
        self.assertEqual(snapshots[1], snapshots[2], "plan 第三次重跑改变了移除量")


class TestRegressionDeliveredLedger(_PipelineCase):
    """② 首次运行的逐文件交付计数不能是 0。"""

    def test_first_run_ledger_counts(self):
        write_dataset(self.src, [make_record(uuid=f"u{i}", n=100, seed=f"s{i}") for i in range(4)])
        self.run_pipeline("--target-tokens", "1000000000")

        man = self.manifest()
        led = self.ledger()
        per_file_records = sum(f["delivered_records"] for f in led["files"].values())
        per_file_windows = sum(f["delivered_windows"] for f in led["files"].values())

        self.assertGreater(man["delivered_records"], 0)
        self.assertEqual(per_file_records, man["delivered_records"],
                         "ledger.json 逐文件交付条数之和必须等于 manifest 总量")
        self.assertEqual(per_file_windows, man["delivered_samples"])
        self.assertEqual(man["ledger"]["delivered_records"], man["delivered_records"])
        self.assertEqual(man["ledger"]["delivered_windows"], man["delivered_samples"])


# ---------------------------------------------------------------- 分块 Range GET 读取

class _FakeS3:
    """只实现 get_object(Range=...) 的最小 S3 桩，用于验证分块读取。"""

    def __init__(self, data: bytes, short_read_at=None):
        self.data = data
        self.calls = []
        self.short_read_at = short_read_at

    def get_object(self, Bucket, Key, Range):
        assert Range.startswith("bytes=")
        start, end = (int(x) for x in Range[6:].split("-"))
        chunk = self.data[start:end + 1]
        if self.short_read_at is not None and len(self.calls) == self.short_read_at:
            chunk = chunk[: max(1, len(chunk) // 2)]      # 模拟短读
        self.calls.append((start, end))
        return {"Body": __import__("io").BytesIO(chunk)}


class TestRangedReader(unittest.TestCase):
    def test_ranged_reader_matches_sequential(self):
        from types import SimpleNamespace
        rows = [json.dumps({"i": i, "text": "中文-" + "x" * (i % 37)}, ensure_ascii=False).encode() for i in range(200)]
        data = b"\n".join(rows) + b"\n"
        args = SimpleNamespace(bucket="b", chunk_mb=0, workers=4)
        stats = {"requests": 0, "retries": 0, "timeouts": 0, "bytes": 0, "max_chunk": 0}
        got = list(cp.iter_record_bytes_ranged(args, _FakeS3(data), "k", len(data), stats,
                                              chunk_bytes=97, workers=4))
        self.assertEqual(len(got), len(rows))
        # offset 必须是真实字节偏移，raw 必须逐字节等于原行
        expect_off = 0
        for (off, raw), row in zip(got, rows):
            self.assertEqual(off, expect_off)
            self.assertEqual(raw, row + b"\n")
            expect_off += len(raw)
        self.assertEqual(expect_off, len(data))
        self.assertGreater(stats["requests"], 1)          # 确实是分块拉的，不是整文件

    def test_ranged_reader_retries_short_read(self):
        from types import SimpleNamespace
        data = b"a" * 1000
        args = SimpleNamespace(bucket="b", chunk_mb=0, workers=2)
        stats = {"requests": 0, "retries": 0, "timeouts": 0, "bytes": 0, "max_chunk": 0}
        got = list(cp.iter_record_bytes_ranged(args, _FakeS3(data, short_read_at=0), "k", len(data),
                                              stats, chunk_bytes=300, workers=2))
        self.assertEqual(b"".join(raw for _, raw in got), data)
        self.assertGreaterEqual(stats["retries"], 1)
