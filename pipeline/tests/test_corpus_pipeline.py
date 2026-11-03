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
        # 锁定口径：停点在**源文件边界**——第一个文件全部交付后过线即停，
        # 第二个文件一条都不碰（不是记录边界截断）。
        write_dataset(self.src, [make_record(uuid=f"u{i}", n=100, seed=f"s{i}") for i in range(3)])
        key2 = "synthetic-0002.jsonl"
        with open(self.src / key2, "wb") as fh:
            for i in range(3, 6):
                fh.write(json.dumps(make_record(uuid=f"u{i}", n=100, seed=f"s{i}"),
                                    ensure_ascii=False).encode() + b"\n")
        self.run_pipeline("--target-tokens", "1")   # 第一个文件写完就过线
        manifest = json.loads((self.out / "manifest.json").read_text())
        self.assertEqual(manifest["STOP_REASON"], "token_target_reached")
        self.assertEqual(manifest["stop_file"], "synthetic-0001.jsonl")
        self.assertEqual(manifest["delivered_samples"], 3)
        self.assertEqual(manifest["not_delivered"], 3)

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


# ---------------------------------------------------------------- 分段 / 并发 / 交付形态
# 2026-11-03 v2 改造（跨段续跑 + 并发同步读 + 按源文件命名）的回归用例。
# 每条都先红后绿：改造前的实现上必然失败。

import socket  # noqa: E402
import time as _time  # noqa: E402


def write_named(dirpath: Path, name: str, records):
    with open(dirpath / name, "wb") as fh:
        for rec in records:
            fh.write(json.dumps(rec, ensure_ascii=False).encode() + b"\n")
    return name


class TestSegments(_PipelineCase):
    """软时限分段 + 续跑：多次调用必须与一次跑完等价。"""

    def _two_files(self, n_each=3):
        write_named(self.src, "src-0001.jsonl",
                    [make_record(uuid=f"a{i}", n=100, seed=f"a{i}") for i in range(n_each)])
        write_named(self.src, "src-0002.jsonl",
                    [make_record(uuid=f"b{i}", n=100, seed=f"b{i}") for i in range(n_each)])

    def test_many_segments_equal_single_run(self):
        self._two_files()
        ref = self.root / "ref"
        cp.main(["--execute", "--local-dir", str(self.src), "--out-dir", str(ref),
                 "--target-tokens", "1000000000"])

        # 每段只允许推进一条（软时限瞬间过期）：逼出"段间中断 → 续跑"路径
        for _ in range(80):
            cp.main(["--execute", "--local-dir", str(self.src), "--out-dir", str(self.out),
                     "--target-tokens", "1000000000", "--max-seconds", "0.000001"])
            if (self.out / "manifest.json").exists():
                man = json.loads((self.out / "manifest.json").read_text())
                if not man.get("SEGMENT_PAUSED"):
                    break

        man = json.loads((self.out / "manifest.json").read_text())
        self.assertFalse(man.get("SEGMENT_PAUSED"), "分段没有收敛到完成态")
        self.assertEqual(man["STOP_REASON"], "corpus_exhausted")
        self.assertGreater(man["segments"], 5, "用例前提：必须真的分成多段")

        ref_man = json.loads((ref / "manifest.json").read_text())
        self.assertEqual(man["delivered_samples"], ref_man["delivered_samples"])
        self.assertEqual(man["delivered_tokens"], ref_man["delivered_tokens"])
        self.assertEqual(man["delivered_records"], ref_man["delivered_records"])
        for shard in man["shards"]:
            self.assertEqual((self.out / shard["file"]).read_bytes(),
                             (ref / shard["file"]).read_bytes())

    def test_index_stops_at_line_boundary(self):
        self._two_files()
        for _ in range(6):
            cp.main(["--execute", "--local-dir", str(self.src), "--out-dir", str(self.out),
                     "--phase", "index", "--max-seconds", "0.000001"])
        with open(self.out / "index.jsonl", "rb") as fh:
            data = fh.read()
        self.assertTrue(data.endswith(b"\n"), "暂停点必须落在完整行边界")
        rows = [json.loads(ln) for ln in data.splitlines() if ln.strip()]
        self.assertEqual(len(rows), 6)
        self.assertEqual(len({r["id"] for r in rows}), 6, "续跑不得重复索引同一条记录")
        offsets = {}
        for row in rows:
            offsets.setdefault(row["key"], []).append(row["off"])
        for key, offs in offsets.items():
            self.assertEqual(len(offs), len(set(offs)), f"{key} 的偏移重复")

    def test_index_truncates_half_written_tail(self):
        self._two_files()
        cp.main(["--execute", "--local-dir", str(self.src), "--out-dir", str(self.out),
                 "--phase", "index"])
        before = (self.out / "index.jsonl").stat().st_size
        with open(self.out / "index.jsonl", "ab") as fh:
            fh.write(b'{"id": "half-line-from-a-crash')
        cp.main(["--execute", "--local-dir", str(self.src), "--out-dir", str(self.out),
                 "--phase", "index"])
        self.assertEqual((self.out / "index.jsonl").stat().st_size, before)
        rows = [json.loads(ln) for ln in (self.out / "index.jsonl").read_text().splitlines()]
        self.assertEqual(len(rows), 6)


    def test_scan_reports_progress_so_lock_heartbeat_stays_fresh(self):
        """长扫描必须在扫描中上报进度，否则 20 GiB 单文件会被误判成死锁而遭接管。"""
        write_named(self.src, "long-0001.jsonl",
                    [make_record(uuid=f"u{i}", n=100, seed=f"s{i}") for i in range(450)])
        args = cp.build_parser(["--execute", "--local-dir", str(self.src),
                                "--out-dir", str(self.out)]).parse_args(
            ["--execute", "--local-dir", str(self.src), "--out-dir", str(self.out)])
        beats = []
        res = cp._scan_file(args, None, "long-0001.jsonl", 0, 0, cp.SegmentBudget(0),
                            on_progress=lambda: beats.append(1))
        self.assertEqual(res["scanned"], 450)
        self.assertTrue(res["eof"])
        self.assertGreaterEqual(len(beats), 2, "每 200 条至少上报一次进度")

    def test_index_gate_survives_later_segments(self):
        """闸门一旦触发，后续分段不得把闸门清掉继续拉新文件（否则会白下整桶）。"""
        for i in range(3):
            write_named(self.src, f"src-{i:04d}.jsonl",
                        [make_record(uuid=f"g{i}", n=100, seed=f"g{i}")])
        for _ in range(3):
            cp.main(["--execute", "--local-dir", str(self.src), "--out-dir", str(self.out),
                     "--target-tokens", "1", "--index-connections", "1"])
        man = json.loads((self.out / "manifest.json").read_text())
        self.assertEqual(man["index"]["files_done"], 1, "闸门触发后不允许再索引新文件")
        self.assertEqual(man["STOP_REASON"], "token_target_reached")
        self.assertEqual(man["stop_file"], "src-0000.jsonl")
        self.assertEqual({s["source_key"] for s in man["shards"]}, {"src-0000.jsonl"})


class TestLock(_PipelineCase):
    def test_fresh_lock_blocks_second_instance(self):
        write_named(self.src, "src-0001.jsonl", [make_record(uuid="u1", n=100)])
        self.out.mkdir(parents=True, exist_ok=True)
        (self.out / "run.lock").write_text(json.dumps({
            "pid": os.getpid(), "host": socket.gethostname(), "heartbeat": _time.time()}))
        rc = cp.main(["--execute", "--local-dir", str(self.src), "--out-dir", str(self.out)])
        self.assertEqual(rc, 0)
        self.assertFalse((self.out / "state.json").exists(), "锁被占时不允许写任何进度")
        self.assertFalse((self.out / "index.jsonl").exists())

    def test_stale_lock_is_taken_over(self):
        write_named(self.src, "src-0001.jsonl", [make_record(uuid="u1", n=100)])
        self.out.mkdir(parents=True, exist_ok=True)
        (self.out / "run.lock").write_text(json.dumps({
            "pid": 999999, "host": "old-host", "heartbeat": _time.time() - 3600}))
        cp.main(["--execute", "--local-dir", str(self.src), "--out-dir", str(self.out)])
        self.assertTrue((self.out / "manifest.json").exists(), "过期锁必须被接管")
        self.assertFalse((self.out / "run.lock").exists(), "跑完要释放锁")


class TestConcurrencyEquivalence(_PipelineCase):
    """串行 vs 并发必须语义等价：同一条记录集合、同样的交付字节。"""

    def _four_files(self):
        for i in range(4):
            write_named(self.src, f"src-{i:04d}.jsonl",
                        [make_record(uuid=f"u{i}-{j}", n=100, seed=f"s{i}-{j}") for j in range(3)])

    def test_index_and_emit_serial_vs_concurrent(self):
        self._four_files()
        serial = self.root / "serial"
        parallel = self.root / "parallel"
        for out, conn, pre in ((serial, "1", "1"), (parallel, "4", "4")):
            cp.main(["--execute", "--local-dir", str(self.src), "--out-dir", str(out),
                     "--target-tokens", "1000000000",
                     "--index-connections", conn, "--emit-prefetch", pre])
        a = [json.loads(ln) for ln in (serial / "index.jsonl").read_text().splitlines()]
        b = [json.loads(ln) for ln in (parallel / "index.jsonl").read_text().splitlines()]
        self.assertEqual(sorted(a, key=lambda r: r["id"]), sorted(b, key=lambda r: r["id"]),
                         "并发与串行索引出的记录集合/顺序值必须一致")

        ma = json.loads((serial / "manifest.json").read_text())
        mb = json.loads((parallel / "manifest.json").read_text())
        self.assertEqual(ma["delivered_samples"], mb["delivered_samples"])
        self.assertEqual(ma["delivered_tokens"], mb["delivered_tokens"])
        names_a = sorted(s["file"] for s in ma["shards"])
        names_b = sorted(s["file"] for s in mb["shards"])
        self.assertEqual(names_a, names_b)
        for name in names_a:
            self.assertEqual((serial / name).read_bytes(), (parallel / name).read_bytes())


class TestDeliveryNaming(_PipelineCase):
    def test_artifacts_named_after_source(self):
        write_named(self.src, "gpt-6-astra-0009-20261001.jsonl",
                    [make_record(uuid="u1", n=100, seed="a")])
        write_named(self.src, "gpt-6-astra-0010-20261001.jsonl",
                    [make_record(uuid="u2", n=100, seed="b")])
        self.run_pipeline("--target-tokens", "1000000000")
        man = self.manifest()
        names = sorted(s["file"] for s in man["shards"])
        self.assertEqual(names, ["gpt-6-astra-0009-20261001.jsonl",
                                 "gpt-6-astra-0010-20261001.jsonl"])
        self.assertFalse(any(n.startswith("deliver-") for n in names))

    def test_parts_when_single_source_exceeds_shard_bytes(self):
        write_named(self.src, "big-0001.jsonl",
                    [make_record(uuid=f"u{i}", n=100, seed=f"s{i}") for i in range(6)])
        self.run_pipeline("--target-tokens", "1000000000", "--shard-bytes", "4000")
        man = self.manifest()
        names = [s["file"] for s in man["shards"]]
        self.assertGreater(len(names), 1)
        for name in names:
            self.assertRegex(name, r"^big-0001\.part-\d{4}\.jsonl$")
        parts = sorted(int(name.split("part-")[1].split(".")[0]) for name in names)
        self.assertEqual(parts, list(range(1, len(names) + 1)), "part 编号必须从 1 连续")
        self.assertEqual(sum(s["samples"] for s in man["shards"]), 6)


class TestS3RangeResume(_PipelineCase):
    """S3 分支：分段续跑必须从 next_offset 发 Range GET，而不是从头再流一遍。"""

    class _FakeClient:
        def __init__(self, payload: bytes):
            self.payload = payload
            self.calls = []

        def get_object(self, **kwargs):
            import io
            self.calls.append(kwargs)
            data = self.payload
            rng = kwargs.get("Range")
            if rng:
                start = int(rng.split("=")[1].rstrip("-"))
                data = data[start:]
            return {"Body": io.BytesIO(data)}

    def test_resume_uses_range_from_next_offset(self):
        import io  # noqa: F401
        import unittest.mock as mock

        payload = b"".join(
            json.dumps(make_record(uuid=f"u{i}", n=100, seed=f"s{i}"), ensure_ascii=False).encode() + b"\n"
            for i in range(6)
        )
        fake = self._FakeClient(payload)
        with mock.patch.object(cp, "make_client", lambda args: fake):
            for _ in range(12):
                cp.main(["--execute", "--keys", "remote-0001.jsonl",
                         "--out-dir", str(self.out), "--phase", "index",
                         "--max-seconds", "0.000001"])
                state = json.loads((self.out / "state.json").read_text())
                if state["index"]["done"]:
                    break

        self.assertEqual([c.get("Range") for c in fake.calls if c.get("Range") is None][:1], [None])
        ranges = [c["Range"] for c in fake.calls if c.get("Range")]
        self.assertTrue(ranges, "续跑必须用 Range GET")
        for rng in ranges:
            start = int(rng.split("=")[1].rstrip("-"))
            self.assertGreater(start, 0)
        rows = [json.loads(ln) for ln in (self.out / "index.jsonl").read_text().splitlines()]
        self.assertEqual(len(rows), 6)
        self.assertEqual(len({r["id"] for r in rows}), 6)


class TestSessionTieBreak(_PipelineCase):
    """并列规则必须全序：messages 最长 → timestamp 较新 → 原下标较小。

    这条是契约缺口（合成数据造不出 tie，只有真实数据会撞）：
    开发侧按 timestamp 选，验收侧曾按序列指纹选，1B 规模下 crosscheck 会持续误报。
    """

    def test_same_count_prefers_newer_timestamp(self):
        old = index_item(make_record(uuid="tie", sid_suffix="a", n=100, seed="x",
                                     ts="2026-01-01T00:00:00Z"), off=1)
        new = index_item(make_record(uuid="tie", sid_suffix="b", n=100, seed="y",
                                     ts="2026-02-01T00:00:00Z"), off=2, order=1)
        kept, removed = cp.session_dedup([old, new])
        self.assertEqual(ids(kept), ["k.jsonl#2"])
        self.assertEqual(ids(removed), ["k.jsonl#1"])

    def test_full_tie_keeps_lower_index(self):
        a = index_item(make_record(uuid="tie", sid_suffix="a", n=100, seed="x",
                                   ts="2026-01-01T00:00:00Z"), off=1)
        b = index_item(make_record(uuid="tie", sid_suffix="b", n=100, seed="y",
                                   ts="2026-01-01T00:00:00Z"), off=2, order=1)
        kept, _ = cp.session_dedup([a, b])
        self.assertEqual(ids(kept), ["k.jsonl#1"], "同长同时间的完全并列必须取原下标小的")

    def test_longer_wins_over_newer(self):
        longer = index_item(make_record(uuid="tie", sid_suffix="a", n=120, seed="x",
                                        ts="2026-01-01T00:00:00Z"), off=1)
        newer = index_item(make_record(uuid="tie", sid_suffix="b", n=100, seed="y",
                                       ts="2026-12-01T00:00:00Z"), off=2, order=1)
        kept, _ = cp.session_dedup([longer, newer])
        self.assertEqual(ids(kept), ["k.jsonl#1"])

    def test_order_independent(self):
        a = index_item(make_record(uuid="tie", sid_suffix="a", n=100, seed="x",
                                   ts="2026-01-01T00:00:00Z"), off=1)
        b = index_item(make_record(uuid="tie", sid_suffix="b", n=100, seed="y",
                                   ts="2026-02-01T00:00:00Z"), off=2, order=1)
        self.assertEqual(ids(cp.session_dedup([a, b])[0]), ids(cp.session_dedup([b, a])[0]),
                         "去重结果不能依赖输入顺序（分段/并发下顺序会变）")
