"""断流续读回归：连接被掐断时 index 必须自己续上，而不是整段 abort。

今天真实遇到两种形态（B2 单流读一个 25 GiB 对象）：
  1. 读到一半连接断掉（`IncompleteReadError`）；
  2. 断点正好落在半行上。

判据：
  * 续读后**行数与内容**与「一次读通」完全一致——不重复、不遗漏；
  * 每个 (offset, raw) 的 offset 仍等于该行在原对象里的字节起点，
    这样 `state.json` 的 `partial` 记账才能和已有索引对上；
  * 半行被丢弃并从行首重读，绝不把半截 JSON 当记录产出去；
  * 鉴权类错误不被吞掉，连续零进展的重试有上限。
"""

from __future__ import annotations

import io
import json
import sys
import unittest
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline import corpus_pipeline as cp  # noqa: E402
from botocore.exceptions import IncompleteReadError  # noqa: E402

LINES = [json.dumps({"n": i, "pad": "x" * (i * 3 + 1)}, ensure_ascii=False).encode() + b"\n" for i in range(6)]
PAYLOAD = b"".join(LINES)
LINE_STARTS = [sum(len(x) for x in LINES[:i]) for i in range(len(LINES))]


class _CutBody(io.RawIOBase):
    """在相对偏移 cut_at 处抛 IncompleteReadError 的流。"""

    def __init__(self, data: bytes, cut_at: int | None):
        self._buf = io.BytesIO(data)
        self._cut = cut_at
        self._fired = False

    def readable(self) -> bool:
        return True

    def readinto(self, buf) -> int:
        pos = self._buf.tell()
        if self._cut is not None and not self._fired and pos >= self._cut:
            self._fired = True
            raise IncompleteReadError(actual_bytes=pos, expected_bytes=len(self._buf.getvalue()))
        want = len(buf)
        if self._cut is not None and not self._fired:
            want = min(want, self._cut - pos)
        chunk = self._buf.read(want)
        buf[: len(chunk)] = chunk
        return len(chunk)


class _FakeS3:
    """最小 S3 桩：支持 Range，按 cuts 列表依次在绝对位置掐断。"""

    def __init__(self, data: bytes, cuts: list[int | None]):
        self.data = data
        self.cuts = list(cuts)
        self.calls: list[int] = []

    def get_object(self, Bucket=None, Key=None, Range=None):  # noqa: N803
        start = 0
        if Range:
            start = int(str(Range).split("=")[1].rstrip("-"))
        self.calls.append(start)
        cut = self.cuts.pop(0) if self.cuts else None
        rel = None if cut is None else max(0, cut - start)
        chunk = self.data[start:]
        return {
            "Body": _CutBody(chunk, rel),
            "ContentRange": f"bytes {start}-{len(self.data) - 1}/{len(self.data)}",
            "ContentLength": len(chunk),
        }


def _args(local_dir=None):
    return SimpleNamespace(bucket="b", local_dir=local_dir)


def _old_reader(args, client, key):
    """旧实现的等价物（4 行），用来证明「不修就会抛」。"""
    body = client.get_object(Bucket=args.bucket, Key=key)["Body"]
    stream = io.BufferedReader(body, buffer_size=1 << 22)
    offset = 0
    out = []
    try:
        for raw in stream:
            out.append((offset, raw))
            offset += len(raw)
    finally:
        stream.close()
    return out


class TestReadResume(unittest.TestCase):
    def test_old_reader_aborts_when_stream_is_cut(self):
        """红：旧读法遇到断流直接抛，整段扫描作废。"""
        client = _FakeS3(PAYLOAD, cuts=[LINE_STARTS[3]])
        with self.assertRaises(IncompleteReadError):
            _old_reader(_args(), client, "k")

    def test_resume_when_cut_on_a_line_boundary(self):
        """绿：断在行边界，续读后行数/内容/offset 全对。"""
        client = _FakeS3(PAYLOAD, cuts=[LINE_STARTS[3]])
        got = list(cp._iter_file_lines(_args(), client, "k"))
        self.assertEqual([raw for _, raw in got], LINES)
        self.assertEqual([off for off, _ in got], LINE_STARTS)
        self.assertEqual(len(client.calls), 2)          # 第二次是 Range 续读
        self.assertEqual(client.calls[1], LINE_STARTS[3])

    def test_resume_when_cut_mid_line(self):
        """绿：断在半行上——半行丢弃、从行首重读，绝不输出半截 JSON。"""
        cut = LINE_STARTS[2] + max(1, len(LINES[2]) // 2)
        client = _FakeS3(PAYLOAD, cuts=[cut])
        got = list(cp._iter_file_lines(_args(), client, "k"))
        self.assertEqual([raw for _, raw in got], LINES)
        self.assertEqual([off for off, _ in got], LINE_STARTS)
        for _off, raw in got:
            json.loads(raw)                              # 每行都是完整 JSON

    def test_multiple_cuts_accumulate_correctly(self):
        """绿：连断三次仍能读完，且不重复不遗漏。"""
        client = _FakeS3(PAYLOAD, cuts=[LINE_STARTS[1], LINE_STARTS[3] + 5, LINE_STARTS[4]])
        got = list(cp._iter_file_lines(_args(), client, "k"))
        self.assertEqual([raw for _, raw in got], LINES)
        self.assertEqual([off for off, _ in got], LINE_STARTS)
        self.assertEqual(len(client.calls), 4)

    def test_offsets_align_with_partial_accounting(self):
        """绿：最后一条的 offset+len 等于对象大小，可与 state.json 的 partial 对账。"""
        client = _FakeS3(PAYLOAD, cuts=[LINE_STARTS[2] + 2])
        got = list(cp._iter_file_lines(_args(), client, "k"))
        last_off, last_raw = got[-1]
        self.assertEqual(last_off + len(last_raw), len(PAYLOAD))

    def test_gives_up_after_repeated_zero_progress(self):
        """连续零进展（例如每次刚连上就断）必须报错，不能无限重试。"""
        client = _FakeS3(PAYLOAD, cuts=[0, 0, 0, 0, 0, 0, 0])
        with self.assertRaises(RuntimeError):
            list(cp._iter_file_lines(_args(), client, "k"))

    def test_auth_error_is_not_swallowed(self):
        """鉴权类异常必须原样抛出，不能被当成断流重试。"""
        class Boom(Exception):
            pass

        class Client:
            def get_object(self, **kw):
                raise Boom("AccessDenied")

        with self.assertRaises(Boom):
            list(cp._iter_file_lines(_args(), Client(), "k"))

    def test_local_dir_unchanged(self):
        """本地模式不受影响（不走网络分支）。"""
        import tempfile

        with tempfile.TemporaryDirectory() as d:
            p = Path(d) / "x.jsonl"
            p.write_bytes(PAYLOAD)
            got = list(cp._iter_file_lines(_args(local_dir=d), None, "x.jsonl"))
            self.assertEqual([raw for _, raw in got], LINES)
            self.assertEqual([off for off, _ in got], LINE_STARTS)


if __name__ == "__main__":
    unittest.main()


class _Budget:
    def expired(self):
        return False


class TestTallyCompleteness(unittest.TestCase):
    """断流续跑后「计数」也不能丢：scanned == 各过滤之和 + passed，且 passed == index 行数。

    HK 实测 sol-0005/0006/0007 出现 passed 比 index 行少 409/176/316——行落盘了、tally 没记上。
    这个用例把「中途断流 → 续跑完成」整条链路跑一遍，断言两个恒等式都成立。
    """

    def _payload(self):
        import json as _json

        def rec(i, *, rs=True, enc=True, n=100):
            msgs = [{"role": "user" if k % 2 == 0 else "assistant",
                     "content": ([{"type": "text", "text": f"t{i}-{k}"}]
                                 + ([{"type": "encrypted_content", "encrypted_content": f"e{i}-{k}"}] if enc else []))}
                    for k in range(n)]
            out = [{"id": f"rs_{i:03d}"}] if rs else [{"id": "not_rs"}]
            return {"session_id": f"uuid-{i:03d}~p", "request": {"messages": msgs},
                    "response": {"response_data": {"output": out}}}

        rows = [rec(0), rec(1), rec(2, rs=False), rec(3, enc=False), rec(4, n=40), rec(5), rec(6)]
        return b"".join((__import__("json").dumps(r, ensure_ascii=False) + "\n").encode() for r in rows), rows

    def test_counts_survive_a_cut(self):
        import json as _json

        payload, rows = self._payload()
        starts = []
        off = 0
        for r in rows:
            starts.append(off)
            off += len(_json.dumps(r, ensure_ascii=False).encode()) + 1

        client = _FakeS3(payload, cuts=[starts[4] + 7])     # 断在第 5 条中间
        args = SimpleNamespace(bucket="b", local_dir=None, max_records=0, min_messages=100)
        res = cp._scan_file(args, client, "k.jsonl", 0, 7, _Budget())

        filters = sum(res["reasons"].values())
        self.assertEqual(res["scanned"], filters + res["passed"],
                         f"scanned {res['scanned']} != filters {filters} + passed {res['passed']}")
        self.assertEqual(res["passed"], len(res["items"]),
                         f"passed {res['passed']} != items {len(res['items'])}")
        # 期望：7 条里 0/1/5/6 通过（2 无 rs_id、3 无密文、4 太短）
        self.assertEqual(res["passed"], 4)
        self.assertEqual(res["scanned"], 7)
        self.assertEqual(res["eof"], True)
        self.assertEqual(res["next_offset"], len(payload))
        # index 行 = 一条一个 (id, off)，不重复不遗漏
        ids = [it["id"] for it in res["items"]]
        self.assertEqual(len(ids), len(set(ids)))
