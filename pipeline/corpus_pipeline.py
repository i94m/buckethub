#!/usr/bin/env python3
"""SFT 语料导出主流水线（B2 → 1B token jsonl 分包）。

口径（与 P0 表一致，锁定版）：
  1. 「100 轮」= request.messages 条数 >= 100
  2. encrypted_content = request.messages[].content[] 中 type == "encrypted_content" 且非空的 part
  3. rs_id = response.response_data.output[].id 以 "rs_" 开头
  4. session 去重：按 session_id 中 "~" 之前的 uuid 段分组，保留 messages 最长的一条
  5. token：tiktoken o200k_base / encode_ordinary，逐条 message 的紧凑 JSON 单独计数后求和（含密文）
  6. 分片：按体积 ~500 MB；单条超过 128k token 按窗口切分

三段式：
  index  —— 流式读取源对象，过滤 + 计算 messages 成员 hash 序列 + 记录字节区间；不落原文件
  plan   —— session 去重 → 前缀去重（排序 + 相邻比对 O(n log n)），产出保留清单与账本
  emit   —— 只按字节区间 Range GET 保留样本 → tokenize → 切窗 → 写分片，累加器过 1B 即停

跨段执行（两天无人值守的唯一可行形态）：
  * ``--max-seconds`` 给每次调用一个软时限，index 在完整行边界、emit 在记录组边界让出；
  * index 进度按 ``(key, next_offset)`` 持久化，``index.jsonl`` 按已 fsync 的字节长度截尾，
    续跑不会重复也不会解析到半行；
  * ``run.lock`` 是单实例锁：心跳还活着的实例在跑，第二个实例立刻退出，绝不并发写同一 out-dir；
  * emit 的交付量一律从 ``emit.journal`` 重算，段与段之间不依赖内存变量。

交付形态（用户锁定口径）：
  产物按源文件命名（``<源文件主干>.jsonl``），单源超 ``--shard-bytes`` 时切成
  ``<主干>.part-0001.jsonl`` …；**停点在源文件边界**：累计交付量过目标后，把当前源文件
  剩下的记录全部处理完再停，后续源文件一条都不碰。

默认 dry-run：不带 --execute 只打印计划，不连网、不写盘。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import socket
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

DEFAULT_ENDPOINT = "https://s3.us-east-005.backblazeb2.com"
DEFAULT_BUCKET = "tvk-data-1012-a"
DEFAULT_PREFIX = "Gpt-2026-10-18/"
MIN_MESSAGES = 100
MAX_SAMPLE_TOKENS = 128_000
TARGET_TOKENS = 1_000_000_000
SHARD_BYTES = 500 * 1024 * 1024
# 实测 0.1015 token/byte（astra-0009 全量 7295 条），仅用于 index 阶段的提前止损预估
DEFAULT_TOKENS_PER_BYTE = 0.1015
# 预估过线后就不再拉新源文件（v2 裁决：1.1 倍；1.3 倍会白下 ~100 GB 且永远进不了交付）
INDEX_STOP_SAFETY = 1.10
# order = 文件排名 × 该步长 + 文件内字节偏移；保证跨段/并发/续跑都稳定且不冲突（1 TiB 内安全）
ORDER_STRIDE = 1 << 40
LOCK_TTL_SECONDS = 300
LOCK_EXIT_OK = 0


# --------------------------------------------------------------------- 纯函数

def canon(obj) -> str:
    """稳定序列化：键排序、无多余空白；非 ASCII 直出，避免转义差异。"""
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def member_hash(message) -> bytes:
    return hashlib.sha1(canon(message).encode("utf-8", "surrogatepass")).digest()[:8]


def record_seq(messages) -> bytes:
    """messages 逐成员的 hash 序列（成员层面，不是文本层面）。"""
    return b"".join(member_hash(m) for m in messages)


def session_uuid(session_id) -> str:
    """session_id 形如 <uuid>~u<hash>，去重只看 ~ 之前的 uuid 段。"""
    if not session_id:
        return ""
    return str(session_id).split("~", 1)[0]


def messages_of(record):
    msgs = (record.get("request") or {}).get("messages")
    return msgs if isinstance(msgs, list) else []


def has_rs_id(record) -> bool:
    out = ((record.get("response") or {}).get("response_data") or {}).get("output") or []
    return any(isinstance(it, dict) and str(it.get("id", "")).startswith("rs_") for it in out)


IMAGE_PART_TYPES = ("input_image", "image_url", "input_image_url")


def has_image(messages) -> bool:
    """A1 口径：任一 message 的 content[] 含图片 part 即整条剔除。

    判据与验收侧同一份：``type`` 命中 ``input_image`` / ``image_url``，
    或者 part 里带 ``image_url`` 字段（有些上游把图片塞进别的 type 里）。
    """
    for m in messages:
        content = m.get("content") if isinstance(m, dict) else None
        if not isinstance(content, list):
            continue
        for part in content:
            if not isinstance(part, dict):
                continue
            if part.get("type") in IMAGE_PART_TYPES or "image_url" in part:
                return True
    return False


def has_encrypted_content(messages) -> bool:
    for m in messages:
        content = m.get("content") if isinstance(m, dict) else None
        if not isinstance(content, list):
            continue
        for part in content:
            if isinstance(part, dict) and part.get("type") == "encrypted_content" and part.get("encrypted_content"):
                return True
    return False


def filter_reason(record, min_messages=MIN_MESSAGES):
    """返回 None 表示通过，否则返回被丢弃的原因（同时用于漏斗统计）。"""
    if not isinstance(record, dict):
        return "parse_error"
    if not session_uuid(record.get("session_id")):
        return "no_session_id"
    messages = messages_of(record)
    if not has_rs_id(record):
        return "no_rs_id"
    if not has_encrypted_content(messages):
        return "no_encrypted_content"
    if len(messages) < min_messages:
        return "too_short"
    # A1：整条丢弃含图记录。**放在最后一道**，这样 has_image 的计数恰好是
    # "本来能交付、只因为含图被丢"的条数；含图且缺密文/太短的记录仍记在更前面那道。
    if has_image(messages):
        return "has_image"
    return None


def session_dedup(items):
    """同 uuid 的去重是全序、确定性的：

    **messages 最长 → timestamp 较新 → 原下标较小**

    第三条不能用「输入列表里谁先出现」糊弄——分段/并发下 index 的追加顺序会变，
    同一份数据两次跑可能选出不同记录。这里的「原下标」取稳定的 ``order`` 字段
    （= 文件排名 × 2^40 + 文件内字节偏移），与分段/并发无关；老数据没有该字段时
    退回输入下标。
    """
    groups = {}
    for idx, it in enumerate(items):
        groups.setdefault(it["uuid"], []).append(idx)

    winners = set()
    removed_idx = []
    for members in groups.values():
        if len(members) == 1:
            winners.add(members[0])
            continue
        # 三次稳定排序拼出 (最长, timestamp 新→旧, 稳定 order 小→大)
        ranked = sorted(members, key=lambda i: items[i].get("order", i))
        ranked.sort(key=lambda i: items[i].get("timestamp") or "", reverse=True)
        ranked.sort(key=lambda i: -int(items[i]["n_msgs"]))
        winners.add(ranked[0])
        removed_idx.extend(i for i in members if i != ranked[0])

    removed_idx.sort()
    kept = [it for i, it in enumerate(items) if i in winners]
    removed = [items[i] for i in removed_idx]
    return kept, removed


def prefix_dedup(items):
    """严格前缀去重：字典序排序后相邻比对，O(n log n)。

    排序后，若序列 A 是某条更长序列 B 的严格前缀，则 B（或 B 所在前缀块的第一个元素）
    必然紧邻 A。完全相同的序列只保留输入顺序最前的一条。
    """
    ordered = sorted(items, key=lambda it: (it["seq"], it.get("order", 0)))
    drop = set()
    exact = 0
    for a, b in zip(ordered, ordered[1:]):
        if a["seq"] == b["seq"]:
            exact += 1
            drop.add(a["id"])
        elif len(b["seq"]) > len(a["seq"]) and b["seq"].startswith(a["seq"]):
            drop.add(a["id"])
    kept = [it for it in items if it["id"] not in drop]
    removed = [it for it in items if it["id"] in drop]
    return kept, removed, exact


def plan_dedup(items):
    """先 session 去重，再前缀去重；两步移除量分开返回。"""
    s_kept, s_removed = session_dedup(items)
    p_kept, p_removed, exact = prefix_dedup(s_kept)
    return {
        "kept": p_kept,
        "session_removed": s_removed,
        "prefix_removed": p_removed,
        "exact_dup": exact,
        "strict_prefix": len(p_removed) - exact,
    }


def count_message_tokens(message, encoder) -> int:
    return len(encoder.encode_ordinary(canon(message)))


def count_tokens(messages, encoder) -> int:
    return sum(count_message_tokens(m, encoder) for m in messages)


def split_windows(messages, encoder, max_tokens=MAX_SAMPLE_TOKENS):
    """按累计 token 切成连续窗口；单条 message 超限时自成一个窗口（会在账单里计 oversize）。"""
    counts = [count_message_tokens(m, encoder) for m in messages]
    if sum(counts) <= max_tokens:
        return [list(messages)], [sum(counts)], 0
    windows, toks, cur, cur_tok, oversize = [], [], [], 0, 0
    for m, c in zip(messages, counts):
        if cur and cur_tok + c > max_tokens:
            windows.append(cur)
            toks.append(cur_tok)
            cur, cur_tok = [], 0
        cur.append(m)
        cur_tok += c
        if cur_tok > max_tokens:      # 单条本身超限：独占一个窗口
            oversize += 1
            windows.append(cur)
            toks.append(cur_tok)
            cur, cur_tok = [], 0
    if cur:
        windows.append(cur)
        toks.append(cur_tok)
    return windows, toks, oversize


# --------------------------------------------------------------------- 账本

class Ledger:
    """条数对账：原始条数 = 过滤各原因之和；通过 = session 移除 + 前缀移除 + 保留。"""

    def __init__(self):
        self.files = {}
        self.meta = {}

    def file(self, key):
        return self.files.setdefault(key, {
            "scanned": 0, "parse_error": 0, "no_session_id": 0, "no_rs_id": 0,
            "no_encrypted_content": 0, "too_short": 0, "passed": 0,
            "session_removed": 0, "prefix_removed": 0, "exact_dup": 0,
            "delivered_records": 0, "delivered_windows": 0,
        })

    def tally_filter(self, key, reason):
        row = self.file(key)
        row["scanned"] += 1
        if reason is None:
            row["passed"] += 1
        else:
            row[f"reason_{reason}"] = row.get(f"reason_{reason}", 0) + 1
            row[reason] = row.get(reason, 0) + 1

    def totals(self):
        keys = ["scanned", "parse_error", "no_session_id", "no_rs_id", "no_encrypted_content",
                "too_short", "has_image", "passed", "session_removed", "prefix_removed",
                "exact_dup", "delivered_records", "delivered_windows", "delivered_tokens",
                "kept_records", "not_delivered"]
        out = {k: sum(f.get(k, 0) for f in self.files.values()) for k in keys}
        out.update(self.meta)
        out["filter_closure_ok"] = (
            out["scanned"] - out["passed"]
            == out["parse_error"] + out["no_session_id"] + out["no_rs_id"]
            + out["no_encrypted_content"] + out["too_short"] + out["has_image"]
        )
        return out


def finalize_ledger(ledger) -> None:
    """逐文件补齐验收侧对账所需的派生字段（不做任何口径放宽）。

    accept 侧的对账恒等式是 ``原始 = 各移除步骤 + 交付 + 停止后未交付``。开发侧原来只写了
    交付条数，停止点之后的文件（``delivered=0``）和零保留文件都会让该等式两边对不上，
    被 judge 成「交付计数没有回写账本」。这里补三个派生字段：

    * ``kept_records``  —— 去重后应保留条数 = passed − session_removed − prefix_removed
    * ``not_delivered`` —— 停止点之后没交付的条数 = kept − delivered
    * ``status``        —— ``delivered`` / ``zero_kept`` / ``not_reached``（防御性另有 ``partial``）

    注意：**绝不能写 ``stop_cut``**——accept 把它当作 ``not_delivered`` 的别名，
    两边同时写会双重计数（M4 已实证）。
    """
    for row in ledger.files.values():
        row.pop("stop_cut", None)
        kept = max(0, int(row.get("passed", 0))
                   - int(row.get("session_removed", 0))
                   - int(row.get("prefix_removed", 0)))
        delivered = int(row.get("delivered_records", 0))
        row["kept_records"] = kept
        row["not_delivered"] = max(0, kept - delivered)
        if kept == 0:
            row["status"] = "zero_kept"
        elif delivered == 0:
            row["status"] = "not_reached"
        elif delivered >= kept:
            row["status"] = "delivered"
        else:
            row["status"] = "partial"


# --------------------------------------------------------------------- 源数据

def make_client(args):
    import boto3
    from botocore.config import Config

    kw = dict(
        endpoint_url=args.endpoint,
        region_name=args.region,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                      retries={"max_attempts": 5}, max_pool_connections=16),
    )
    ak = os.environ.get("B2_KEY_ID") or os.environ.get("AWS_ACCESS_KEY_ID")
    sk = os.environ.get("B2_APP_KEY") or os.environ.get("AWS_SECRET_ACCESS_KEY")
    if ak and sk:
        kw.update(aws_access_key_id=ak, aws_secret_access_key=sk)
    return boto3.client("s3", **kw)


def list_sources(args, client):
    """返回 [(key, size)]。来源三选一：--keys / --keys-file / 前缀列举。"""
    if args.keys:
        return [(k, None) for k in args.keys]
    if args.keys_file:
        keys = [ln.strip() for ln in Path(args.keys_file).read_text().splitlines() if ln.strip()]
        return [(k, None) for k in keys]
    if args.local_dir:
        base = Path(args.local_dir)
        return [(str(p.name), p.stat().st_size) for p in sorted(base.glob("*.jsonl"))]
    if client is None:
        raise SystemExit("dry-run 下需要 --keys/--keys-file 才能知道要处理哪些对象（--execute 才会列举前缀）")
    out, token = [], None
    while True:
        kw = dict(Bucket=args.bucket, Prefix=args.prefix, MaxKeys=1000)
        if token:
            kw["ContinuationToken"] = token
        r = client.list_objects_v2(**kw)
        for o in r.get("Contents", []):
            if o["Key"].endswith("/") and o["Size"] == 0:
                continue
            out.append((o["Key"], o["Size"]))
        if not r.get("IsTruncated"):
            break
        token = r["NextContinuationToken"]
    return out


def read_range(args, client, key, offset, length) -> bytes:
    if args.local_dir:
        with open(Path(args.local_dir) / Path(key).name, "rb") as fh:
            fh.seek(offset)
            return fh.read(length)
    r = client.get_object(Bucket=args.bucket, Key=key, Range=f"bytes={offset}-{offset + length - 1}")
    return r["Body"].read()


# --------------------------------------------------------------------- 分段 / 锁

class SegmentBudget:
    """一次调用（一段）的软时限。到点后各阶段在自己的安全点让出。"""

    def __init__(self, max_seconds: float = 0, started_at: float | None = None):
        self.max_seconds = float(max_seconds or 0)
        self.started_at = started_at or time.time()
        self.paused = False

    def expired(self) -> bool:
        if not self.max_seconds:
            return False
        if time.time() - self.started_at >= self.max_seconds:
            self.paused = True
            return True
        return False

    def elapsed(self) -> float:
        return time.time() - self.started_at


class RunLock:
    """out-dir 级单实例锁：心跳新鲜就拒绝第二个实例，心跳过期则接管。

    锁只是防并发写，不是数据真相；真相是 journal / state.json。
    """

    def __init__(self, out_dir: Path, ttl: int = LOCK_TTL_SECONDS):
        self.path = Path(out_dir) / "run.lock"
        self.ttl = ttl
        self.held = False
        self._mutex = threading.Lock()

    def _read(self):
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return None

    def acquire(self) -> bool:
        info = self._read()
        if info:
            hb = float(info.get("heartbeat") or 0)
            age = time.time() - hb if hb else self.ttl + 1
            if age < self.ttl:
                return False
            print(f"[lock] 接管过期锁（{info.get('host')} pid={info.get('pid')}，"
                  f"心跳已过期 {age:.0f}s）")
        self.held = True
        self.heartbeat()
        return True

    def heartbeat(self) -> None:
        if not self.held:
            return
        with self._mutex:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            payload = {"pid": os.getpid(), "host": socket.gethostname(),
                       "heartbeat": time.time()}
            tmp = self.path.with_suffix(".tmp")
            tmp.write_text(json.dumps(payload), encoding="utf-8")
            tmp.replace(self.path)

    def release(self) -> None:
        if not self.held:
            return
        self.held = False
        try:
            self.path.unlink()
        except FileNotFoundError:
            pass


def _truncate_file(path: Path, size: int) -> None:
    if not path.exists():
        return
    if path.stat().st_size > size:
        with open(path, "r+b") as fh:
            fh.truncate(size)
            fh.flush()
            os.fsync(fh.fileno())


def _repair_partial_tail(path: Path) -> int:
    """把末尾不完整的半行截掉，返回修好后的字节数（缺文件返回 0）。"""
    if not path.exists():
        return 0
    size = path.stat().st_size
    if not size:
        return 0
    with open(path, "rb") as fh:
        fh.seek(-1, os.SEEK_END)
        if fh.read(1) == b"\n":
            return size
    keep = 0
    with open(path, "rb") as fh:
        while True:
            chunk = fh.read(1 << 20)
            if not chunk:
                break
            idx = chunk.rfind(b"\n")
            if idx >= 0:
                keep = fh.tell() - len(chunk) + idx + 1
    _truncate_file(path, keep)
    print(f"[index] 截掉不完整的尾行：{size} → {keep} 字节")
    return keep


def source_stem(key: str) -> str:
    """源对象名 → 产物主干名（去目录、去 .jsonl 扩展名）。"""
    name = Path(key).name
    return name[:-6] if name.endswith(".jsonl") else name


def part_name(key: str, part: int, mode: str) -> str:
    stem = source_stem(key)
    if mode == "parts":
        return f"{stem}.part-{part:04d}.jsonl"
    return f"{stem}.jsonl"


# --------------------------------------------------------------------- 状态

class State:
    DEFAULTS = {
        "index": {"done": [], "stop_reason": None, "files": {},
                  "partial": {}, "index_bytes": 0},
        "emit": {"stop_reason": None, "tokens": 0, "total_samples": 0,
                 "stop_after_record": None, "stop_file": None, "files": {}},
        "ledger": None,
        "segments": 0,
    }

    def __init__(self, path: Path):
        self.path = path
        self.data = json.loads(json.dumps(self.DEFAULTS))  # 深拷贝，避免共享可变默认值
        if path.exists():
            stored = json.loads(path.read_text())
            for key, default in self.DEFAULTS.items():
                value = stored.get(key)
                if isinstance(default, dict) and isinstance(value, dict):
                    merged = json.loads(json.dumps(default))
                    merged.update(value)
                    self.data[key] = merged
                elif key in stored:
                    self.data[key] = value

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data, ensure_ascii=False, indent=1))
        tmp.replace(self.path)


# --------------------------------------------------------------------- 三个阶段

_CONTENT_RANGE_TOTAL = re.compile(r"/(\d+)\s*$")


def _is_transient_read_error(exc) -> bool:
    """只把「传输层断流」当可重试，鉴权/参数类错误照旧抛出去。"""
    name = type(exc).__name__
    if name in (
        "IncompleteReadError", "ProtocolError", "ReadTimeoutError",
        "ChunkedEncodingError", "ResponseStreamingError", "ConnectionError",
        "ConnectionResetError", "ConnectionAbortedError", "BrokenPipeError",
        "TimeoutError", "SSLError",
    ):
        return True
    return isinstance(exc, (OSError, EOFError))


def _iter_file_lines(args, client, key, start_offset=0):
    """从 start_offset（必须是行边界）起逐行产出 (offset, raw)。"""
    if args.local_dir:
        with open(Path(args.local_dir) / Path(key).name, "rb") as fh:
            fh.seek(start_offset)
            offset = start_offset
            for raw in fh:
                yield offset, raw
                offset += len(raw)
        return
    import io

    offset = start_offset          # 始终停在「完整行」的边界上
    total = None                   # 对象总字节数（从 ContentRange/ContentLength 取）
    stalls = 0                     # 连续「零进展」的断流次数
    while True:
        kw = dict(Bucket=args.bucket, Key=key)
        if offset:
            kw["Range"] = f"bytes={offset}-"
        resp = client.get_object(**kw)
        m = _CONTENT_RANGE_TOTAL.search(str(resp.get("ContentRange") or ""))
        if m:
            total = int(m.group(1))
        elif offset == 0 and resp.get("ContentLength") is not None:
            total = int(resp["ContentLength"])
        body = resp["Body"]
        stream = io.BufferedReader(body, buffer_size=1 << 22)
        progressed = False
        cut = False
        try:
            while True:
                try:
                    raw = stream.readline()
                except Exception as exc:            # 断流（IncompleteReadError 等）
                    if not _is_transient_read_error(exc):
                        raise
                    cut = True
                    break
                if not raw:                          # 行边界处断掉，或真的 EOF
                    if total is not None and offset < total:
                        cut = True
                    else:
                        return
                    break
                if not raw.endswith(b"\n"):          # 断在半行上
                    if total is not None and offset + len(raw) >= total:
                        # 源对象本身末尾没有换行：按原样交付这一行
                        yield offset, raw
                        offset += len(raw)
                        return
                    cut = True                       # 丢弃半行，从它的起点重读
                    break
                yield offset, raw
                offset += len(raw)
                progressed = True
        finally:
            try:
                stream.close()
            except Exception:
                pass
        if not cut:
            return
        stalls = 0 if progressed else stalls + 1
        if stalls >= 5:
            raise RuntimeError(
                f"{key}: 连续 {stalls} 次断流且零进展，放弃（offset={offset}）")
        # 断点续读：offset 由「最后一个完整行」推出，天然是行边界，
        # 因此与 state.json 的 partial 记账口径一致，重读不会产生重复行。


def _scan_file(args, client, key, start_offset, file_rank, budget, on_progress=None):
    """单个源对象的一次扫描（可在线程里跑）。到软时限就在行边界返回。"""
    items = []
    scanned = 0
    passed = 0
    next_offset = start_offset
    eof = False
    made_progress = False
    reason_counts = {}
    for offset, raw in _iter_file_lines(args, client, key, start_offset):
        if args.max_records and scanned >= args.max_records:
            eof = True
            break
        # 软时限只在"本段已经前进过"之后生效：保证每段至少推进一条，绝不空转死循环
        if made_progress and budget.expired():
            break
        scanned += 1
        made_progress = True
        next_offset = offset + len(raw)
        if on_progress is not None and scanned % 200 == 0:
            on_progress()
        try:
            rec = json.loads(raw)
        except Exception:
            reason_counts["parse_error"] = reason_counts.get("parse_error", 0) + 1
            continue
        reason = filter_reason(rec, args.min_messages)
        if reason is not None:
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
            continue
        msgs = messages_of(rec)
        items.append({
            "id": f"{key}#{offset}",
            "key": key,
            "off": offset,
            "len": len(raw),
            "uuid": session_uuid(rec.get("session_id")),
            "timestamp": rec.get("timestamp") or "",
            "n_msgs": len(msgs),
            "seq": record_seq(msgs).hex(),
            "order": file_rank * ORDER_STRIDE + offset,
        })
        passed += 1
    else:
        eof = True
    return {
        "key": key, "items": items, "scanned": scanned, "passed": passed,
        "next_offset": next_offset, "eof": eof, "reasons": reason_counts,
    }


def phase_index(args, state, ledger, out_dir: Path, budget: SegmentBudget):
    idx_path = out_dir / "index.jsonl"
    idx_state = state.data["index"]
    idx_state.setdefault("done", [])
    idx_state.setdefault("files", {})
    idx_state.setdefault("partial", {})

    # 续跑先对齐 index.jsonl：只保留上次 fsync 记账过的前缀；没有记账过的旧目录则只修尾行
    if idx_state.get("index_bytes"):
        _truncate_file(idx_path, int(idx_state["index_bytes"]))
    else:
        idx_state["index_bytes"] = _repair_partial_tail(idx_path)

    client = None if args.local_dir else make_client(args)
    sources = list_sources(args, client)
    done = set(idx_state["done"])
    todo = [(rank, key, size) for rank, (key, size) in enumerate(sources) if key not in done]
    if todo and idx_state.get("stop_reason") == "token_target_reached":
        gate_target = int(idx_state.get("gate_target") or 0)
        if gate_target and args.target_tokens > gate_target:
            # 只有目标真的提高才重新拉新文件；否则每次分段续跑都会把闸门清掉，等于没有闸门
            idx_state["stop_reason"] = None
            idx_state["gate_target"] = None
            state.save()
        else:
            print("[index] 已到拉取闸门（目标未提高），本段不再拉新文件")
            todo = []
    est_bytes = sum(s or 0 for _, _, s in todo)
    print(f"[index] 源对象 {len(sources)} 个（待处理 {len(todo)}），已知体积 {est_bytes/1024**3:.2f} GiB"
          f"，并发 {args.index_connections}")

    paused = False
    if todo:
        with open(idx_path, "a", encoding="utf-8") as out, \
                ThreadPoolExecutor(max_workers=max(1, args.index_connections)) as pool:
            for start in range(0, len(todo), max(1, args.index_connections)):
                batch = todo[start:start + max(1, args.index_connections)]
                futures = []
                for rank, key, _size in batch:
                    offset = int(idx_state["partial"].get(key, 0))
                    futures.append((key, pool.submit(_scan_file, args, client, key, offset,
                                                     rank, budget, _heartbeat_cb(state))))
                for key, fut in futures:
                    res = fut.result()
                    t0 = idx_state["files"].get(key, {}).get("started_at", time.time())
                    for reason, count in res["reasons"].items():
                        for _ in range(count):
                            ledger.tally_filter(key, reason)
                    for _ in range(res["passed"]):
                        ledger.tally_filter(key, None)
                    for item in res["items"]:
                        out.write(json.dumps(item, ensure_ascii=False) + "\n")
                    # 顺序：分片必须先落盘，账本(字节数/偏移)后记
                    out.flush()
                    os.fsync(out.fileno())
                    idx_state["index_bytes"] = out.tell()
                    if res["eof"]:
                        idx_state["done"].append(key)
                        idx_state["partial"].pop(key, None)
                        row = ledger.file(key)
                        prev = idx_state["files"].get(key, {})
                        idx_state["files"][key] = {
                            "scanned": prev.get("scanned", 0) + res["scanned"],
                            "passed": prev.get("passed", 0) + res["passed"],
                            "elapsed_s": round(time.time() - t0, 1),
                            "next_offset": res["next_offset"], "done": True,
                        }
                        print(f"[index] {key}: 扫描 {res['scanned']} 条，通过 {row['passed']}，"
                              f"{time.time()-t0:.0f}s")
                    else:
                        idx_state["partial"][key] = res["next_offset"]
                        prev = idx_state["files"].get(key, {})
                        idx_state["files"][key] = {
                            "scanned": prev.get("scanned", 0) + res["scanned"],
                            "passed": prev.get("passed", 0) + res["passed"],
                            "next_offset": res["next_offset"], "done": False,
                            "started_at": t0,
                        }
                        print(f"[index] {key}: 本段扫描 {res['scanned']} 条（续跑点 "
                              f"{res['next_offset']} 字节）")
                    state.save()
                    lock_heartbeat(state)
                if state.data["index"].get("stop_reason") == "token_target_reached":
                    break
                # 提前止损：按实测 token/byte 预估保留集，过线就不再拉新文件
                projected = project_kept_tokens(idx_path, args.tokens_per_byte)
                if projected and projected >= args.target_tokens * INDEX_STOP_SAFETY:
                    idx_state["stop_reason"] = "token_target_reached"
                    idx_state["gate_target"] = args.target_tokens
                    state.save()
                    print(f"[index] 预估保留集 {projected/1e9:.3f}B token ≥ 目标×{INDEX_STOP_SAFETY}，"
                          f"停止拉取新文件")
                    break
                if budget.expired():
                    paused = True
                    break
            if budget.expired() and any(not idx_state["files"].get(k, {}).get("done")
                                        for _, k, _ in todo):
                paused = True

    total = _count_lines(idx_path)
    print(f"[index] 完成：index.jsonl {total} 条"
          + ("（本段暂停，待续跑）" if paused else ""))
    state.data["segments"] = int(state.data.get("segments") or 0)
    return total, paused


def lock_heartbeat(state):
    lock = getattr(state, "lock", None)
    if lock is not None:
        lock.heartbeat()


def _heartbeat_cb(state, every: float = 30.0):
    """给长扫描用的心跳回调：最多每 `every` 秒写一次，避免 20 GiB 单文件被误判为死锁。"""
    lock = getattr(state, "lock", None)
    if lock is None:
        return None
    last = [0.0]

    def _beat():
        now = time.time()
        if now - last[0] >= every:
            last[0] = now
            lock.heartbeat()

    return _beat


def _count_lines(path: Path) -> int:
    if not path.exists():
        return 0
    with open(path, encoding="utf-8") as fh:
        return sum(1 for _ in fh)


def project_kept_tokens(idx_path: Path, tokens_per_byte: float) -> int:
    """用已索引的记录做一次去重，按 bytes × token/byte 粗估保留集 token。"""
    if not idx_path.exists():
        return 0
    items = load_index(idx_path)
    if not items:
        return 0
    kept = plan_dedup(items)["kept"]
    total_bytes = sum(it["len"] for it in kept)
    return int(total_bytes * tokens_per_byte)


def load_index(idx_path: Path):
    items = []
    with open(idx_path, encoding="utf-8") as fh:
        for line in fh:
            it = json.loads(line)
            it["seq"] = bytes.fromhex(it["seq"])
            items.append(it)
    return items


def phase_plan(args, state, ledger, out_dir: Path):
    idx_path = out_dir / "index.jsonl"
    items = load_index(idx_path)
    result = plan_dedup(items)
    kept = sorted(result["kept"], key=lambda it: (it["key"], it["off"]))
    (out_dir / "keep.jsonl").write_text(
        "".join(json.dumps({k: (v.hex() if k == "seq" else v) for k, v in it.items()},
                           ensure_ascii=False) + "\n" for it in kept), encoding="utf-8")
    # 幂等：先清零该阶段计数，再按文件重建（plan 可以反复重跑，不能累加）
    for row in ledger.files.values():
        row["session_removed"] = 0
        row["prefix_removed"] = 0
    for it in result["session_removed"]:
        ledger.file(it["key"])["session_removed"] += 1
    for it in result["prefix_removed"]:
        ledger.file(it["key"])["prefix_removed"] += 1
    ledger.meta["exact_dup"] = result["exact_dup"]
    finalize_ledger(ledger)
    state.data["ledger"] = ledger.files
    state.save()
    write_ledger_json(out_dir, ledger)
    print(f"[plan] 输入 {len(items)} → session 移除 {len(result['session_removed'])} "
          f"→ 前缀移除 {len(result['prefix_removed'])}（其中完全重复 {result['exact_dup']}）"
          f" → 保留 {len(kept)}")
    return kept


def write_ledger_json(out_dir: Path, ledger):
    (out_dir / "ledger.json").write_text(
        json.dumps({"files": ledger.files, "totals": ledger.totals()}, ensure_ascii=False, indent=1))


def _load_journal(jpath: Path):
    entries = []
    if jpath.exists():
        with open(jpath, encoding="utf-8") as fh:
            for line in fh:
                if line.strip():
                    entries.append(json.loads(line))
    return entries


def _journal_tokens(entries) -> int:
    return sum(int(e.get("tokens") or 0) for e in entries)


def _rebuild_parts(out_dir: Path, entries, files_state):
    """按 journal 重建分片账本（按源文件的 part 归组），并截掉崩溃留下的半行。"""
    parts = {}
    for e in entries:
        key = e["key"]
        part = int(e.get("part") or 1)
        info = parts.setdefault((key, part), {"bytes": 0, "samples": 0, "tokens": 0})
        info["bytes"] += int(e.get("bytes") or 0)
        info["samples"] += int(e.get("lines") or 1)
        info["tokens"] += int(e.get("tokens") or 0)
    for (key, part), info in parts.items():
        st = files_state.setdefault(key, {})
        mode = st.get("mode", "single")
        single_path = out_dir / part_name(key, 1, "single")
        parts_path = out_dir / part_name(key, 1, "parts")
        if mode == "single" and part == 1 and not single_path.exists() and parts_path.exists():
            # 重命名后、state 落盘前崩溃的窄窗口：以磁盘为准
            mode = "parts"
            st["mode"] = "parts"
        info["name"] = part_name(key, part, mode)
        _truncate_file(out_dir / info["name"], info["bytes"])
    return parts


def _prefetch_iter(items, args, client, depth):
    """有序预取：并发读，按原顺序产出；深度受限，内存里最多 depth 条记录。"""
    if depth <= 1:
        for it in items:
            yield it, read_range(args, client, it["key"], it["off"], it["len"])
        return
    from collections import deque

    with ThreadPoolExecutor(max_workers=depth) as pool:
        pending = deque()
        cursor = 0
        while cursor < len(items) and len(pending) < depth:
            it = items[cursor]
            pending.append((it, pool.submit(read_range, args, client, it["key"], it["off"], it["len"])))
            cursor += 1
        while pending:
            it, fut = pending.popleft()
            raw = fut.result()
            if cursor < len(items):
                nxt = items[cursor]
                pending.append((nxt, pool.submit(read_range, args, client, nxt["key"], nxt["off"], nxt["len"])))
                cursor += 1
            yield it, raw


def phase_emit(args, state, ledger, out_dir: Path, budget: SegmentBudget):
    import tiktoken
    encoder = tiktoken.get_encoding(args.tokenizer)
    keep_path = out_dir / "keep.jsonl"
    kept = []
    with open(keep_path, encoding="utf-8") as fh:
        for ln in fh:
            if ln.strip():
                kept.append(json.loads(ln))

    jpath = out_dir / "emit.journal"
    entries = _load_journal(jpath)
    done = {e["id"] for e in entries}
    written_tokens = _journal_tokens(entries)
    emit_state = state.data["emit"]
    # 上一段判定为「单条 message 自身超限、整条不交付」的记录，同样视为已处理（幂等）
    skipped_oversize = list(emit_state.get("skipped_oversize") or [])
    skipped_set = set(skipped_oversize)
    done |= skipped_set
    files_state = emit_state.setdefault("files", {})
    parts = _rebuild_parts(out_dir, entries, files_state)
    client = None if args.local_dir else make_client(args)

    if emit_state.get("stop_reason") == "token_target_reached" and emit_state.get("stop_file"):
        print(f"[emit] 上一段已在文件边界停止（stop_file={emit_state['stop_file']}），不再继续")
        return written_tokens, "token_target_reached"

    # 按源文件分组，顺序沿用 (key, off)
    order_keys, groups = [], {}
    for it in kept:
        key = it["key"]
        if key not in groups:
            groups[key] = []
            order_keys.append(key)
        groups[key].append(it)

    stop_reason = None
    stop_after_record = emit_state.get("stop_after_record")
    stop_file = emit_state.get("stop_file")
    paused = False
    cur_key = None
    cur_part = 1
    cur_mode = "single"
    cur_bytes = 0
    cur_fh = None

    def close_cur():
        nonlocal cur_fh
        if cur_fh is not None:
            cur_fh.flush()
            os.fsync(cur_fh.fileno())
            cur_fh.close()
            cur_fh = None

    def open_cur():
        nonlocal cur_fh
        name = part_name(cur_key, cur_part, cur_mode)
        cur_fh = open(out_dir / name, "ab")
        return name

    def start_key(key):
        nonlocal cur_key, cur_part, cur_mode, cur_bytes
        close_cur()
        cur_key = key
        st = files_state.setdefault(key, {})
        cur_mode = st.get("mode", "single")
        existing = sorted(p for (k, p) in parts if k == key)
        cur_part = max(existing) if existing else 1
        cur_bytes = parts.get((key, cur_part), {}).get("bytes", 0)
        if cur_bytes >= args.shard_bytes:
            # 上一片已满，开新片；单片命名要先升级成 parts 命名
            if cur_mode == "single":
                old = out_dir / part_name(key, 1, "single")
                if old.exists():
                    old.replace(out_dir / part_name(key, 1, "parts"))
                cur_mode = "parts"
                st["mode"] = "parts"
                if (key, 1) in parts:
                    parts[(key, 1)]["name"] = part_name(key, 1, "parts")
                state.save()
            cur_part += 1
            cur_bytes = parts.get((key, cur_part), {}).get("bytes", 0)
        st.setdefault("mode", cur_mode)
        st["status"] = "partial"
        open_cur()

    def rotate():
        nonlocal cur_part, cur_mode, cur_bytes
        close_cur()
        if cur_part == 1 and cur_mode == "single":
            # 第一次切分片：把已写好的单片改名为 .part-0001，并先落盘状态再写下一片
            old = out_dir / part_name(cur_key, 1, "single")
            if old.exists():
                old.replace(out_dir / part_name(cur_key, 1, "parts"))
            cur_mode = "parts"
            files_state[cur_key]["mode"] = "parts"
            if (cur_key, 1) in parts:
                parts[(cur_key, 1)]["name"] = part_name(cur_key, 1, "parts")
            state.save()
        cur_part += 1
        cur_bytes = 0
        open_cur()

    try:
        wrote_any = False
        for key in order_keys:
            key_items = groups[key]
            remaining = [it for it in key_items if it["id"] not in done]
            if not remaining:
                # 该文件上一段已交付完：仍要在文件边界做停点判定
                written_tokens = _journal_tokens(_load_journal(jpath))
                if written_tokens >= args.target_tokens:
                    stop_reason = "token_target_reached"
                    stop_file = key
                    break
                continue
            # 软时限只在"本段已经交付过记录"之后生效，否则每段都会空转
            if paused or (wrote_any and budget.expired()):
                paused = True
                break
            start_key(key)
            st = files_state[cur_key]
            for it, raw in _prefetch_iter(remaining, args, client, args.emit_prefetch):
                if wrote_any and budget.expired():
                    paused = True
                    break
                try:
                    rec = json.loads(raw)
                except Exception:
                    print(f"[emit] 跳过无法解析的记录 {it['id']}")
                    done.add(it["id"])
                    continue
                msgs = messages_of(rec)
                windows, window_tokens, oversize = split_windows(msgs, encoder, args.max_sample_tokens)
                if oversize > 0:
                    # 整条 message 自身就超过单条上限：切窗无解（切出来的窗口里那条还是超），
                    # 按裁决 B 整条不交付，账本记 not_delivered_oversize_message。
                    if it["id"] not in skipped_set:
                        skipped_set.add(it["id"])
                        skipped_oversize.append(it["id"])
                    done.add(it["id"])
                    continue

                # 整组窗口是最小提交单位：先全部拼好，再决定换不换分片，最后一起落盘 + 一笔入账
                group = []
                for wi, (window, tok) in enumerate(zip(windows, window_tokens)):
                    if len(windows) == 1:
                        line = raw if raw.endswith(b"\n") else raw + b"\n"
                    else:
                        out_rec = dict(rec)
                        out_rec["request"] = {**(rec.get("request") or {}), "messages": window}
                        out_rec["_window"] = {"index": wi, "of": len(windows),
                                              "source": {"key": it["key"], "off": it["off"]},
                                              "messages_total": len(msgs), "oversize_message": oversize}
                        line = (json.dumps(out_rec, ensure_ascii=False) + "\n").encode()
                    group.append((line, tok))

                group_bytes = sum(len(ln) for ln, _ in group)
                group_tokens = sum(tok for _, tok in group)
                if cur_bytes > 0 and cur_bytes + group_bytes > args.shard_bytes:
                    rotate()
                for line, _ in group:
                    cur_fh.write(line)
                # 顺序铁律：分片先 fsync 落盘，journal 后记；反过来会出现"账上有、盘上没有"
                cur_fh.flush()
                os.fsync(cur_fh.fileno())
                cur_bytes += group_bytes
                with open(jpath, "a", encoding="utf-8") as jf:
                    jf.write(json.dumps({"id": it["id"], "key": cur_key, "part": cur_part,
                                         "bytes": group_bytes, "tokens": group_tokens,
                                         "lines": len(group)}) + "\n")
                    jf.flush()
                    os.fsync(jf.fileno())
                written_tokens += group_tokens
                done.add(it["id"])
                wrote_any = True
                st["bytes"] = cur_bytes
                st["tokens"] = st.get("tokens", 0) + group_tokens
                st["records"] = st.get("records", 0) + 1
                st["samples"] = st.get("samples", 0) + len(group)
                st["parts"] = max(cur_part, int(st.get("parts", 0)))
                stop_after_record = {"id": it["id"], "key": it["key"], "off": it["off"],
                                     "n_msgs": len(msgs), "windows": len(group),
                                     "tokens": group_tokens, "tokens_total_after": written_tokens,
                                     "part": cur_part}
                if len(done) % 200 == 0:
                    print(f"[emit] {sum(int(s.get('samples', 0)) for s in files_state.values())} 行 / "
                          f"{written_tokens/1e6:.1f}M token")
                    lock_heartbeat(state)
            close_cur()
            if paused:
                break
            # 文件边界停点：这个源文件的记录全部交付完，才判断要不要停
            written_tokens = _journal_tokens(_load_journal(jpath))
            st["status"] = "delivered"
            st["tokens"] = st.get("tokens", 0)
            state.save()
            lock_heartbeat(state)
            if written_tokens >= args.target_tokens:
                stop_reason = "token_target_reached"
                stop_file = key
                break
    finally:
        close_cur()

    if not stop_reason:
        if paused:
            stop_reason = "segment_paused"
        else:
            stop_reason = "token_target_reached" if written_tokens >= args.target_tokens else "corpus_exhausted"

    # 账本按 journal 重算（续跑不重复计数）；快照必须在本次全部写完之后再取
    entries = _load_journal(jpath)
    written_tokens = _journal_tokens(entries)
    total_samples = sum(int(e.get("lines") or 1) for e in entries)
    for row in ledger.files.values():
        row["delivered_records"] = 0
        row["delivered_windows"] = 0
        row["delivered_tokens"] = 0
    seen_records = set()
    for e in entries:
        row = ledger.file(e["key"])
        row["delivered_windows"] += int(e.get("lines") or 1)
        row["delivered_tokens"] += int(e.get("tokens") or 0)
        if e["id"] not in seen_records:
            seen_records.add(e["id"])
            row["delivered_records"] += 1

    for row in ledger.files.values():
        row["not_delivered_oversize_message"] = 0
    for rid in skipped_oversize:
        key = rid.rsplit("#", 1)[0]
        if key in ledger.files:
            ledger.files[key]["not_delivered_oversize_message"] = (
                ledger.files[key].get("not_delivered_oversize_message", 0) + 1)

    finalize_ledger(ledger)
    emit_state["skipped_oversize"] = skipped_oversize
    if skipped_oversize:
        print(f"[emit] 整条不交付（单条 message 自身 > {args.max_sample_tokens:,} token）：{len(skipped_oversize)} 条")
    emit_state.update({"stop_reason": stop_reason, "tokens": written_tokens,
                       "total_samples": total_samples,
                       "stop_after_record": stop_after_record,
                       "stop_file": stop_file})
    state.data["emit"] = emit_state
    state.save()
    print(f"[emit] 交付 {total_samples} 行 / {written_tokens:,} token / 停止原因 {stop_reason}"
          + (f" / stop_file={stop_file}" if stop_file else ""))
    return written_tokens, stop_reason


def write_manifest(args, state, ledger, out_dir: Path, delivered_tokens, stop_reason):
    emit = state.data["emit"]
    keep_path = out_dir / "keep.jsonl"
    manifest_kept = 0
    if keep_path.exists():
        with open(keep_path, encoding="utf-8") as fh:
            manifest_kept = sum(1 for _ in fh)
    journal = _load_journal(out_dir / "emit.journal")
    journal_ids = {e["id"] for e in journal}
    delivered_records = len(journal_ids)
    finalize_ledger(ledger)
    totals = ledger.totals()

    files_state = emit.get("files") or {}
    shard_entries = []
    for key in sorted(files_state):
        st = files_state[key]
        mode = st.get("mode", "single")
        part_count = int(st.get("parts", 1) or 1)
        for part in range(1, part_count + 1):
            shard_entries.append({
                "file": part_name(key, part, mode),
                "source_key": key,
                "part": part,
                "samples": 0, "bytes": 0, "tokens": 0,
            })
    # 逐片数字以 journal 为准（分段续跑后内存计数不可信）
    for entry in journal:
        key = entry["key"]
        target = next((s for s in shard_entries
                       if s["source_key"] == key and s["part"] == int(entry.get("part") or 1)), None)
        if target is None:
            continue
        target["samples"] += int(entry.get("lines") or 1)
        target["bytes"] += int(entry.get("bytes") or 0)
        target["tokens"] += int(entry.get("tokens") or 0)

    stalled = bool(stop_reason == "segment_paused")
    manifest = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "STOP_REASON": stop_reason,
        "SEGMENT_PAUSED": stalled,
        "stop_after_record": emit.get("stop_after_record"),
        "stop_file": emit.get("stop_file"),
        "target_tokens": args.target_tokens,
        "delivered_tokens": delivered_tokens,
        "delivered_samples": emit.get("total_samples", 0),
        "kept_records": manifest_kept,
        "delivered_records": delivered_records,
        "not_delivered": max(0, manifest_kept - delivered_records),
        "shards": shard_entries,
        "delivered_files": [
            {"source_key": key,
             "status": files_state.get(key, {}).get("status", ""),
             "mode": files_state.get(key, {}).get("mode", "single"),
             "parts": int(files_state.get(key, {}).get("parts", 1) or 1),
             "records": sum(1 for e in journal if e["key"] == key),
             "tokens": sum(int(e.get("tokens") or 0) for e in journal if e["key"] == key)}
            for key in sorted(files_state)
        ],
        "config": {
            "bucket": args.bucket, "prefix": args.prefix, "min_messages": args.min_messages,
            "max_sample_tokens": args.max_sample_tokens, "shard_bytes": args.shard_bytes,
            "tokenizer": args.tokenizer, "tokens_per_byte_estimate": args.tokens_per_byte,
            "index_connections": args.index_connections, "emit_prefetch": args.emit_prefetch,
            "max_seconds": args.max_seconds, "index_stop_safety": INDEX_STOP_SAFETY,
        },
        "ledger": totals,
        "dedup_closure_ok": (totals["passed"]
                             == totals["session_removed"] + totals["prefix_removed"] + manifest_kept),
        "index": {"files_done": len(state.data["index"]["done"]),
                  "stop_reason": state.data["index"].get("stop_reason")},
        "segments": state.data.get("segments", 0),
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2))
    write_ledger_json(out_dir, ledger)
    return manifest


# --------------------------------------------------------------------- CLI

def build_parser(argv=None):
    ap = argparse.ArgumentParser(description="B2 → 1B token SFT 语料导出流水线（默认 dry-run）")
    ap.add_argument("--phase", choices=["all", "index", "plan", "emit"], default="all")
    ap.add_argument("--execute", action="store_true", help="真正执行（默认只打印计划）")
    ap.add_argument("--reset", action="store_true", help="清空断点状态（index/keep 文件保留）")
    ap.add_argument("--out-dir", default="./corpus_out")
    ap.add_argument("--endpoint", default=DEFAULT_ENDPOINT)
    ap.add_argument("--region", default="us-east-005")
    ap.add_argument("--bucket", default=DEFAULT_BUCKET)
    ap.add_argument("--prefix", default=DEFAULT_PREFIX)
    ap.add_argument("--keys", nargs="*", default=[])
    ap.add_argument("--keys-file")
    ap.add_argument("--local-dir", help="本地目录模式（离线自测用，不连 B2）")
    ap.add_argument("--max-records", type=int, default=0, help="每个文件最多扫描多少条（0=全部，自测用）")
    ap.add_argument("--min-messages", type=int, default=MIN_MESSAGES)
    ap.add_argument("--max-sample-tokens", type=int, default=MAX_SAMPLE_TOKENS)
    ap.add_argument("--target-tokens", type=int, default=TARGET_TOKENS)
    ap.add_argument("--shard-bytes", type=int, default=SHARD_BYTES)
    ap.add_argument("--tokenizer", default="o200k_base")
    ap.add_argument("--tokens-per-byte", type=float, default=DEFAULT_TOKENS_PER_BYTE)
    ap.add_argument("--max-seconds", type=float, default=0,
                    help="本段软时限（秒）；到点后在安全点让出，下次调用自动续跑")
    ap.add_argument("--index-connections", type=int, default=4,
                    help="index 阶段跨文件并发连接数（上限即同时在读的源文件数）")
    ap.add_argument("--emit-prefetch", type=int, default=4,
                    help="emit 阶段有序预取深度（并发 Range GET 条数，写仍然单线程）")
    ap.add_argument("--lock-ttl", type=int, default=LOCK_TTL_SECONDS,
                    help="run.lock 心跳过期秒数；心跳新鲜时第二个实例直接退出")
    return ap


def print_plan(args):
    print("=== dry-run：未加 --execute，不会连网、不会写盘 ===")
    print(f"阶段        : {args.phase}")
    print(f"数据源      : {args.local_dir or f'{args.endpoint} / {args.bucket} / {args.prefix}'}")
    if args.keys:
        print(f"对象清单    : {len(args.keys)} 个（--keys 指定）")
        for k in args.keys[:10]:
            print(f"              - {k}")
        if len(args.keys) > 10:
            print(f"              … 另有 {len(args.keys)-10} 个")
    elif args.keys_file:
        n = len([1 for ln in open(args.keys_file) if ln.strip()])
        print(f"对象清单    : {n} 行（来自 {args.keys_file}）")
    else:
        print("对象清单    : 前缀列举（--execute 时执行 ListObjectsV2）")
    print(f"过滤口径    : messages ≥ {args.min_messages} 条 | rs_id 存在 | messages[].content[] 含 encrypted_content")
    print(f"去重口径    : session_id 的 uuid 段去重（保留最长）→ messages 前缀去重（排序 + 相邻比对）")
    print(f"token 口径  : tiktoken {args.tokenizer} / encode_ordinary / 每条 message 紧凑 JSON 求和（含密文）")
    print(f"目标/止损   : {args.target_tokens:,} token 到达即停，STOP_REASON=token_target_reached")
    print(f"分片        : 按源文件命名（单源 > {args.shard_bytes/1024**2:.0f} MB 切 .part-NNNN）；"
          f"单条 > {args.max_sample_tokens:,} token 切窗")
    print(f"停点        : 累计 {args.target_tokens:,} token 后，把当前源文件处理完即停（文件边界）")
    print(f"并发/分段   : index {args.index_connections} 连接 / emit 预取 {args.emit_prefetch}"
          + (f" / 每段 {args.max_seconds:.0f}s" if args.max_seconds else " / 不分段"))
    print(f"输出目录    : {Path(args.out_dir).resolve()}（index.jsonl / keep.jsonl / "
          f"<源文件>.jsonl / manifest.json / emit.journal / run.lock）")
    print("执行方式    : 加上 --execute 才会真正跑；中断后重跑同一命令自动续跑。")


def main(argv=None):
    args = build_parser(argv).parse_args(argv)
    if not args.execute:
        print_plan(args)
        return 0

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    lock = RunLock(out_dir, ttl=args.lock_ttl)
    if not lock.acquire():
        print(f"[lock] 同一 out-dir 已有实例在跑（{out_dir / 'run.lock'} 心跳新鲜），本段直接退出")
        return LOCK_EXIT_OK
    try:
        budget = SegmentBudget(args.max_seconds)
        state_path = out_dir / "state.json"
        if args.reset and state_path.exists():
            state_path.unlink()
        state = State(state_path)
        state.lock = lock
        state.data["segments"] = int(state.data.get("segments") or 0) + 1
        lock.heartbeat()

        ledger = Ledger()
        stored = state.data.get("ledger")
        if stored:
            ledger.files = stored
        state.data["ledger"] = ledger.files

        paused = False
        if args.phase in ("all", "index"):
            _, paused = phase_index(args, state, ledger, out_dir, budget)
        if paused:
            # index 在本段内暂停：绝不拿半份索引去跑 plan/emit
            write_manifest(args, state, ledger, out_dir,
                           int(state.data["emit"].get("tokens") or 0), "segment_paused")
            return 0
        if args.phase in ("all", "plan"):
            phase_plan(args, state, ledger, out_dir)
        if args.phase in ("all", "emit"):
            tokens, reason = phase_emit(args, state, ledger, out_dir, budget)
            write_manifest(args, state, ledger, out_dir, tokens, reason)
        return 0
    finally:
        lock.release()


if __name__ == "__main__":
    sys.exit(main())
