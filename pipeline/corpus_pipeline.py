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

默认 dry-run：不带 --execute 只打印计划，不连网、不写盘。
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
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
INDEX_STOP_SAFETY = 1.15  # 预估过线后再多留 15% 余量，避免 emit 阶段凑不满


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
    return None


def session_dedup(items):
    """同 uuid 只留 messages 最长；并列取 timestamp 较新、再取输入顺序靠前。"""
    best = {}
    for it in items:
        cur = best.get(it["uuid"])
        if cur is None:
            best[it["uuid"]] = it
            continue
        a = (it["n_msgs"], it.get("timestamp") or "")
        b = (cur["n_msgs"], cur.get("timestamp") or "")
        if a > b:
            best[it["uuid"]] = it
    kept_ids = {id(v) for v in best.values()}
    kept = [it for it in items if id(it) in kept_ids]
    removed = [it for it in items if id(it) not in kept_ids]
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
                "too_short", "passed", "session_removed", "prefix_removed", "exact_dup",
                "delivered_records", "delivered_windows"]
        out = {k: sum(f.get(k, 0) for f in self.files.values()) for k in keys}
        out.update(self.meta)
        out["filter_closure_ok"] = (
            out["scanned"] - out["passed"]
            == out["parse_error"] + out["no_session_id"] + out["no_rs_id"]
            + out["no_encrypted_content"] + out["too_short"]
        )
        return out


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


def iter_record_bytes(args, client, key):
    """逐行产出 (offset, raw)；不整文件进内存，也不落本地磁盘。"""
    if args.local_dir:
        offset = 0
        with open(Path(args.local_dir) / Path(key).name, "rb") as fh:
            for raw in fh:
                yield offset, raw
                offset += len(raw)
        return
    import io
    body = client.get_object(Bucket=args.bucket, Key=key)["Body"]
    stream = io.BufferedReader(body, buffer_size=1 << 22)
    offset = 0
    try:
        for raw in stream:
            yield offset, raw
            offset += len(raw)
    finally:
        stream.close()


def read_range(args, client, key, offset, length) -> bytes:
    if args.local_dir:
        with open(Path(args.local_dir) / Path(key).name, "rb") as fh:
            fh.seek(offset)
            return fh.read(length)
    r = client.get_object(Bucket=args.bucket, Key=key, Range=f"bytes={offset}-{offset + length - 1}")
    return r["Body"].read()


# --------------------------------------------------------------------- 状态

class State:
    def __init__(self, path: Path):
        self.path = path
        self.data = {"index": {"done": [], "stop_reason": None, "files": {}},
                     "emit": {"done": [], "shard_no": 0, "shard_bytes": 0,
                              "shard_samples": 0, "tokens": 0, "shards": []},
                     "ledger": None}
        if path.exists():
            self.data.update(json.loads(path.read_text()))

    def save(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(self.data, ensure_ascii=False, indent=1))
        tmp.replace(self.path)


# --------------------------------------------------------------------- 三个阶段

def phase_index(args, state, ledger, out_dir: Path):
    idx_path = out_dir / "index.jsonl"
    client = None if args.local_dir else make_client(args)
    sources = list_sources(args, client)
    done = set(state.data["index"]["done"])
    todo = [(k, s) for k, s in sources if k not in done]
    if todo and state.data["index"].get("stop_reason") == "token_target_reached":
        # 续跑且目标提高：清掉上一轮的提前止损标记，继续拉新文件
        state.data["index"]["stop_reason"] = None
        state.save()
    est_bytes = sum(s or 0 for k, s in todo)
    print(f"[index] 源对象 {len(sources)} 个（待处理 {len(todo)}），已知体积 {est_bytes/1024**3:.2f} GiB")

    existing = sum(1 for _ in open(idx_path, encoding="utf-8")) if idx_path.exists() else 0
    new_index = []
    with open(idx_path, "a", encoding="utf-8") as out:
        for key, size in todo:
            t0 = time.time()
            row = ledger.file(key)
            scanned_in_file = 0
            for offset, raw in iter_record_bytes(args, client, key):
                scanned_in_file += 1
                if args.max_records and scanned_in_file > args.max_records:
                    break
                try:
                    rec = json.loads(raw)
                except Exception:
                    ledger.tally_filter(key, "parse_error")
                    continue
                reason = filter_reason(rec, args.min_messages)
                ledger.tally_filter(key, reason)
                if reason is not None:
                    continue
                msgs = messages_of(rec)
                item = {
                    "id": f"{key}#{offset}",
                    "key": key,
                    "off": offset,
                    "len": len(raw),
                    "uuid": session_uuid(rec.get("session_id")),
                    "timestamp": rec.get("timestamp") or "",
                    "n_msgs": len(msgs),
                    "seq": record_seq(msgs).hex(),
                    "order": existing + len(new_index),
                }
                new_index.append(item)
                out.write(json.dumps(item, ensure_ascii=False) + "\n")
            out.flush()
            state.data["index"]["done"].append(key)
            state.data["index"]["files"][key] = {"scanned": scanned_in_file,
                                                 "elapsed_s": round(time.time() - t0, 1)}
            state.save()
            print(f"[index] {key}: 扫描 {scanned_in_file} 条，通过 {row['passed']}，{time.time()-t0:.0f}s")

            # 提前止损：按实测 token/byte 预估保留集，过线就不再拉新文件
            projected = project_kept_tokens(idx_path, args.tokens_per_byte)
            if projected and projected >= args.target_tokens * INDEX_STOP_SAFETY:
                state.data["index"]["stop_reason"] = "token_target_reached"
                state.save()
                print(f"[index] 预估保留集 {projected/1e9:.3f}B token ≥ 目标×{INDEX_STOP_SAFETY}，"
                      f"停止拉取新文件")
                break

    total = sum(1 for _ in open(idx_path)) if idx_path.exists() else 0
    print(f"[index] 完成：index.jsonl {total} 条")
    return total


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
    for it in result["session_removed"]:
        ledger.file(it["key"])["session_removed"] += 1
    for it in result["prefix_removed"]:
        ledger.file(it["key"])["prefix_removed"] += 1
    ledger.meta["exact_dup"] = result["exact_dup"]
    state.data["ledger"] = ledger.files
    state.save()
    print(f"[plan] 输入 {len(items)} → session 移除 {len(result['session_removed'])} "
          f"→ 前缀移除 {len(result['prefix_removed'])}（其中完全重复 {result['exact_dup']}）"
          f" → 保留 {len(kept)}")
    return kept


def _load_journal(jpath: Path):
    entries = []
    if jpath.exists():
        for line in jpath.read_text(encoding="utf-8").splitlines():
            if line.strip():
                entries.append(json.loads(line))
    return entries


def _rebuild_shards(out_dir: Path, entries):
    """按 journal 重建分片账本，并把崩溃留下的半行截掉。"""
    shards = {}
    for e in entries:
        s = shards.setdefault(e["shard"], {"file": f"deliver-{e['shard']:04d}.jsonl",
                                           "samples": 0, "bytes": 0, "tokens": 0})
        s["samples"] += 1
        s["bytes"] += e["bytes"]
        s["tokens"] += e["tokens"]
    for no, info in shards.items():
        path = out_dir / info["file"]
        if not path.exists():
            continue
        if path.stat().st_size > info["bytes"]:      # 上次中断留下的半行 / 未入账的行
            with open(path, "r+b") as fh:
                fh.truncate(info["bytes"])
    return shards


def phase_emit(args, state, ledger, out_dir: Path):
    import tiktoken
    encoder = tiktoken.get_encoding(args.tokenizer)
    kept = [json.loads(ln) for ln in open(out_dir / "keep.jsonl", encoding="utf-8")]
    jpath = out_dir / "emit.journal"
    entries = _load_journal(jpath)
    done = {e["id"] for e in entries}
    written_tokens = sum(e["tokens"] for e in entries)
    total_samples = len(entries)
    shards = _rebuild_shards(out_dir, entries)

    if shards:
        cur_no = max(shards)
        cur = shards[cur_no]
        if cur["bytes"] >= args.shard_bytes:          # 上一片已满，开新片
            cur_no += 1
            cur = {"file": f"deliver-{cur_no:04d}.jsonl", "samples": 0, "bytes": 0, "tokens": 0}
            shards[cur_no] = cur
    else:
        cur_no = 1
        cur = {"file": "deliver-0001.jsonl", "samples": 0, "bytes": 0, "tokens": 0}
        shards[cur_no] = cur

    client = None if args.local_dir else make_client(args)
    shard_fh = open(out_dir / cur["file"], "ab")
    journal_fh = open(jpath, "a", encoding="utf-8")
    stop_reason = None

    def rotate():
        nonlocal cur_no, cur, shard_fh
        shard_fh.flush()
        shard_fh.close()
        cur_no += 1
        cur = {"file": f"deliver-{cur_no:04d}.jsonl", "samples": 0, "bytes": 0, "tokens": 0}
        shards[cur_no] = cur
        shard_fh = open(out_dir / cur["file"], "ab")

    try:
        for it in kept:
            if it["id"] in done:
                continue
            if written_tokens >= args.target_tokens:
                stop_reason = "token_target_reached"
                break
            raw = read_range(args, client, it["key"], it["off"], it["len"])
            try:
                rec = json.loads(raw)
            except Exception:
                print(f"[emit] 跳过无法解析的记录 {it['id']}")
                continue
            msgs = messages_of(rec)
            windows, window_tokens, oversize = split_windows(msgs, encoder, args.max_sample_tokens)

            for wi, (window, tok) in enumerate(zip(windows, window_tokens)):
                if written_tokens >= args.target_tokens:
                    stop_reason = "token_target_reached"
                    break
                if len(windows) == 1:
                    line = raw if raw.endswith(b"\n") else raw + b"\n"
                else:
                    out_rec = dict(rec)
                    out_rec["messages"] = window
                    out_rec["_window"] = {"index": wi, "of": len(windows),
                                          "source": {"key": it["key"], "off": it["off"]},
                                          "messages_total": len(msgs), "oversize_message": oversize}
                    line = (json.dumps(out_rec, ensure_ascii=False) + "\n").encode()
                if cur["bytes"] + len(line) > args.shard_bytes and cur["samples"] > 0:
                    rotate()
                shard_fh.write(line)
                shard_fh.flush()
                cur["bytes"] += len(line)
                cur["samples"] += 1
                cur["tokens"] += tok
                journal_fh.write(json.dumps({"id": it["id"], "key": it["key"], "shard": cur_no,
                                             "bytes": len(line), "tokens": tok}) + "\n")
                journal_fh.flush()
                written_tokens += tok
                total_samples += 1
                done.add(it["id"])
            if len(done) % 200 == 0 and total_samples:
                print(f"[emit] {total_samples} 行 / {written_tokens/1e6:.1f}M token")
            if stop_reason:
                break
    finally:
        shard_fh.flush()
        shard_fh.close()
        journal_fh.close()

    if not stop_reason:
        stop_reason = "token_target_reached" if written_tokens >= args.target_tokens else "corpus_exhausted"

    # 账本按 journal 重算（续跑不重复计数）
    for row in ledger.files.values():
        row["delivered_records"] = 0
        row["delivered_windows"] = 0
    seen_records = set()
    for e in entries:
        row = ledger.file(e["key"])
        row["delivered_windows"] += 1
        if e["id"] not in seen_records:
            seen_records.add(e["id"])
            row["delivered_records"] += 1

    state.data["emit"] = {"stop_reason": stop_reason, "tokens": written_tokens,
                          "total_samples": total_samples,
                          "shards": [shards[k] for k in sorted(shards)]}
    state.save()
    print(f"[emit] 交付 {total_samples} 行 / {written_tokens:,} token / 停止原因 {stop_reason}")
    return written_tokens, stop_reason


def write_manifest(args, state, ledger, out_dir: Path, delivered_tokens, stop_reason):
    emit = state.data["emit"]
    keep_path = out_dir / "keep.jsonl"
    manifest_kept = sum(1 for _ in open(keep_path, encoding="utf-8")) if keep_path.exists() else 0
    journal_ids = {e["id"] for e in _load_journal(out_dir / "emit.journal")}
    delivered_records = len(journal_ids)
    totals = ledger.totals()
    manifest = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "STOP_REASON": stop_reason,
        "target_tokens": args.target_tokens,
        "delivered_tokens": delivered_tokens,
        "delivered_samples": emit.get("total_samples", 0),
        "kept_records": manifest_kept,
        "delivered_records": delivered_records,
        "not_delivered": max(0, manifest_kept - delivered_records),
        "shards": emit.get("shards", []),
        "config": {
            "bucket": args.bucket, "prefix": args.prefix, "min_messages": args.min_messages,
            "max_sample_tokens": args.max_sample_tokens, "shard_bytes": args.shard_bytes,
            "tokenizer": args.tokenizer, "tokens_per_byte_estimate": args.tokens_per_byte,
        },
        "ledger": totals,
        "dedup_closure_ok": (totals["passed"]
                             == totals["session_removed"] + totals["prefix_removed"] + manifest_kept),
        "index": {"files_done": len(state.data["index"]["done"]),
                  "stop_reason": state.data["index"].get("stop_reason")},
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2))
    (out_dir / "ledger.json").write_text(
        json.dumps({"files": ledger.files, "totals": ledger.totals()}, ensure_ascii=False, indent=1))
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
    print(f"分片        : 每包 ≥ {args.shard_bytes/1024**2:.0f} MB 换包；单条 > {args.max_sample_tokens:,} token 切窗")
    print(f"输出目录    : {Path(args.out_dir).resolve()}（index.jsonl / keep.jsonl / deliver-NNNN.jsonl / manifest.json）")
    print("执行方式    : 加上 --execute 才会真正跑；中断后重跑同一命令自动续跑。")


def main(argv=None):
    args = build_parser(argv).parse_args(argv)
    if not args.execute:
        print_plan(args)
        return 0

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    state_path = out_dir / "state.json"
    if args.reset and state_path.exists():
        state_path.unlink()
    state = State(state_path)

    ledger = Ledger()
    stored = state.data.get("ledger")
    if stored:
        ledger.files = stored
    state.data["ledger"] = ledger.files

    if args.phase in ("all", "index"):
        phase_index(args, state, ledger, out_dir)
    if args.phase in ("all", "plan"):
        phase_plan(args, state, ledger, out_dir)
    if args.phase in ("all", "emit"):
        tokens, reason = phase_emit(args, state, ledger, out_dir)
        write_manifest(args, state, ledger, out_dir, tokens, reason)
    return 0


if __name__ == "__main__":
    sys.exit(main())
